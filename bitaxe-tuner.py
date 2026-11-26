#!/usr/bin/env python3
import argparse
import copy
import csv
import ipaddress
import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

PROFILES = {
    "BM1370": {"f_min": 400, "f_max": 1100, "v_min": 1100, "v_max": 1300, "verified": True},
    "BM1368": {"f_min": 400, "f_max": 700, "v_min": 1100, "v_max": 1300, "verified": False},
    "BM1366": {"f_min": 400, "f_max": 650, "v_min": 1150, "v_max": 1300, "verified": False},
    "BM1397": {"f_min": 300, "f_max": 575, "v_min": 1250, "v_max": 1450, "verified": False},
}

LOCK = threading.RLock()
TLS = threading.local()
STOP = threading.Event()
ACTIVE = {}
TEMP_F = False


def T(c):
    return c * 9.0 / 5.0 + 32.0 if TEMP_F else c


def TU():
    return "F" if TEMP_F else "C"


def dur(seconds):
    seconds = int(round(seconds))
    if seconds < 120:
        return "%d seconds" % seconds
    minutes, rest = divmod(seconds, 60)
    if rest == 0:
        return "%d minutes" % minutes
    if rest >= 30:
        return "%d minutes" % (minutes + 1) if minutes >= 10 else "%d and a half minutes" % minutes
    return "%d minutes" % minutes


class Offline(Exception):
    pass


class Stop(Exception):
    pass


def stamp():
    return datetime.now().strftime("%I:%M:%S %p").lstrip("0")


def log(msg):
    tag = getattr(TLS, "tag", "")
    with LOCK:
        print("[%s]%s %s" % (stamp(), (" [%s]" % tag) if tag else "", msg), flush=True)


def nap(seconds):
    end = time.time() + seconds
    while time.time() < end:
        if STOP.is_set():
            raise Stop()
        time.sleep(min(1.0, max(0.0, end - time.time())))


def http(host, path, method="GET", body=None, timeout=8):
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request("http://%s%s" % (host, path), data=data, method=method, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except ValueError:
        return {}


def info(host):
    return http(host, "/api/system/info")


def wait_online(host, limit=180):
    end = time.time() + limit
    while time.time() < end:
        try:
            info(host)
            return True
        except Exception:
            time.sleep(4)
    return False


def apply_settings(host, freq, volt, restart=True):
    http(host, "/api/system", method="PATCH", body={"frequency": int(freq), "coreVoltage": int(volt)})
    time.sleep(1)
    if not restart:
        time.sleep(4)
        return wait_online(host, limit=60)
    try:
        http(host, "/api/system/restart", method="POST", body={})
    except Exception:
        pass
    time.sleep(15)
    return wait_online(host)


def apply_with_retry(host, freq, volt, attempts=5):
    for i in range(1, attempts + 1):
        if not wait_online(host, limit=120):
            log("miner not answering, attempt %d of %d" % (i, attempts))
            continue
        try:
            apply_settings(host, freq, volt)
            d = info(host)
            if int(d.get("frequency", 0)) == int(freq) and int(d.get("coreVoltage", 0)) == int(volt):
                return True
            log("settings did not stick, attempt %d of %d" % (i, attempts))
        except Exception as e:
            log("apply failed (%s), attempt %d of %d" % (e, i, attempts))
            time.sleep(10)
    return False


def settle(c):
    start = time.time()
    temps = []
    while time.time() - start < c.settle:
        nap(c.poll)
        try:
            d = info(c.host)
        except Exception:
            continue
        temps.append(d.get("temp", 0) or 0)
        if time.time() - start >= c.settle_min and len(temps) >= 4 and max(temps[-4:]) - min(temps[-4:]) <= 0.6:
            return


def collect(c, seconds, err_limit):
    d0 = info(c.host)
    rej0 = d0.get("sharesRejected", 0)
    samples = []
    early_kind = ""
    early_reason = ""
    end = time.time() + seconds
    while time.time() < end:
        try:
            d = info(c.host)
        except Exception:
            raise Offline("lost connection during test")
        temp = d.get("temp", 0) or 0
        vr = d.get("vrTemp", 0) or 0
        if temp >= c.hard_temp or vr >= c.hard_vr_temp:
            return {"ok": False, "kind": "limit", "reason": "hard temp limit (asic %.1f %s, vr %.1f %s)" % (T(temp), TU(), T(vr), TU()),
                    "hash": 0, "jth": 0, "err": 0, "temp": temp, "power": d.get("power", 0), "rej": 0}
        samples.append({
            "hash": (d.get("hashRate", 0) or 0) / 1000.0,
            "expected": (d.get("expectedHashrate", 0) or 0) / 1000.0,
            "err": d.get("errorPercentage", 0) or 0,
            "temp": temp,
            "power": d.get("power", 0) or 0,
            "rej": d.get("sharesRejected", 0),
            "vin": d.get("voltage", 0) or 0,
        })
        new_rej = samples[-1]["rej"] - rej0
        if new_rej > 0:
            early_kind, early_reason = "stability", "%d rejected" % new_rej
            break
        if len(samples) >= 3:
            recent = sum(s["err"] for s in samples[-3:]) / 3.0
            if recent > max(3.0, err_limit * 3):
                early_kind, early_reason = "stability", "error %.2f%%" % recent
                break
        if temp > c.max_temp:
            early_kind, early_reason = "limit", "temp %.1f %s" % (T(temp), TU())
            break
        nap(c.poll)
    if not samples:
        raise Offline("no samples")
    n = len(samples)
    avg_hash = sum(s["hash"] for s in samples) / n
    avg_exp = sum(s["expected"] for s in samples) / n
    avg_err = sum(s["err"] for s in samples) / n
    avg_pow = sum(s["power"] for s in samples) / n
    vins = [s["vin"] for s in samples if s["vin"] > 0]
    avg_vin = sum(vins) / len(vins) if vins else 0
    max_t = max(s["temp"] for s in samples)
    rej = samples[-1]["rej"] - rej0
    stability = []
    limits = []
    if avg_err > err_limit:
        stability.append("error %.2f%%" % avg_err)
    if rej > 0:
        stability.append("%d rejected" % rej)
    if n >= 3 and avg_exp > 0 and avg_hash < 0.97 * avg_exp:
        stability.append("hashrate below expected")
    if max_t > c.max_temp:
        limits.append("temp %.1f %s" % (T(max_t), TU()))
    if c.min_input and avg_vin and avg_vin < c.min_input:
        limits.append("input voltage %.2f V" % (avg_vin / 1000.0))
    if c.max_watts and avg_pow > c.max_watts:
        limits.append("power %.1f W" % avg_pow)
    if early_kind == "stability" and not stability:
        stability.append(early_reason)
    if early_kind == "limit" and not limits:
        limits.append(early_reason)
    kind = "stability" if stability else ("limit" if limits else "")
    return {
        "ok": not (stability or limits),
        "kind": kind,
        "reason": ", ".join(stability + limits),
        "hash": avg_hash,
        "jth": avg_pow / avg_hash if avg_hash > 0 else 0,
        "err": avg_err,
        "temp": max_t,
        "power": avg_pow,
        "rej": rej,
    }


def load_state(path):
    if os.path.exists(path):
        try:
            with open(path) as fh:
                return json.load(fh)
        except Exception:
            return {}
    return {}


def save_state(path, results):
    with open(path, "w") as fh:
        json.dump(results, fh, indent=1)


def known_fail(results, f, v):
    for key, r in results.items():
        if r["ok"]:
            continue
        rf, rv = [int(x) for x in key.split("/")]
        if r["kind"] == "stability" and rf <= f and rv >= v:
            return "stability"
        if r["kind"] in ("limit", "thermal") and rf <= f and rv <= v:
            return "limit"
    return None


def live_check(c):
    hs = []
    ex = []
    for _ in range(3):
        d = info(c.host)
        hs.append((d.get("hashRate", 0) or 0) / 1000.0)
        ex.append((d.get("expectedHashrate", 0) or 0) / 1000.0)
        nap(5)
    avg_h = sum(hs) / len(hs)
    avg_e = sum(ex) / len(ex)
    return avg_e <= 0 or abs(avg_h - avg_e) / avg_e <= 0.10


def uptime(c):
    try:
        return info(c.host).get("uptimeSeconds", 0) or 0
    except Exception:
        return 0


def live_result(c):
    try:
        d = info(c.host)
    except Exception:
        return None
    up = d.get("uptimeSeconds", 0) or 0
    hr = (d.get("hashRate_10m", 0) or 0) / 1000.0
    exp = (d.get("expectedHashrate", 0) or 0) / 1000.0
    pw = d.get("power", 0) or 0
    err = d.get("errorPercentage", 0) or 0
    temp = d.get("temp", 0) or 0
    rej = d.get("sharesRejected", 0) or 0
    if up < 600 or hr <= 0 or rej > 0 or err > c.max_error or temp > c.max_temp:
        return None
    if exp > 0 and hr < 0.97 * exp:
        return None
    if c.max_watts and pw > c.max_watts:
        return None
    return {"ok": True, "kind": "", "reason": "", "hash": hr, "jth": pw / hr, "err": err, "temp": temp,
            "power": pw, "rej": 0, "stage": "full"}


def run_test(c, results, f, v):
    key = "%d/%d" % (f, v)
    if key in results:
        r = results[key]
        log("cached %s: %s" % (key, "PASS" if r["ok"] else "FAIL " + r["reason"]))
        return r
    if STOP.is_set():
        raise Stop()
    in_place = getattr(c, "base", None) == (f, v)
    r = live_result(c) if in_place else None
    if r is not None:
        log("current settings %s already validated by live 10 minute data, no retest needed" % key)
    else:
        if in_place:
            log("measuring current settings %s in place, no restart" % key)
            if uptime(c) < 600:
                settle(c)
        else:
            log("testing %d MHz / %d mV" % (f, v))
            if not apply_settings(c.host, f, v, restart=c.restart):
                raise Offline("miner did not come back online")
            settle(c)
            if not c.restart and not live_check(c):
                log("live apply did not take effect, switching to restarts")
                c.restart = True
                if not apply_settings(c.host, f, v, restart=True):
                    raise Offline("miner did not come back online")
                settle(c)
        r = collect(c, c.screen, c.max_error * 1.5)
        r["stage"] = "screen"
        if r["ok"]:
            log("screen passed, running the full %s hold" % dur(c.hold))
            r = collect(c, c.hold, c.max_error)
            r["stage"] = "full"
    if r["ok"]:
        log("PASS %s: %.2f TH/s  %.2f J/TH  err %.2f%%  %.1f %s  %.1f W" % (
            key, r["hash"], r["jth"], r["err"], T(r["temp"]), TU(), r["power"]))
    else:
        log("FAIL %s (%s): %s" % (key, r["kind"], r["reason"]))
    r["ts"] = time.time()
    results[key] = r
    save_state(c.state, results)
    return r


def attempt(c, results, f, volts, i):
    kind = known_fail(results, f, volts[i])
    if kind:
        log("skipping %d/%d, implied %s failure" % (f, volts[i], kind))
        return kind, None
    r = run_test(c, results, f, volts[i])
    if r["ok"]:
        return "pass", r
    return ("stability" if r["kind"] == "stability" else "limit"), r


def lowest_passing(c, results, f, volts, lo, hi, hint=None):
    best = None
    if hint is not None and lo <= hint <= hi:
        i = hint
        state, r = attempt(c, results, f, volts, i)
        if state == "pass":
            best = (i, r)
            i -= 1
            steps = 0
            while i >= lo and steps < 3:
                state, r = attempt(c, results, f, volts, i)
                if state != "pass":
                    return best
                best = (i, r)
                i -= 1
                steps += 1
            hi = i
        elif state == "limit":
            hi = i - 1
        else:
            i += 1
            steps = 0
            while i <= hi and steps < 3:
                state, r = attempt(c, results, f, volts, i)
                if state == "pass":
                    return (i, r)
                if state == "limit":
                    return None
                i += 1
                steps += 1
            lo = i
    while lo <= hi:
        mid = (lo + hi) // 2
        state, r = attempt(c, results, f, volts, mid)
        if state == "pass":
            best = (mid, r)
            hi = mid - 1
        elif state == "stability":
            lo = mid + 1
        else:
            hi = mid - 1
    return best


def write_csv(path, results):
    rows = []
    for key, r in results.items():
        f, v = [int(x) for x in key.split("/")]
        rows.append((f, v, r))
    rows.sort(key=lambda x: (x[0], x[1]))
    with open(path, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["freq_mhz", "volt_mv", "pass", "stage", "hash_ths", "j_per_th", "err_pct", "max_temp_c", "power_w", "rejected", "reason"])
        for f, v, r in rows:
            w.writerow([f, v, r["ok"], r.get("stage", ""), "%.3f" % r["hash"], "%.2f" % r["jth"], "%.2f" % r["err"],
                        "%.1f" % r["temp"], "%.1f" % r["power"], r["rej"], r["reason"]])
    return rows


def choose(passing, c):
    roomy = [r for r in passing if r["temp"] <= c.max_temp - c.headroom]
    pool = roomy or passing
    best_eff = min(pool, key=lambda r: r["jth"])
    if c.goal == "efficient":
        return best_eff
    if c.goal == "fast":
        return max(pool, key=lambda r: r["hash"])
    near = [r for r in pool if r["jth"] <= best_eff["jth"] * 1.03]
    return max(near, key=lambda r: r["hash"])


def detect(c):
    d = info(c.host)
    model = d.get("ASICModel", "") or ""
    prof = dict(PROFILES.get(model, {}))
    cur_f = int(d.get("frequency", 0) or 0)
    cur_v = int(d.get("coreVoltage", 0) or 0)
    if not prof:
        prof = {"f_min": max(300, cur_f - 100), "f_max": cur_f + 75, "v_min": cur_v - 50, "v_max": cur_v + 50, "verified": False}
    prof["f_max"] = max(prof["f_max"], cur_f)
    prof["v_max"] = max(prof["v_max"], cur_v)
    prof["v_min"] = min(prof["v_min"], cur_v)
    prof["f_min"] = min(prof["f_min"], cur_f)
    if c.freq_max:
        prof["f_max"] = c.freq_max
    if c.volt_max:
        prof["v_max"] = c.volt_max
    nominal = d.get("nominalVoltage", 0) or 0
    min_input = c.min_input
    if min_input is None:
        vin = d.get("voltage", 0) or 0
        base = nominal * 1000 if 0 < nominal < 100 else (nominal or (5000 if 0 < vin < 6500 else (12000 if vin else 0)))
        min_input = base * 0.93 if base else 0
    c.min_input = min_input
    return d, model, prof, cur_f, cur_v


def backup_path(host):
    return "bitaxe-tuner-backup-%s.json" % host.replace(".", "-").replace(":", "-")


def load_backup(host):
    try:
        with open(backup_path(host)) as fh:
            return json.load(fh)
    except Exception:
        return None


def write_backup(host, f, v, in_progress):
    try:
        with open(backup_path(host), "w") as fh:
            json.dump({"host": host, "original_f": int(f), "original_v": int(v), "in_progress": bool(in_progress),
                       "time": datetime.now().strftime("%Y-%m-%d %I:%M:%S %p")}, fh, indent=1)
    except Exception:
        pass


def keep_awake():
    if sys.platform == "darwin" and shutil.which("caffeinate"):
        try:
            subprocess.Popen(["caffeinate", "-i", "-w", str(os.getpid())], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception:
            pass


def notify(msg):
    print("\a", end="", flush=True)
    if sys.platform == "darwin" and shutil.which("osascript"):
        try:
            subprocess.run(["osascript", "-e", 'display notification "%s" with title "bitaxe-tuner"' % msg],
                           timeout=5, capture_output=True)
        except Exception:
            pass


def restore(c, base_f, base_v):
    with LOCK:
        st = ACTIVE.get(c.host)
        if st is None or st["restored"]:
            return
        st["restored"] = True
    log("restoring %d MHz / %d mV" % (base_f, base_v))
    if not apply_with_retry(c.host, base_f, base_v):
        print("could not restore %d MHz / %d mV, set it manually at http://%s" % (base_f, base_v, c.host))
    else:
        bk = load_backup(c.host)
        if bk:
            write_backup(c.host, bk["original_f"], bk["original_v"], False)


def prepare(c):
    try:
        d, model, prof, cur_f, cur_v = detect(c)
    except Exception as e:
        print("cannot reach %s: %s" % (c.host, e))
        return None
    name = d.get("hostname", "?")
    log("%s (%s) model %s, currently %d MHz / %d mV, %.1f W, input %.2f V" % (
        c.host, name, model or "unknown", cur_f, cur_v, d.get("power", 0) or 0, (d.get("voltage", 0) or 0) / 1000.0))
    if (d.get("hashRate", 0) or 0) <= 0 and not c.force:
        print("%s is not hashing right now (0 TH/s), check its pool connection or use --force" % c.host)
        return None
    bk = load_backup(c.host)
    if bk and bk.get("in_progress") and (bk.get("original_f"), bk.get("original_v")) != (cur_f, cur_v) and not c.dry_run:
        print("a previous run on %s did not finish: original settings were %d MHz / %d mV, the miner is at %d MHz / %d mV" % (
            c.host, bk["original_f"], bk["original_v"], cur_f, cur_v))
        try:
            answer = "y" if c.yes else input("restore the original settings first? [Y/n] ").strip().lower()
        except EOFError:
            answer = "y"
        if answer != "n":
            if apply_with_retry(c.host, bk["original_f"], bk["original_v"]):
                try:
                    d, model, prof, cur_f, cur_v = detect(c)
                except Exception as e:
                    print("cannot reach %s: %s" % (c.host, e))
                    return None
    if not prof["verified"]:
        print("warning: %s has not been verified with this tool, using conservative limits" % (model or "unknown board"))
    volts = sorted(set(range(prof["v_min"], prof["v_max"] + 1, c.volt_step)) | {cur_v})
    if not volts or cur_f <= 0 or cur_v <= 0:
        print("could not read current settings from %s" % c.host)
        return None
    log("limits: up to %d MHz, %d to %d mV, temp %.0f %s, each test takes up to %s" % (
        prof["f_max"], prof["v_min"], prof["v_max"], T(c.max_temp), TU(), dur(c.settle + c.screen + c.hold)))
    if c.dry_run:
        print("dry run for %s (%s): would start at %d MHz / %d mV, search %d to %d mV in %d mV steps, climb %d MHz at a time up to %d MHz, stop near %.0f %s, each test takes up to %s. no settings were changed" % (
            c.host, name, cur_f, cur_v, prof["v_min"], prof["v_max"], c.volt_step, c.freq_step, prof["f_max"],
            T(c.max_temp), TU(), dur(c.settle + c.screen + c.hold)))
        return None
    if not c.yes:
        if input("start tuning %s? it will change settings and restart the miner repeatedly [y/N] " % c.host).strip().lower() != "y":
            return None
    hr = (d.get("hashRate_10m", 0) or d.get("hashRate", 0) or 0) / 1000.0
    pw = d.get("power", 0) or 0
    return {"name": name, "prof": prof, "volts": volts, "cur_f": cur_f, "cur_v": cur_v,
            "cur_hash": hr, "cur_jth": (pw / hr) if hr > 0 else 0, "cur_power": pw,
            "cur_temp": d.get("temp", 0) or 0, "cur_err": d.get("errorPercentage", 0) or 0}


def nearest(volts, target):
    return min(range(len(volts)), key=lambda i: abs(volts[i] - target))


def run_tuning(c, ctx):
    prof = ctx["prof"]
    volts = ctx["volts"]
    cur_f = ctx["cur_f"]
    cur_v = ctx["cur_v"]
    results = {} if c.fresh else load_state(c.state)
    if results and c.max_age > 0:
        cutoff = time.time() - c.max_age * 86400
        stale = [k for k, r in results.items() if r.get("ts", 0) < cutoff]
        for k in stale:
            del results[k]
        if stale:
            log("discarded %d saved results older than %g days" % (len(stale), c.max_age))
    if results:
        log("resuming with %d saved results from %s" % (len(results), c.state))
    bk = load_backup(c.host)
    if bk and bk.get("in_progress"):
        write_backup(c.host, bk["original_f"], bk["original_v"], True)
    else:
        write_backup(c.host, cur_f, cur_v, True)
    c.base = (cur_f, cur_v)
    with LOCK:
        ACTIVE[c.host] = {"cfg": c, "base": (cur_f, cur_v), "restored": False}
    try:
        f = max(prof["f_min"], min(cur_f, prof["f_max"]))
        lo_v = nearest(volts, cur_v - 100)
        hi_v = nearest(volts, cur_v + 100)
        hint = nearest(volts, cur_v)
        found = None
        tries = 0
        while found is None and tries <= 4 and f >= prof["f_min"]:
            log("searching lowest passing voltage at %d MHz" % f)
            if tries == 0:
                found = lowest_passing(c, results, f, volts, lo_v, hi_v, hint)
            else:
                found = lowest_passing(c, results, f, volts, 0, len(volts) - 1)
            if found is None:
                log("no passing voltage at %d MHz, trying lower" % f)
                f -= c.freq_step
                tries += 1
        if found is None:
            log("could not find any stable setting")
        else:
            idx, r = found
            log("%d MHz needs %d mV: %.2f TH/s  %.2f J/TH  %.1f %s" % (f, volts[idx], r["hash"], r["jth"], T(r["temp"]), TU()))
            while r["temp"] < c.max_temp - c.temp_margin:
                f += c.freq_step
                if f > prof["f_max"]:
                    log("reached frequency ceiling for this board")
                    break
                log("searching lowest passing voltage at %d MHz" % f)
                nxt = lowest_passing(c, results, f, volts, max(0, idx - 2), len(volts) - 1, idx)
                if not nxt:
                    log("no passing voltage at %d MHz, stopping climb" % f)
                    break
                idx, r = nxt
                log("%d MHz needs %d mV: %.2f TH/s  %.2f J/TH  %.1f %s" % (f, volts[idx], r["hash"], r["jth"], T(r["temp"]), TU()))
            else:
                log("temperature within %.0f %s of limit, stopping climb" % (c.temp_margin * (1.8 if TEMP_F else 1), TU()))
    except Offline as e:
        log("aborting: %s" % e)
    finally:
        restore(c, cur_f, cur_v)
    return results


def worker(c, ctx, tag):
    TLS.tag = c.host if tag else ""
    try:
        return run_tuning(c, ctx)
    except Stop:
        return None


HOURS_PER_MONTH = 730


def kwh_month(watts):
    return watts * HOURS_PER_MONTH / 1000.0


def pct(before, after):
    if not before:
        return ""
    return "%+.1f%%" % ((after - before) / before * 100.0)


def print_compare(c, ctx, pick, applied):
    state = "applied" if applied else "recommended, not applied"
    print()
    print("%s (%s) before and after" % (c.host, ctx["name"]))
    print("%-11s %-14s %-14s %-8s" % ("", "Before", "After", "Change"))
    rows = [
        ("MHz / mV", "%d / %d" % (ctx["cur_f"], ctx["cur_v"]), "%d / %d" % (pick["f"], pick["v"]), ""),
        ("TH/s", "%.2f" % ctx["cur_hash"], "%.2f" % pick["hash"], pct(ctx["cur_hash"], pick["hash"])),
        ("J/TH", "%.2f" % ctx["cur_jth"], "%.2f" % pick["jth"], pct(ctx["cur_jth"], pick["jth"])),
        ("Watts", "%.1f" % ctx["cur_power"], "%.1f" % pick["power"], pct(ctx["cur_power"], pick["power"])),
        ("kWh / month", "%.1f" % kwh_month(ctx["cur_power"]), "%.1f" % kwh_month(pick["power"]), ""),
        ("Temp (%s)" % TU(), "%.1f" % T(ctx["cur_temp"]), "%.1f" % T(pick["temp"]), ""),
        ("Error %", "%.2f" % ctx["cur_err"], "%.2f" % pick["err"], ""),
    ]
    for label, b, a, ch in rows:
        print("%-11s %-14s %-14s %-8s" % (label, b, a, ch))
    print("after column: %s" % state)


def build_options(passing, c, pick):
    eff = min(passing, key=lambda r: r["jth"])
    fast = max(passing, key=lambda r: r["hash"])
    top = max(r["hash"] for r in passing)
    pool = [r for r in passing if r["hash"] >= top * c.saver_pct / 100.0]
    saver = min(pool, key=lambda r: r["power"])
    entries = []
    for label, r in (("Recommended (%s)" % c.goal, pick), ("Fastest", fast), ("Most efficient", eff), ("Power saver", saver)):
        for e in entries:
            if e[1]["f"] == r["f"] and e[1]["v"] == r["v"]:
                e[0].append(label)
                break
        else:
            entries.append([[label], r])
    return entries


def options_header():
    print("%-3s %-40s %-12s %-6s %-6s %-6s %-6s %-8s" % ("#", "Option", "MHz / mV", "TH/s", "Watts", "J/TH", "Temp " + TU(), "kWh/mo"))


def option_row(idx, label, r):
    print("%-3s %-40s %-12s %-6.2f %-6.1f %-6.2f %-6.1f %-8.1f" % (
        idx, label, "%d / %d" % (r["f"], r["v"]), r["hash"], r["power"], r["jth"], T(r["temp"]), kwh_month(r["power"])))


def pick_option(c, ctx, passing, entries):
    n = len(entries)
    keep_n = n + 1
    all_n = n + 2
    while True:
        print()
        print("options for %s (%s)" % (c.host, ctx["name"]))
        options_header()
        for i, (labels, r) in enumerate(entries, 1):
            option_row(str(i), " / ".join(labels), r)
        print("%-3s Keep current settings (%d / %d, %.2f TH/s, %.1f W)" % (
            keep_n, ctx["cur_f"], ctx["cur_v"], ctx["cur_hash"], ctx["cur_power"]))
        print("%-3s Show all passing combos" % all_n)
        try:
            ans = input("\nchoose an option [1]: ").strip()
        except EOFError:
            print("no answer given, keeping the current settings")
            return None
        if not ans:
            return entries[0][1]
        if not ans.isdigit():
            print("enter a number")
            continue
        k = int(ans)
        if 1 <= k <= n:
            return entries[k - 1][1]
        if k == keep_n:
            return None
        if k == all_n:
            allr = sorted(passing, key=lambda r: (r["f"], r["v"]))
            print()
            options_header()
            for j, r in enumerate(allr, 1):
                option_row(str(j), "", r)
            a2 = input("\nchoose a combo number (blank to go back): ").strip()
            if a2.isdigit() and 1 <= int(a2) <= len(allr):
                return allr[int(a2) - 1]
            continue
        print("not a valid option")


def confirm_and_apply(c, ctx, passing, chosen):
    tried = set()
    cand = chosen
    attempts = 0
    while cand is not None and attempts < 3:
        attempts += 1
        if not apply_with_retry(c.host, cand["f"], cand["v"]):
            print("could not reach the miner, set %d MHz / %d mV manually at http://%s" % (cand["f"], cand["v"], c.host))
            return None
        log("applied %d MHz / %d mV to %s" % (cand["f"], cand["v"], c.host))
        if c.confirm <= 0:
            return cand
        log("confirming %d MHz / %d mV for %s" % (cand["f"], cand["v"], dur(c.confirm)))
        try:
            settle(c)
            r = collect(c, c.confirm, c.max_error)
        except Offline:
            r = {"ok": False, "reason": "lost connection"}
        if r["ok"]:
            log("confirmed: %.2f TH/s  %.2f J/TH  err %.2f%%  %.1f %s  %.1f W" % (
                r["hash"], r["jth"], r["err"], T(r["temp"]), TU(), r["power"]))
            return cand
        log("confirmation failed (%s)" % r["reason"])
        tried.add((cand["f"], cand["v"]))
        lower = [x for x in passing if (x["f"], x["v"]) not in tried and x["power"] < cand["power"]]
        cand = choose(lower, c) if lower else None
        if cand is not None:
            log("falling back to %d MHz / %d mV" % (cand["f"], cand["v"]))
    log("could not confirm a setting, restoring %d MHz / %d mV" % (ctx["cur_f"], ctx["cur_v"]))
    apply_with_retry(c.host, ctx["cur_f"], ctx["cur_v"])
    return None


def write_summary(c, ctx, results, chosen, applied):
    if not c.json:
        return
    path = "bitaxe-tuner-%s-summary.json" % c.host.replace(".", "-").replace(":", "-")
    rows = []
    for k, r in results.items():
        f, v = [int(x) for x in k.split("/")]
        rows.append({"freq_mhz": f, "volt_mv": v, "pass": r["ok"], "stage": r.get("stage", ""), "th_s": round(r["hash"], 3),
                     "j_per_th": round(r["jth"], 2), "error_pct": round(r["err"], 2), "temp_c": round(r["temp"], 1),
                     "watts": round(r["power"], 1), "reason": r["reason"]})
    rows.sort(key=lambda x: (x["freq_mhz"], x["volt_mv"]))
    out = {
        "host": c.host, "name": ctx["name"], "applied": applied,
        "before": {"freq_mhz": ctx["cur_f"], "volt_mv": ctx["cur_v"], "th_s": round(ctx["cur_hash"], 3),
                   "j_per_th": round(ctx["cur_jth"], 2), "watts": round(ctx["cur_power"], 1)},
        "chosen": None if chosen is None else {"freq_mhz": chosen["f"], "volt_mv": chosen["v"], "th_s": round(chosen["hash"], 3),
                                               "j_per_th": round(chosen["jth"], 2), "watts": round(chosen["power"], 1),
                                               "temp_c": round(chosen["temp"], 1)},
        "results": rows,
    }
    try:
        with open(path, "w") as fh:
            json.dump(out, fh, indent=1)
        print("summary saved to %s" % path)
    except Exception as e:
        print("could not write %s: %s" % (path, e))


def report(c, ctx, results):
    cur_f = ctx["cur_f"]
    cur_v = ctx["cur_v"]
    rows = write_csv(c.csv, results)
    print()
    print("%s (%s)" % (c.host, ctx["name"]))
    print("%-6s %-6s %-5s %-7s %-8s %-8s %-7s %-7s %-7s" % ("MHz", "mV", "pass", "stage", "TH/s", "J/TH", "err%", "temp " + TU(), "watts"))
    for f, v, r in rows:
        print("%-6d %-6d %-5s %-7s %-8.2f %-8.2f %-7.2f %-7.1f %-7.1f" % (
            f, v, "yes" if r["ok"] else "no", r.get("stage", ""), r["hash"], r["jth"], r["err"], T(r["temp"]), r["power"]))
    passing = []
    for k, r in results.items():
        if r["ok"] and r.get("stage") == "full":
            e = dict(r)
            e["f"], e["v"] = [int(x) for x in k.split("/")]
            passing.append(e)
    if not passing:
        print("\nno fully validated combination, settings restored to %d MHz / %d mV" % (cur_f, cur_v))
        return
    pick = choose(passing, c)
    print("\nresults saved to %s" % c.csv)
    print("current setting: %d MHz / %d mV  %.2f TH/s  %.1f W  %.2f J/TH" % (
        cur_f, cur_v, ctx["cur_hash"], ctx["cur_power"], ctx["cur_jth"]))
    entries = build_options(passing, c, pick)
    if pick["hash"] < ctx["cur_hash"] * 0.97 and pick["jth"] >= ctx["cur_jth"]:
        print("warning: the recommendation is slower and no more efficient than your current setting")
        c.yes = False
    chosen = pick if c.yes else pick_option(c, ctx, passing, entries)
    if chosen is None:
        log("left %s at %d MHz / %d mV" % (c.host, cur_f, cur_v))
        write_summary(c, ctx, results, None, False)
        return (ctx["cur_power"], ctx["cur_power"], ctx["cur_hash"], ctx["cur_hash"])
    applied = False
    if chosen["f"] == cur_f and chosen["v"] == cur_v:
        applied = True
        log("%s is already at %d MHz / %d mV" % (c.host, cur_f, cur_v))
    else:
        final = confirm_and_apply(c, ctx, passing, chosen)
        if final is not None:
            chosen = final
            applied = True
    print_compare(c, ctx, chosen, applied)
    write_summary(c, ctx, results, chosen, applied)
    after_p = chosen["power"] if applied else ctx["cur_power"]
    after_h = chosen["hash"] if applied else ctx["cur_hash"]
    return (ctx["cur_power"], after_p, ctx["cur_hash"], after_h)


def watch_host(c):
    try:
        watch_loop(c)
    except Stop:
        return


def watch_loop(c):
    TLS.tag = c.host
    try:
        d = info(c.host)
    except Exception as e:
        print("cannot reach %s: %s" % (c.host, e))
        return
    model = d.get("ASICModel", "") or ""
    f_min = int(PROFILES.get(model, {"f_min": 300})["f_min"])
    target_f = int(d.get("frequency", 0) or 0)
    volt = int(d.get("coreVoltage", 0) or 0)
    if target_f <= 0 or volt <= 0:
        print("could not read current settings from %s" % c.host)
        return
    cur_f = target_f
    last_change = 0.0
    last_status = 0.0
    hot = 0
    log("watching %s (%s): keeping ASIC under %.0f %s, target %d MHz / %d mV" % (
        c.host, d.get("hostname", "?"), T(c.watch_temp), TU(), target_f, volt))
    while not STOP.is_set():
        try:
            d = info(c.host)
        except Exception:
            log("miner not answering")
            nap(30)
            continue
        t = d.get("temp", 0) or 0
        now = time.time()
        hot = hot + 1 if t >= c.watch_temp else 0
        emergency = t >= c.hard_temp
        if (hot >= 3 or emergency) and now - last_change >= (120 if emergency else c.watch_cooldown):
            new_f = max(f_min, cur_f - c.freq_step)
            if new_f < cur_f:
                log("temp %.1f %s, lowering to %d MHz" % (T(t), TU(), new_f))
                if apply_with_retry(c.host, new_f, volt):
                    cur_f = new_f
            else:
                log("temp %.1f %s but already at the minimum frequency" % (T(t), TU()))
            last_change = time.time()
            hot = 0
        elif cur_f < target_f and t <= c.watch_temp - c.watch_gap and now - last_change >= c.watch_cooldown * 3:
            new_f = min(target_f, cur_f + c.freq_step)
            log("temp %.1f %s, raising to %d MHz" % (T(t), TU(), new_f))
            if apply_with_retry(c.host, new_f, volt):
                cur_f = new_f
            last_change = time.time()
        if now - last_status >= 300:
            log("status: %d MHz, %.2f TH/s, %.1f W, %.1f %s" % (
                cur_f, (d.get("hashRate", 0) or 0) / 1000.0, d.get("power", 0) or 0, T(t), TU()))
            last_status = now
        nap(30)


def watch_all(hosts, args):
    cs = []
    for h in hosts:
        c = copy.copy(args)
        c.host = h
        cs.append(c)
    with ThreadPoolExecutor(max_workers=len(cs)) as ex:
        futs = [ex.submit(watch_host, c) for c in cs]
        for f in futs:
            f.result()


def revert_hosts(hosts):
    for h in hosts:
        bk = load_backup(h)
        if not bk:
            print("no backup found for %s" % h)
            continue
        print("restoring %s to %d MHz / %d mV (saved %s)" % (h, bk["original_f"], bk["original_v"], bk.get("time", "?")))
        if apply_with_retry(h, bk["original_f"], bk["original_v"]):
            write_backup(h, bk["original_f"], bk["original_v"], False)
            print("done")
        else:
            print("could not reach %s" % h)


def local_subnet():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    except Exception:
        ip = "192.168.1.1"
    finally:
        s.close()
    return str(ipaddress.ip_network(ip + "/24", strict=False))


def add_nmap_to_path():
    for d in (os.environ.get("ProgramFiles(x86)"), os.environ.get("ProgramFiles")):
        if d and os.path.isfile(os.path.join(d, "Nmap", "nmap.exe")):
            os.environ["PATH"] = os.path.join(d, "Nmap") + os.pathsep + os.environ.get("PATH", "")


def ensure_nmap():
    if sys.platform == "win32":
        add_nmap_to_path()
    if shutil.which("nmap"):
        return True
    print("nmap is not installed")
    if input("install nmap now? [y/N] ").strip().lower() != "y":
        return False
    if sys.platform == "darwin":
        if not shutil.which("brew"):
            print("Homebrew was not found, install it from https://brew.sh first")
            return False
        cmd = ["brew", "install", "nmap"]
    elif sys.platform == "win32":
        if not shutil.which("winget"):
            print("winget was not found, install nmap from https://nmap.org/download first")
            return False
        cmd = ["winget", "install", "-e", "--id", "Insecure.Nmap"]
    elif shutil.which("apt-get"):
        cmd = ["sudo", "apt-get", "install", "-y", "nmap"]
    elif shutil.which("dnf"):
        cmd = ["sudo", "dnf", "install", "-y", "nmap"]
    else:
        print("no supported package manager found")
        return False
    print("running: %s" % " ".join(cmd))
    try:
        subprocess.run(cmd, check=True)
    except Exception as e:
        print("install failed: %s" % e)
        return False
    if sys.platform == "win32":
        add_nmap_to_path()
    return bool(shutil.which("nmap"))


def nmap_hosts(subnet):
    out = subprocess.run(["nmap", "-Pn", "-n", "-T4", "--max-retries", "1", "--host-timeout", "3s",
                          "-p", "80", "--open", "-oG", "-", subnet],
                         capture_output=True, text=True, timeout=120).stdout
    hosts = []
    for line in out.splitlines():
        if line.startswith("Host:") and "80/open" in line:
            hosts.append(line.split()[1])
    return hosts


def python_hosts(subnet, timeout=0.5):
    def check(ip):
        s = socket.socket()
        s.settimeout(timeout)
        try:
            return ip if s.connect_ex((ip, 80)) == 0 else None
        except Exception:
            return None
        finally:
            s.close()
    ips = [str(h) for h in ipaddress.ip_network(subnet, strict=False).hosts()]
    with ThreadPoolExecutor(max_workers=200) as ex:
        return [ip for ip in ex.map(check, ips) if ip]


def probe(ip):
    try:
        d = http(ip, "/api/system/info", timeout=1.5)
    except Exception:
        return None
    if isinstance(d, dict) and d.get("ASICModel") and "hashRate" in d:
        d["ip"] = ip
        return d
    return None


def probe_many(ips):
    if not ips:
        return []
    with ThreadPoolExecutor(max_workers=64) as ex:
        return [d for d in ex.map(probe, ips) if d]


def ip_key(ip):
    return [int(x) for x in ip.split(".")]


def load_cache(path):
    try:
        with open(path) as fh:
            return [x for x in json.load(fh) if isinstance(x, str)]
    except Exception:
        return []


def save_cache(path, ips):
    try:
        with open(path, "w") as fh:
            json.dump(sorted(ips, key=ip_key), fh)
    except Exception:
        pass


def arp_ips(net):
    try:
        out = subprocess.run(["arp", "-an"], capture_output=True, text=True, timeout=5).stdout
    except Exception:
        return []
    ips = []
    for m in re.finditer(r"\((\d+\.\d+\.\d+\.\d+)\)", out):
        try:
            if ipaddress.ip_address(m.group(1)) in net:
                ips.append(m.group(1))
        except ValueError:
            continue
    return ips


def scan(subnet, rescan=False, use_nmap=False):
    subnet = subnet or local_subnet()
    net = ipaddress.ip_network(subnet, strict=False)
    cache_path = "bitaxe-tuner-hosts.json"
    found = []
    if not rescan:
        cached = [ip for ip in load_cache(cache_path) if ipaddress.ip_address(ip) in net]
        found = probe_many(sorted(set(cached) | set(arp_ips(net)), key=ip_key))
        answered = {d["ip"] for d in found}
        if found and all(ip in answered for ip in cached):
            log("found %d miner(s) from saved and recently seen addresses" % len(found))
        else:
            found = []
    if not found:
        ips = []
        if use_nmap and ensure_nmap():
            log("scanning %s with nmap" % subnet)
            try:
                ips = nmap_hosts(subnet)
            except Exception as e:
                log("nmap failed (%s), using built-in scan" % e)
                ips = python_hosts(subnet)
        else:
            log("scanning %s" % subnet)
            ips = python_hosts(subnet)
        found = probe_many(ips)
        if not found:
            log("nothing answered, retrying with a slower timeout")
            found = probe_many(python_hosts(subnet, timeout=1.5))
    found.sort(key=lambda d: ip_key(d["ip"]))
    if not found:
        print("no miners found on %s" % subnet)
        return []
    save_cache(cache_path, [d["ip"] for d in found])
    print()
    print("%-3s %-34s %-8s %-12s %-7s %-7s" % ("#", "IP (NAME)", "chip", "MHz / mV", "TH/s", "Watts"))
    for i, d in enumerate(found, 1):
        label = "%s (%s)" % (d["ip"], d.get("hostname", "?"))
        print("%-3d %-34s %-8s %-12s %-7.2f %-7.1f" % (
            i, label, d.get("ASICModel", "?"),
            "%s / %s" % (d.get("frequency", "?"), d.get("coreVoltage", "?")),
            (d.get("hashRate", 0) or 0) / 1000.0, d.get("power", 0) or 0))
    print("%-3s %s" % ("a", "All of the miners above"))
    print()
    answer = input("select a number, a comma list, or a for All (blank to cancel): ").strip().lower()
    if not answer:
        return []
    if answer in ("a", "all"):
        return [d["ip"] for d in found]
    picked = []
    for part in answer.split(","):
        part = part.strip()
        if part.isdigit() and 1 <= int(part) <= len(found):
            picked.append(found[int(part) - 1]["ip"])
        else:
            print("ignoring invalid choice: %s" % part)
    return picked


def main():
    p = argparse.ArgumentParser(description="Adaptive frequency and core voltage tuner for Bitaxe style miners")
    p.add_argument("hosts", nargs="*")
    p.add_argument("--host", action="append", default=[])
    p.add_argument("--scan", action="store_true")
    p.add_argument("--subnet", default="")
    p.add_argument("--rescan", action="store_true")
    p.add_argument("--nmap", action="store_true")
    p.add_argument("--sequential", action="store_true")
    p.add_argument("--no-restart", dest="restart", action="store_false")
    p.add_argument("--goal", choices=["balanced", "efficient", "fast"], default="balanced")
    p.add_argument("--freq-step", type=int, default=25)
    p.add_argument("--volt-step", type=int, default=10)
    p.add_argument("--freq-max", type=int, default=0)
    p.add_argument("--volt-max", type=int, default=0)
    p.add_argument("--settle", type=int, default=150)
    p.add_argument("--settle-min", type=int, default=45)
    p.add_argument("--screen", type=int, default=90)
    p.add_argument("--hold", type=int, default=300)
    p.add_argument("--poll", type=int, default=15)
    p.add_argument("--max-temp", type=float, default=66.0)
    p.add_argument("--hard-temp", type=float, default=69.0)
    p.add_argument("--hard-vr-temp", type=float, default=90.0)
    p.add_argument("--temp-margin", type=float, default=3.0)
    p.add_argument("--headroom", type=float, default=2.0)
    p.add_argument("--saver-pct", type=float, default=85.0)
    p.add_argument("--max-error", type=float, default=1.0)
    p.add_argument("--max-watts", type=float, default=0.0)
    p.add_argument("--min-input", type=float, default=None)
    p.add_argument("--state", default="")
    p.add_argument("--csv", default="")
    p.add_argument("--fresh", action="store_true")
    p.add_argument("--yes", action="store_true")
    p.add_argument("--revert", action="store_true", help="restore the settings saved before the last tune")
    p.add_argument("--watch", action="store_true", help="keep running and lower frequency when the ASIC runs hot")
    p.add_argument("--watch-temp", type=float, default=64.0, help="ASIC temperature limit in Celsius for watch mode")
    p.add_argument("--watch-cooldown", type=int, default=600, help="seconds between watch mode changes")
    p.add_argument("--watch-gap", type=float, default=6.0, help="degrees below the limit before stepping back up")
    p.add_argument("--max-age", type=float, default=7.0, help="discard saved results older than this many days, 0 keeps them")
    p.add_argument("--confirm", type=int, default=300, help="seconds to verify the applied setting, 0 skips")
    p.add_argument("--fine", action="store_true", help="use 5 mV voltage steps")
    p.add_argument("--dry-run", action="store_true", help="show the plan without changing anything")
    p.add_argument("--force", action="store_true", help="tune even if the miner is not hashing")
    p.add_argument("--json", action="store_true", help="write a JSON summary for each miner")
    p.add_argument("--fahrenheit", action="store_true", help="show temperatures in Fahrenheit, limit options stay in Celsius")
    args = p.parse_args()
    global TEMP_F
    TEMP_F = args.fahrenheit
    if args.fine:
        args.volt_step = 5

    hosts = list(args.hosts) + list(args.host)
    if args.scan:
        hosts += scan(args.subnet, args.rescan, args.nmap)
        if not hosts:
            sys.exit(0)
    if not hosts:
        p.error("give at least one miner IP or use --scan, for example: bitaxe-tuner.py 192.168.1.103")
    if len(hosts) > 1 and (args.state or args.csv):
        p.error("--state and --csv only work with a single miner")

    def on_signal(sig, frame):
        STOP.set()
        for st in list(ACTIVE.values()):
            restore(st["cfg"], st["base"][0], st["base"][1])
        os._exit(130)

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)
    if hasattr(signal, "SIGHUP"):
        signal.signal(signal.SIGHUP, on_signal)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, on_signal)

    if args.revert:
        revert_hosts(hosts)
        return
    keep_awake()
    if args.watch:
        watch_all(hosts, args)
        return

    ctxs = []
    for h in hosts:
        c = copy.copy(args)
        c.host = h
        safe = h.replace(".", "-").replace(":", "-")
        c.state = args.state or "bitaxe-tuner-%s.json" % safe
        c.csv = args.csv or "bitaxe-tuner-%s.csv" % safe
        ctx = prepare(c)
        if ctx:
            ctxs.append((c, ctx))
    if not ctxs:
        return

    multi = len(ctxs) > 1
    if multi and not args.sequential:
        log("tuning %d miners at the same time" % len(ctxs))
        with ThreadPoolExecutor(max_workers=len(ctxs)) as ex:
            futs = [ex.submit(worker, c, ctx, True) for c, ctx in ctxs]
            outs = [f.result() for f in futs]
    else:
        outs = [worker(c, ctx, multi) for c, ctx in ctxs]

    notify("Tuning finished on %d miner(s)" % len(ctxs))
    totals = []
    for (c, ctx), res in zip(ctxs, outs):
        if res is not None:
            t = report(c, ctx, res)
            if t:
                totals.append(t)
    if len(totals) > 1:
        bw = sum(t[0] for t in totals)
        aw = sum(t[1] for t in totals)
        bh = sum(t[2] for t in totals)
        ah = sum(t[3] for t in totals)
        print()
        print("all miners combined")
        print("%-11s %-14s %-14s %-8s" % ("", "Before", "After", "Change"))
        print("%-11s %-14s %-14s %-8s" % ("Watts", "%.1f" % bw, "%.1f" % aw, pct(bw, aw)))
        print("%-11s %-14s %-14s %-8s" % ("kWh / month", "%.1f" % kwh_month(bw), "%.1f" % kwh_month(aw), ""))
        print("%-11s %-14s %-14s %-8s" % ("TH/s", "%.2f" % bh, "%.2f" % ah, pct(bh, ah)))
        print("%-11s %-14s %-14s %-8s" % ("J/TH", "%.2f" % (bw / bh if bh else 0), "%.2f" % (aw / ah if ah else 0),
                                          pct(bw / bh if bh else 0, aw / ah if ah else 0)))


if __name__ == "__main__":
    main()
