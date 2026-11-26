#!/usr/bin/env python3
import argparse
import concurrent.futures
import ipaddress
import json
import os
import re
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.request
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

INFO_KEYS = (
    "hostname", "ASICModel", "boardVersion", "version", "frequency", "coreVoltage", "coreVoltageActual",
    "hashRate", "hashRate_1m", "hashRate_10m", "hashRate_1h", "expectedHashrate", "errorPercentage",
    "temp", "vrTemp", "power", "voltage", "current", "nominalVoltage", "fanspeed", "fanrpm",
    "sharesAccepted", "sharesRejected", "bestDiff", "bestSessionDiff", "uptimeSeconds", "wifiRSSI",
    "stratumURL", "stratumPort", "stratumUser", "isUsingFallbackStratum", "poolDifficulty", "overheat_mode",
    "stratumProtocol", "responseTime", "blockFound", "blockHeight", "networkDifficulty", "coinbaseOutputs",
)

COLUMNS = ("hash", "temp", "vr", "power", "volt", "freq", "core", "err", "rssi", "fan", "acc", "rej")

SCHEMA = (
    "CREATE TABLE IF NOT EXISTS samples (ts INTEGER, ip TEXT, hash REAL, temp REAL, vr REAL, power REAL, "
    "volt REAL, freq REAL, core REAL, err REAL, rssi REAL, fan REAL, acc REAL, rej REAL)",
    "CREATE INDEX IF NOT EXISTS idx_samples ON samples (ts, ip)",
    "CREATE TABLE IF NOT EXISTS events (ts INTEGER, ip TEXT, kind TEXT, detail TEXT)",
    "CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT)",
)

SOLO_MARKERS = ("solo", "public-pool.io", "ckpool", "nerdminers", "atlaspool", "noderunners")
POOLED_MARKERS = ("braiins", "slushpool", "foundry", "antpool", "f2pool", "viabtc", "luxor", "ocean", "binance",
                  "poolin", "btc.com", "emcd", "kryptex", "nicehash", "secpool", "spiderpool", "ultimuspool")
PUBLIC_POOL_PPLNS_PORTS = (13333, 14333, 23331, 23336)
CKPOOL_HOSTS = ("solo.ckpool.org", "eusolo.ckpool.org", "ausolo.ckpool.org")
ADDRESS_RE = re.compile(r"^(bc1|tb1|[13])[a-zA-HJ-NP-Z0-9]{25,87}$")
SAFE_ADDR_RE = re.compile(r"^[A-Za-z0-9]{25,90}$")
SUBSIDY_SATS = 312500000


RANGES = (300, 900, 3600, 21600, 86400, 604800, 2592000)


def split_host(h):
    if ":" in h:
        a, p = h.rsplit(":", 1)
        if p.isdigit():
            return a, int(p)
    return h, 80


def valid_host(h):
    try:
        a, p = split_host(h)
        ip = ipaddress.ip_address(a)
    except ValueError:
        return False
    return ip.version == 4 and (ip.is_private or ip.is_loopback) and 0 < p < 65536


def norm_host(h):
    a, p = split_host(h.strip())
    return a if p == 80 else "%s:%d" % (a, p)


def host_key(h):
    a, p = split_host(h)
    try:
        return (int(ipaddress.ip_address(a)), p)
    except ValueError:
        return (0, p)


def fetch(host, timeout=2.5):
    a, p = split_host(host)
    req = urllib.request.Request("http://%s:%d/api/system/info" % (a, p), headers={"User-Agent": "bitaxe-monitor"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            d = json.loads(r.read().decode("utf-8", "replace"))
    except Exception:
        return None
    if isinstance(d, dict) and "hashRate" in d and ("ASICModel" in d or "hostname" in d):
        return d
    return None


ERRS = {}


def tcp_open(ip, port=80, timeout=0.5):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect((ip, port))
        return True
    except OSError as e:
        ERRS[e.errno] = ERRS.get(e.errno, 0) + 1
        return False
    finally:
        s.close()


def local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"
    finally:
        s.close()


def arp_ips():
    try:
        out = subprocess.run(["arp", "-a"], capture_output=True, text=True, timeout=6).stdout
    except Exception:
        return []
    found = []
    for ip in re.findall(r"\b(\d{1,3}(?:\.\d{1,3}){3})\b", out):
        try:
            a = ipaddress.ip_address(ip)
        except ValueError:
            continue
        if a.is_private and not a.is_multicast and not a.is_loopback and not ip.endswith(".255") and not ip.endswith(".0"):
            found.append(ip)
    return sorted(set(found), key=lambda x: int(ipaddress.ip_address(x)))


def local_subnet():
    return str(ipaddress.ip_network(local_ip() + "/24", strict=False))


def classify_pool(info):
    url = str(info.get("stratumURL") or "").strip().lower()
    url = re.sub(r"^[a-z0-9+]+://", "", url).split("/")[0]
    host, port = url, info.get("stratumPort")
    if ":" in url:
        host, p = url.rsplit(":", 1)
        if p.isdigit():
            port = int(p)
    try:
        port = int(port)
    except (TypeError, ValueError):
        port = None
    user = str(info.get("stratumUser") or "").strip()
    first, _, worker = user.partition(".")
    address = first if ADDRESS_RE.match(first) else None
    account = None if address else (first or None)
    mode = "unknown"
    if host:
        if "public-pool.io" in host and port in PUBLIC_POOL_PPLNS_PORTS:
            mode = "pool"
        elif any(m in host for m in SOLO_MARKERS):
            mode = "solo"
        elif any(m in host for m in POOLED_MARKERS):
            mode = "pool"
    page = None
    kind = None
    if address and SAFE_ADDR_RE.match(address):
        if host == "public-pool.io" or host.endswith(".public-pool.io"):
            page = "https://web.public-pool.io/#/app/" + address
            kind = "public-pool"
        elif host in CKPOOL_HOSTS:
            page = "https://%s/users/%s" % (host, address)
            kind = "ckpool"
        elif "ocean" in host:
            page = "https://ocean.xyz/dashboard/" + address
    elif "braiins" in host:
        page = "https://pool.braiins.com/"
    return {"mode": mode, "host": host, "port": port, "address": address, "account": account,
            "worker": worker or None, "page": page, "stats_kind": kind}


def get_json(url, timeout=8):
    req = urllib.request.Request(url, headers={"User-Agent": "bitaxe-monitor"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8", "replace"))


def get_text(url, timeout=8):
    req = urllib.request.Request(url, headers={"User-Agent": "bitaxe-monitor"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace").strip()


def fetch_network():
    out = {}
    try:
        out["height"] = int(get_text("https://mempool.space/api/blocks/tip/height"))
    except Exception:
        pass
    try:
        d = get_json("https://mempool.space/api/v1/mining/hashrate/3d")
        out["hashrate"] = float(d["currentHashrate"])
        out["difficulty"] = float(d["currentDifficulty"])
    except Exception:
        pass
    try:
        out["usd"] = float(get_json("https://mempool.space/api/v1/prices")["USD"])
    except Exception:
        pass
    try:
        rewards = []
        for b in get_json("https://mempool.space/api/v1/blocks")[:10]:
            r = (b.get("extras") or {}).get("reward")
            if isinstance(r, (int, float)) and r > 0:
                rewards.append(float(r))
        if rewards:
            out["reward_sats"] = sum(rewards) / len(rewards)
    except Exception:
        pass
    if out:
        out["fetched"] = int(time.time())
    return out


def fetch_pool_stats(kind, host, addr):
    try:
        if kind == "public-pool":
            d = get_json("https://public-pool.io:40557/api/client/" + addr)
            return {"best": d.get("bestDifficulty"), "workers": d.get("workersCount")}
        if kind == "ckpool":
            d = get_json("https://%s/users/%s" % (host, addr))
            best = d.get("bestever", d.get("bestshare"))
            return {"best": best, "workers": d.get("workers"), "shares": d.get("shares"),
                    "hashrate": d.get("hashrate1hr") or d.get("hashrate1m"), "lastshare": d.get("lastshare")}
    except Exception:
        return None
    return None


def block_count(v):
    if v is True:
        return 1
    try:
        return max(0, int(float(v)))
    except (TypeError, ValueError):
        return 0


def num(d, k):
    try:
        return float(d.get(k))
    except (TypeError, ValueError):
        return None


def sample_row(ts, host, d):
    return (ts, host, num(d, "hashRate"), num(d, "temp"), num(d, "vrTemp"), num(d, "power"), num(d, "voltage"),
            num(d, "frequency"), num(d, "coreVoltage"), num(d, "errorPercentage"), num(d, "wifiRSSI"),
            num(d, "fanrpm"), num(d, "sharesAccepted"), num(d, "sharesRejected"))


class Monitor:
    def __init__(self, args):
        self.interval = max(2, args.interval)
        self.max_days = args.max_days
        self.hosts_file = args.hosts_file
        self.lock = threading.Lock()
        self.dblock = threading.Lock()
        self.stop = threading.Event()
        self.miners = {}
        self.removed = set()
        self.offline = args.offline
        self.network = {}
        self.pool_stats = {}
        self.scan_state = {"scanning": False, "found": 0, "done": 0, "total": 0, "last": None, "subnet": None}
        self.db = sqlite3.connect(args.db, check_same_thread=False)
        for stmt in SCHEMA:
            self.db.execute(stmt)
        self.db.commit()
        self.load_hosts()

    def load_hosts(self):
        try:
            with open(self.hosts_file) as f:
                d = json.load(f)
        except Exception:
            return
        for h in d.get("hosts", []):
            if valid_host(h):
                self.miners[norm_host(h)] = self.blank()
        self.removed = set(h for h in d.get("removed", []) if valid_host(h))

    def save_hosts(self):
        with self.lock:
            d = {"hosts": sorted(self.miners, key=host_key), "removed": sorted(self.removed, key=host_key)}
        try:
            with open(self.hosts_file, "w") as f:
                json.dump(d, f, indent=1)
        except OSError:
            pass

    def blank(self):
        return {"online": False, "fails": 0, "last_seen": None, "info": None}

    def add_host(self, h, explicit=False):
        if not valid_host(h):
            return False
        h = norm_host(h)
        with self.lock:
            if h in self.removed and not explicit:
                return False
            self.removed.discard(h)
            if h in self.miners:
                return False
            self.miners[h] = self.blank()
        self.save_hosts()
        return True

    def remove_host(self, h):
        h = norm_host(h)
        with self.lock:
            if h not in self.miners:
                return False
            del self.miners[h]
            self.removed.add(h)
        self.save_hosts()
        return True

    def start(self):
        threading.Thread(target=self.poll_loop, daemon=True).start()
        if not self.offline:
            threading.Thread(target=self.net_loop, daemon=True).start()
            threading.Thread(target=self.pool_loop, daemon=True).start()

    def kv_get(self, k):
        with self.dblock:
            r = self.db.execute("SELECT v FROM kv WHERE k = ?", (k,)).fetchone()
        return r[0] if r else None

    def kv_set(self, k, v):
        with self.dblock:
            self.db.execute("INSERT OR REPLACE INTO kv VALUES (?, ?)", (k, str(v)))
            self.db.commit()

    def add_event(self, ts, ip, kind, detail):
        with self.dblock:
            self.db.execute("INSERT INTO events VALUES (?, ?, ?, ?)", (ts, ip, kind, detail))
            self.db.commit()

    def track_block(self, h, d, ts):
        if "blockFound" not in d:
            return
        bc = block_count(d.get("blockFound"))
        prev = self.kv_get("blocks:" + h)
        if prev is None:
            self.kv_set("blocks:" + h, bc)
            if bc > 0:
                self.add_event(ts, h, "block", "reported by the miner when first seen")
        elif bc > int(prev):
            self.kv_set("blocks:" + h, bc)
            self.add_event(ts, h, "block", "block count rose from %s to %d" % (prev, bc))

    def ack_block(self, h):
        with self.lock:
            m = self.miners.get(norm_host(h))
            info = m["info"] if m else None
        if not info:
            return False
        self.kv_set("ack:" + norm_host(h), block_count(info.get("blockFound")))
        return True

    def net_loop(self):
        while not self.stop.is_set():
            n = fetch_network()
            if n:
                with self.lock:
                    self.network = n
            self.stop.wait(600 if n else 120)

    def pool_loop(self):
        while not self.stop.is_set():
            targets = {}
            with self.lock:
                for m in self.miners.values():
                    if m["info"]:
                        c = classify_pool(m["info"])
                        if c["address"] and c["stats_kind"]:
                            targets[(c["stats_kind"], c["host"], c["address"])] = True
            for kind, host, addr in targets:
                data = fetch_pool_stats(kind, host, addr)
                if data is not None:
                    with self.lock:
                        self.pool_stats[host + "|" + addr] = dict(data, ts=int(time.time()))
            self.stop.wait(120 if targets else 3)

    def poll_loop(self):
        last_prune = 0
        while not self.stop.is_set():
            started = time.time()
            try:
                self.poll_once()
                if started - last_prune > 3600:
                    last_prune = started
                    self.prune()
            except Exception as e:
                print("poll error: %s" % e, file=sys.stderr)
            self.stop.wait(max(0.5, self.interval - (time.time() - started)))

    def poll_once(self):
        with self.lock:
            hosts = list(self.miners)
        if not hosts:
            return
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(16, len(hosts))) as ex:
            results = list(ex.map(lambda h: (h, fetch(h)), hosts))
        ts = int(time.time())
        rows = []
        fresh = []
        with self.lock:
            for h, d in results:
                m = self.miners.get(h)
                if m is None:
                    continue
                if d is None:
                    m["fails"] += 1
                    if m["fails"] >= 2:
                        m["online"] = False
                else:
                    m["fails"] = 0
                    m["online"] = True
                    m["last_seen"] = ts
                    m["info"] = {k: d.get(k) for k in INFO_KEYS if k in d}
                    rows.append(sample_row(ts, h, d))
                    fresh.append((h, d))
        for h, d in fresh:
            try:
                self.track_block(h, d, ts)
            except Exception as e:
                print("block tracking error: %s" % e, file=sys.stderr)
        if rows:
            with self.dblock:
                self.db.executemany("INSERT INTO samples VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
                self.db.commit()

    def prune(self):
        cutoff = int(time.time()) - self.max_days * 86400
        with self.dblock:
            self.db.execute("DELETE FROM samples WHERE ts < ?", (cutoff,))
            self.db.commit()

    def state(self):
        with self.lock:
            miners = [
                {"ip": h, "online": m["online"], "last_seen": m["last_seen"], "info": m["info"]}
                for h, m in sorted(self.miners.items(), key=lambda kv: host_key(kv[0]))
            ]
            scan = dict(self.scan_state)
            network = dict(self.network)
            stats = dict(self.pool_stats)
        groups = {}
        for m in miners:
            info = m["info"]
            if info:
                c = classify_pool(info)
                m["pool"] = c
                m["block"] = {
                    "count": block_count(info.get("blockFound")),
                    "acked": block_count(self.kv_get("ack:" + m["ip"])),
                    "reports": "blockFound" in info,
                }
                key = (c["host"], c["port"], c["address"] or c["account"])
                g = groups.setdefault(key, dict(c, miners=[], stats=stats.get("%s|%s" % (c["host"], c["address"]))))
                g["miners"].append(m["ip"])
        if not network.get("difficulty"):
            for m in miners:
                try:
                    nd = float((m["info"] or {}).get("networkDifficulty"))
                except (TypeError, ValueError):
                    continue
                if nd > 0:
                    network["difficulty"] = nd
                    network.setdefault("hashrate", nd * 4294967296 / 600)
                    break
        with self.dblock:
            ev = self.db.execute("SELECT ts, ip, kind, detail FROM events ORDER BY ts DESC LIMIT 20").fetchall()
        events = [{"ts": e[0], "ip": e[1], "kind": e[2], "detail": e[3]} for e in ev]
        return {"now": int(time.time()), "interval": self.interval, "scan": scan, "miners": miners,
                "network": network, "pools": list(groups.values()), "events": events, "offline": self.offline,
                "tuning": self.tuner.tuning_ips() if getattr(self, "tuner", None) else []}

    def history(self, rng):
        rng = max(60, min(int(rng), self.max_days * 86400))
        now = int(time.time())
        start = now - rng
        bucket = max(self.interval, rng // 240)
        sql = (
            "SELECT ip, (ts/?)*? AS b, AVG(hash), AVG(temp), AVG(vr), AVG(power), AVG(volt), AVG(freq), AVG(core), "
            "AVG(err), AVG(rssi), AVG(fan), MAX(acc), MAX(rej) FROM samples WHERE ts >= ? GROUP BY ip, b ORDER BY b"
        )
        with self.dblock:
            rows = self.db.execute(sql, (bucket, bucket, start)).fetchall()
        out = {}
        for r in rows:
            s = out.setdefault(r[0], {"t": [], **{c: [] for c in COLUMNS}})
            s["t"].append(r[1])
            for i, c in enumerate(COLUMNS):
                v = r[2 + i]
                s[c].append(None if v is None else round(v, 3))
        return {"start": start, "end": now, "bucket": bucket, "range": rng, "miners": out}

    def scan(self, subnet=None):
        try:
            net = ipaddress.ip_network(subnet or local_subnet(), strict=False)
        except ValueError:
            return "bad subnet"
        if net.version != 4 or net.num_addresses > 1024 or not (net.is_private or net.is_loopback):
            return "subnet must be a private IPv4 range of at most 1024 addresses"
        with self.lock:
            if self.scan_state["scanning"]:
                return "scan already running"
            self.scan_state.update(scanning=True, found=0, done=0, total=net.num_addresses, subnet=str(net), local_ip=local_ip())
        threading.Thread(target=self.scan_run, args=(net,), daemon=True).start()
        return None

    def scan_run(self, net):
        ips = [str(i) for i in net.hosts()]
        direct = set(arp_ips())
        with self.lock:
            direct |= set(h for h in self.miners if ":" not in h)
        candidates = sorted(set(ips) | direct, key=lambda x: int(ipaddress.ip_address(x)))
        with self.lock:
            self.scan_state["total"] = len(candidates)
        print("scanning %s from %s (%d addresses)" % (net, self.scan_state.get("local_ip"), len(candidates)))

        def check(ip):
            d = None
            if ip in direct or tcp_open(ip, 80, 1.0) or tcp_open(ip, 80, 1.0):
                d = fetch(ip, 3.0) or fetch(ip, 3.0)
            with self.lock:
                self.scan_state["done"] += 1
            return ip, d

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=48) as ex:
                for ip, d in ex.map(check, candidates):
                    if d is not None:
                        self.add_host(ip)
                        with self.lock:
                            self.scan_state["found"] += 1
                        print("found miner at %s (%s)" % (ip, d.get("hostname", "?")))
        finally:
            with self.lock:
                self.scan_state["scanning"] = False
                self.scan_state["last"] = int(time.time())
                found = self.scan_state["found"]
            print("scan finished, %d miner(s) found" % found)
            if ERRS:
                print("connection errors seen: %s" % ", ".join("%s x%d" % (os.strerror(k) if k else "timeout", v) for k, v in sorted(ERRS.items(), key=lambda kv: -kv[1])[:4]))
            ERRS.clear()


TUNER_URL = "https://raw.githubusercontent.com/JustinOros/bitaxe-tuner/main/bitaxe-tuner.py"


class TunerMgr:
    def __init__(self, mon):
        self.mon = mon
        self.dir = os.path.dirname(os.path.abspath(__file__))
        self.path = os.path.join(self.dir, "bitaxe-tuner.py")
        self.jobs = {}
        self.lock = threading.Lock()
        self.pfile = os.path.join(self.dir, "bitaxe-monitor-profiles.json")
        self.applies = {}

    def load_profiles(self):
        try:
            with open(self.pfile) as fh:
                d = json.load(fh)
            return d if isinstance(d, dict) else {}
        except Exception:
            return {}

    def state_path(self, ip):
        return os.path.join(self.dir, "bitaxe-tuner-%s.json" % ip.replace(".", "-").replace(":", "-"))

    def build_profiles(self, ip, max_temp=None):
        try:
            with open(self.state_path(ip)) as fh:
                res = json.load(fh)
        except Exception:
            return "no saved tune results found for that miner"
        passing = []
        for k, r in res.items():
            if r.get("ok") and r.get("stage") == "full":
                try:
                    f, v = [int(x) for x in k.split("/")]
                except ValueError:
                    continue
                passing.append({"f": f, "v": v, "hash": float(r["hash"]), "jth": float(r["jth"]), "power": float(r["power"]),
                                "temp": float(r["temp"]), "err": float(r.get("err", 0))})
        if not passing:
            return "no fully tested settings in the saved results yet"
        data = self.load_profiles()
        old = data.get(ip) or {}
        mt = float(max_temp if max_temp else old.get("max_temp") or 66.0)
        roomy = [r for r in passing if r["temp"] <= mt - 2.0]
        pool = roomy or passing
        eff = min(pool, key=lambda r: r["jth"])
        near = [r for r in pool if r["jth"] <= eff["jth"] * 1.03]
        picks = {"balanced": max(near, key=lambda r: r["hash"]), "efficient": eff, "fast": max(pool, key=lambda r: r["hash"])}
        with self.mon.lock:
            m = self.mon.miners.get(ip) or {}
        name = ((m.get("info") or {}).get("hostname")) or old.get("name") or ip
        data[ip] = {"name": name, "max_temp": mt, "updated": int(time.time()), "tested": len(passing), "profiles": picks}
        try:
            tmp = self.pfile + ".tmp"
            with open(tmp, "w") as fh:
                json.dump(data, fh, indent=1)
            os.replace(tmp, self.pfile)
        except Exception as e:
            return "could not save profiles: %s" % e
        return None

    def apply_profile(self, ip, goal):
        if ip not in self.mon.miners:
            return "that miner is not being monitored"
        if self.running(ip):
            return "a tuner job is running for that miner"
        if (self.applies.get(ip) or {}).get("status") == "applying":
            return "a profile is already being applied"
        pr = ((self.load_profiles().get(ip) or {}).get("profiles") or {}).get(goal)
        if not pr:
            return "no %s profile saved for that miner" % goal
        f, v = int(pr["f"]), int(pr["v"])
        st = {"status": "applying", "goal": goal, "f": f, "v": v, "ts": time.time(), "msg": "Applying %d MHz / %d mV" % (f, v)}
        self.applies[ip] = st

        def call(path, method, body):
            req = urllib.request.Request("http://%s%s" % (ip, path), data=json.dumps(body).encode(), method=method,
                                         headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=8) as r:
                r.read()

        def work():
            try:
                call("/api/system", "PATCH", {"frequency": f, "coreVoltage": v})
                time.sleep(1)
                try:
                    call("/api/system/restart", "POST", {})
                except Exception:
                    pass
                time.sleep(15)
                end = time.time() + 150
                d = None
                while time.time() < end:
                    d = fetch(ip, 4.0)
                    if d is not None:
                        break
                    time.sleep(4)
                if d is not None and int(d.get("frequency", 0)) == f and int(d.get("coreVoltage", 0)) == v:
                    st.update(status="done", msg="Applied %d MHz / %d mV" % (f, v))
                else:
                    st.update(status="failed", msg="The miner did not confirm the new settings")
            except Exception as e:
                st.update(status="failed", msg="Could not apply: %s" % e)
            st["ts"] = time.time()

        threading.Thread(target=work, daemon=True).start()
        return None

    def available(self):
        return os.path.exists(self.path)

    def download(self):
        try:
            req = urllib.request.Request(TUNER_URL, headers={"User-Agent": "bitaxe-monitor"})
            with urllib.request.urlopen(req, timeout=20) as r:
                data = r.read()
            if b"bitaxe" not in data[:4000].lower():
                return "the downloaded file did not look like the tuner"
            with open(self.path, "wb") as fh:
                fh.write(data)
            return None
        except Exception as e:
            return "download failed: %s" % e

    def running(self, ip):
        j = self.jobs.get(ip)
        return bool(j and j["proc"].poll() is None)

    def tuning_ips(self):
        with self.lock:
            return [ip for ip, j in self.jobs.items() if j["proc"].poll() is None and j["mode"] in ("tune", "watch")]

    def start(self, ip, mode, o):
        if ip not in self.mon.miners:
            return "that miner is not being monitored"
        if not self.available():
            return "bitaxe-tuner.py was not found next to the monitor"
        if self.running(ip):
            return "a job is already running for that miner"

        def num(k, lo, hi, default, cast=float):
            try:
                v = cast(o.get(k, default))
            except (TypeError, ValueError):
                return default
            return max(lo, min(hi, v))

        safe = ip.replace(".", "-").replace(":", "-")
        cmd = [sys.executable, "-u", self.path, ip]
        if mode == "revert":
            cmd.append("--revert")
        elif mode == "watch":
            cmd += ["--watch", "--watch-temp", "%g" % num("watch_temp", 40, 80, 64.0)]
        else:
            goal = o.get("goal")
            if goal not in ("balanced", "efficient", "fast"):
                goal = "balanced"
            cmd += ["--goal", goal, "--max-temp", "%g" % num("max_temp", 40, 80, 66.0)]
            prof = o.get("length")
            presets = {"quick": (90, 45, 60, 180, 120), "normal": (150, 45, 90, 300, 300), "thorough": (180, 60, 120, 600, 300)}
            st = presets.get(prof, presets["normal"])
            cmd += ["--settle", str(st[0]), "--settle-min", str(st[1]), "--screen", str(st[2]), "--hold", str(st[3]), "--confirm", str(st[4])]
            if o.get("fine"):
                cmd.append("--fine")
            if o.get("fresh"):
                cmd.append("--fresh")
            mw = num("max_watts", 0, 200, 0.0)
            if mw > 0:
                cmd += ["--max-watts", "%g" % mw]
            fm = num("freq_max", 0, 2000, 0, int)
            if fm > 0:
                cmd += ["--freq-max", str(fm)]
            vm = num("volt_max", 0, 1500, 0, int)
            if vm > 0:
                cmd += ["--volt-max", str(vm)]
            if mode == "preview":
                cmd.append("--dry-run")
            else:
                cmd.append("--yes")
        env = dict(os.environ, PYTHONUTF8="1", PYTHONUNBUFFERED="1")
        kw = {}
        if os.name == "nt":
            kw["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        try:
            proc = subprocess.Popen(cmd, cwd=self.dir, env=env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, text=True, encoding="utf-8", errors="replace", **kw)
        except Exception as e:
            return "could not start the tuner: %s" % e
        job = {"proc": proc, "mode": mode, "lines": [], "started": time.time(), "ended": None, "stopped": False,
               "max_temp": num("max_temp", 40, 80, 66.0),
               "state": os.path.join(self.dir, "bitaxe-tuner-%s.json" % safe)}
        with self.lock:
            self.jobs[ip] = job

        def reader():
            try:
                for line in proc.stdout:
                    line = line.rstrip("\r\n")
                    with self.lock:
                        job["lines"].append(line)
                        if len(job["lines"]) > 800:
                            del job["lines"][:200]
            except Exception:
                pass
            proc.wait()
            job["ended"] = time.time()
            if job["mode"] == "tune" and proc.returncode == 0:
                self.build_profiles(ip, job["max_temp"])

        threading.Thread(target=reader, daemon=True).start()
        return None

    def stop(self, ip):
        j = self.jobs.get(ip)
        if not j or j["proc"].poll() is not None:
            return False
        j["stopped"] = True
        try:
            if os.name == "nt":
                import signal as _sg
                j["proc"].send_signal(_sg.CTRL_BREAK_EVENT)
            else:
                import signal as _sg
                j["proc"].send_signal(_sg.SIGINT)
        except Exception:
            pass

        def killer():
            try:
                j["proc"].wait(timeout=90)
            except Exception:
                try:
                    j["proc"].kill()
                except Exception:
                    pass

        threading.Thread(target=killer, daemon=True).start()
        return True

    def stop_all(self):
        procs = []
        for ip in list(self.jobs):
            if self.stop(ip):
                procs.append(self.jobs[ip]["proc"])
        end = time.time() + 60
        for pr in procs:
            try:
                pr.wait(timeout=max(1, end - time.time()))
            except Exception:
                pass

    def view(self):
        out = {}
        with self.lock:
            items = list(self.jobs.items())
        for ip, j in items:
            rc = j["proc"].poll()
            res = {}
            try:
                with open(j["state"]) as fh:
                    res = json.load(fh)
            except Exception:
                pass
            status = "running" if rc is None else ("stopped" if j["stopped"] else ("done" if rc == 0 else "failed"))
            with self.lock:
                lines = j["lines"][-400:]
            out[ip] = {"mode": j["mode"], "status": status, "rc": rc, "started": j["started"], "ended": j["ended"],
                       "lines": lines, "results": res if j["mode"] in ("tune", "preview") else {}}
        return {"available": self.available(), "jobs": out, "profiles": self.load_profiles(), "applies": dict(self.applies)}


def make_handler(mon, page):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):
            pass

        def send(self, code, body, ctype="application/json"):
            if not isinstance(body, bytes):
                body = body.encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", ctype + "; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def js(self, obj, code=200):
            self.send(code, json.dumps(obj))

        def body(self):
            try:
                n = int(self.headers.get("Content-Length") or 0)
                return json.loads(self.rfile.read(n).decode("utf-8")) if n else {}
            except Exception:
                return {}

        def do_GET(self):
            u = urlparse(self.path)
            if u.path in ("/", "/index.html"):
                self.send(200, page, "text/html")
            elif u.path == "/api/state":
                self.js(mon.state())
            elif u.path == "/api/tuner":
                self.js(mon.tuner.view())
            elif u.path == "/api/history":
                q = parse_qs(u.query)
                try:
                    rng = int(q.get("range", ["3600"])[0])
                except ValueError:
                    rng = 3600
                self.js(mon.history(rng))
            else:
                self.send(404, "not found", "text/plain")

        def do_POST(self):
            u = urlparse(self.path)
            b = self.body()
            if u.path == "/api/scan":
                err = mon.scan(b.get("subnet") or None)
                self.js({"ok": err is None, "error": err}, 200 if err is None else 400)
            elif u.path == "/api/add":
                h = str(b.get("host", "")).strip()
                if not valid_host(h):
                    self.js({"ok": False, "error": "enter a private IPv4 address, optionally with :port"}, 400)
                    return
                if fetch(norm_host(h)) is None:
                    self.js({"ok": False, "error": "no Bitaxe answered at that address"}, 400)
                    return
                mon.add_host(h, explicit=True)
                self.js({"ok": True})
            elif u.path == "/api/tuner/start":
                err = mon.tuner.start(str(b.get("host", "")), str(b.get("mode", "tune")), b.get("opts") or {})
                self.js({"ok": err is None, "error": err}, 200 if err is None else 400)
            elif u.path == "/api/profiles/apply":
                err = mon.tuner.apply_profile(str(b.get("host", "")), str(b.get("goal", "")))
                self.js({"ok": err is None, "error": err}, 200 if err is None else 400)
            elif u.path == "/api/profiles/rebuild":
                h = str(b.get("host", ""))
                err = mon.tuner.build_profiles(h, b.get("max_temp")) if h in mon.miners else "that miner is not being monitored"
                self.js({"ok": err is None, "error": err}, 200 if err is None else 400)
            elif u.path == "/api/tuner/stop":
                self.js({"ok": mon.tuner.stop(str(b.get("host", "")))})
            elif u.path == "/api/tuner/download":
                err = mon.tuner.download()
                self.js({"ok": err is None, "error": err}, 200 if err is None else 400)
            elif u.path == "/api/ack":
                self.js({"ok": mon.ack_block(str(b.get("host", "")))})
            elif u.path == "/api/remove":
                self.js({"ok": mon.remove_host(str(b.get("host", "")))})
            else:
                self.send(404, "not found", "text/plain")

    return Handler


def raise_fd_limit():
    try:
        import resource
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        want = 4096 if hard == resource.RLIM_INFINITY else min(4096, hard)
        if soft < want:
            resource.setrlimit(resource.RLIMIT_NOFILE, (want, hard))
    except Exception:
        pass


def main():
    raise_fd_limit()
    p = argparse.ArgumentParser(description="Live dashboard for Bitaxe and other AxeOS miners")
    p.add_argument("hosts", nargs="*", help="miner IP addresses to add (optional, the network is scanned otherwise)")
    p.add_argument("--port", type=int, default=8765, help="web page port (default 8765)")
    p.add_argument("--lan", action="store_true", help="let other devices on the network open the page")
    p.add_argument("--interval", type=int, default=5, help="seconds between polls (default 5)")
    p.add_argument("--subnet", help="network to scan, for example 192.168.1.0/24")
    p.add_argument("--no-scan", action="store_true", help="never scan, only use the saved and listed miners")
    p.add_argument("--rescan", type=int, default=1800, help="seconds between automatic rescans (default 30 minutes)")
    p.add_argument("--no-browser", action="store_true", help="do not open the page in a browser")
    p.add_argument("--offline", action="store_true", help="never contact the internet (no network odds, no pool pages data)")
    p.add_argument("--db", default="bitaxe-monitor.db", help="history database file")
    p.add_argument("--hosts-file", default="bitaxe-monitor-hosts.json", help="saved miner list")
    p.add_argument("--max-days", type=int, default=30, help="days of history to keep (default 30)")
    args = p.parse_args()

    page = INDEX_HTML
    mon = Monitor(args)
    mon.tuner = TunerMgr(mon)
    for h in args.hosts:
        if not mon.add_host(h, explicit=True) and not valid_host(h):
            print("skipping %s, not a private IPv4 address" % h)
    mon.start()
    if not mon.miners and not args.no_scan:
        mon.scan(args.subnet)

    if not args.no_scan:
        def rescan_loop():
            while not mon.stop.wait(args.rescan):
                mon.scan(args.subnet)
        threading.Thread(target=rescan_loop, daemon=True).start()

    bind = "0.0.0.0" if args.lan else "127.0.0.1"
    server = None
    port = args.port
    for attempt in range(10):
        try:
            server = ThreadingHTTPServer((bind, port), make_handler(mon, page))
            break
        except OSError:
            port += 1
    if server is None:
        print("could not open a port starting at %d" % args.port)
        sys.exit(1)
    server.daemon_threads = True

    url = "http://127.0.0.1:%d/" % port
    print("bitaxe-monitor is running")
    print("  open:    %s" % url)
    if args.lan:
        print("  network: http://%s:%d/" % (local_ip(), port))
    else:
        print("  add --lan to open it from your phone or another computer")
    print("  history: %s" % os.path.abspath(args.db))
    print("  press Ctrl+C to stop")
    if not args.no_browser:
        threading.Timer(0.8, webbrowser.open, [url]).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        if mon.tuner.tuning_ips():
            print("stopping the tuner and restoring miner settings, please wait")
        mon.tuner.stop_all()
        mon.stop.set()
        server.server_close()


INDEX_HTML = r'''<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="icon" href="data:,">
<title>Bitaxe Monitor</title>
<style>
:root{--ov:rgba(255,255,255,.07);--gridl:rgba(255,255,255,.03);--gridc:rgba(140,165,255,.12);--cross:rgba(255,255,255,.35);--tipbg:rgba(8,12,26,.94);--scrim:rgba(3,6,14,.78);--sheet:linear-gradient(180deg,rgba(20,29,54,.97),rgba(11,16,32,.97));--hero:linear-gradient(135deg,#fff,#22d3ee);--bg:#060912;--panel:rgba(17,24,44,.66);--panel2:rgba(26,36,64,.55);--line:rgba(125,155,255,.15);--txt:#e8ecff;--dim:#8f9bc4;--cyan:#22d3ee;--orange:#f7931a;--green:#34d399;--red:#fb7185;--amber:#fbbf24;--violet:#a78bfa}
*{box-sizing:border-box}
html,body{margin:0;min-height:100%;background:var(--bg);color:var(--txt);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;-webkit-font-smoothing:antialiased}
body::before{content:"";position:fixed;inset:0;z-index:-2;background:radial-gradient(900px 600px at 8% -10%,rgba(34,211,238,.17),transparent 60%),radial-gradient(800px 600px at 100% 0%,rgba(167,139,250,.15),transparent 60%),radial-gradient(900px 700px at 50% 125%,rgba(247,147,26,.11),transparent 60%)}
html[data-theme=light]{--ov:rgba(20,35,90,.09);--gridl:rgba(30,50,120,.05);--gridc:rgba(40,60,130,.14);--cross:rgba(20,30,70,.4);--tipbg:rgba(255,255,255,.97);--scrim:rgba(220,228,245,.78);--sheet:linear-gradient(180deg,rgba(255,255,255,.98),rgba(238,243,253,.98));--hero:linear-gradient(135deg,#12183a,#0891b2);--bg:#eef2fb;--panel:rgba(255,255,255,.78);--panel2:rgba(238,243,255,.85);--line:rgba(40,60,130,.16);--txt:#12183a;--dim:#5a6690;--cyan:#0891b2;--green:#059669;--red:#e11d48;--amber:#d97706;--violet:#7c3aed;--orange:#ea7a00}
html[data-theme=light] body::before{background:radial-gradient(900px 600px at 8% -10%,rgba(34,211,238,.22),transparent 60%),radial-gradient(800px 600px at 100% 0%,rgba(167,139,250,.2),transparent 60%),radial-gradient(900px 700px at 50% 125%,rgba(247,147,26,.14),transparent 60%)}
html[data-theme=light] .card:hover{box-shadow:0 14px 40px rgba(40,60,130,.18),0 0 30px rgba(34,211,238,.12)}
html[data-theme=light] .tip{box-shadow:0 10px 30px rgba(40,60,130,.22)}
html[data-theme=light] .sheet{box-shadow:0 30px 80px rgba(40,60,130,.28)}
body::after{content:"";position:fixed;inset:0;z-index:-1;pointer-events:none;background-image:linear-gradient(var(--gridl) 1px,transparent 1px),linear-gradient(90deg,var(--gridl) 1px,transparent 1px);background-size:46px 46px;-webkit-mask-image:radial-gradient(ellipse at 50% 0%,#000 0%,transparent 72%);mask-image:radial-gradient(ellipse at 50% 0%,#000 0%,transparent 72%)}
.wrap{max-width:1440px;margin:0 auto;padding:18px 20px 60px}
.top{display:flex;align-items:center;gap:14px;flex-wrap:wrap;margin-bottom:18px}
.brand{display:flex;align-items:center;gap:11px;font-weight:800;letter-spacing:.14em;font-size:15px}
.brand svg{width:30px;height:30px;filter:drop-shadow(0 0 8px rgba(247,147,26,.7))}
.brand small{display:block;font-weight:500;letter-spacing:.08em;color:var(--dim);font-size:11px;margin-top:2px}
.sp{flex:1}
.live{display:flex;align-items:center;gap:8px;font-size:12px;color:var(--dim);padding:7px 12px;border:1px solid var(--line);border-radius:999px;background:var(--panel)}
.dot{width:8px;height:8px;border-radius:50%;background:var(--green);box-shadow:0 0 0 0 rgba(52,211,153,.7);animation:pulse 1.8s infinite}
.dot.off{background:var(--red);animation:none;box-shadow:none}
@keyframes pulse{0%{box-shadow:0 0 0 0 rgba(52,211,153,.6)}70%{box-shadow:0 0 0 9px rgba(52,211,153,0)}100%{box-shadow:0 0 0 0 rgba(52,211,153,0)}}
.btn{appearance:none;border:1px solid var(--line);background:var(--panel);color:var(--txt);padding:8px 14px;border-radius:10px;font-size:13px;font-weight:600;cursor:pointer;transition:.18s}
.btn:hover{border-color:rgba(34,211,238,.6);box-shadow:0 0 18px rgba(34,211,238,.18)}
.btn.pri{background:linear-gradient(135deg,rgba(34,211,238,.28),rgba(167,139,250,.26));border-color:rgba(34,211,238,.45)}
.seg{display:inline-flex;border:1px solid var(--line);border-radius:10px;overflow:hidden;background:var(--panel)}
.seg{max-width:100%;overflow-x:auto}
.seg button{white-space:nowrap;appearance:none;border:0;background:transparent;color:var(--dim);padding:8px 12px;font-size:12.5px;font-weight:600;cursor:pointer}
.seg button.on{color:var(--txt);background:linear-gradient(135deg,rgba(34,211,238,.32),rgba(167,139,250,.3))}
.add{display:flex;gap:6px}
.icon{width:38px;height:36px;padding:0;display:inline-flex;align-items:center;justify-content:center}
.icon svg{width:18px;height:18px;fill:none;stroke:currentColor;stroke-width:2;stroke-linecap:round;stroke-linejoin:round}
.icon .sun{display:none}
html[data-theme=light] .icon .sun{display:block}
html[data-theme=light] .icon .moon{display:none}
.add input{width:150px;background:var(--panel);border:1px solid var(--line);color:var(--txt);padding:8px 11px;border-radius:10px;font-size:13px;outline:none}
.add input:focus{border-color:rgba(34,211,238,.6)}
.scanbar{height:3px;border-radius:3px;background:var(--ov);overflow:hidden;margin:-8px 0 14px;opacity:0;transition:.3s}
.scanbar.on{opacity:1}
.scanbar i{display:block;height:100%;width:0;background:linear-gradient(90deg,var(--cyan),var(--violet));transition:width .25s}
.msg{font-size:12.5px;color:var(--amber);min-height:16px;margin:-8px 0 8px}
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:12px;margin-bottom:14px}
.kpi{position:relative;overflow:hidden;background:var(--panel);border:1px solid var(--line);border-radius:16px;padding:14px 15px 10px;backdrop-filter:blur(10px);-webkit-backdrop-filter:blur(10px)}
.kpi .l{font-size:11px;letter-spacing:.11em;text-transform:uppercase;color:var(--dim);font-weight:700}
.kpi .v{font-size:30px;font-weight:800;margin-top:5px;font-variant-numeric:tabular-nums;letter-spacing:-.01em}
.kpi .v small{font-size:13px;color:var(--dim);font-weight:600;margin-left:4px}
.kpi .s{font-size:12px;color:var(--dim);margin-top:2px;min-height:16px}
.kpi.sp2{padding-bottom:50px}
.kpi canvas{position:absolute;left:0;right:0;bottom:0;width:100%;height:42px;opacity:.9;pointer-events:none}
.kpi.hero .v{background:var(--hero);-webkit-background-clip:text;background-clip:text;color:transparent}
.panel{background:var(--panel);border:1px solid var(--line);border-radius:18px;padding:16px;backdrop-filter:blur(10px);-webkit-backdrop-filter:blur(10px);margin-bottom:14px}
.ph{display:flex;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:10px}
.ph h2{margin:0;font-size:14px;letter-spacing:.1em;text-transform:uppercase;color:var(--dim)}
.legend{display:flex;gap:14px;flex-wrap:wrap;font-size:12.5px;color:var(--dim);margin:2px 0 8px}
.legend span{display:inline-flex;align-items:center;gap:6px}
.legend i{width:10px;height:10px;border-radius:3px;display:inline-block}
.chart{position:relative}
.chart canvas{width:100%;height:300px;display:block;touch-action:pan-y}
.chart.sm canvas{height:190px}
.tip{position:absolute;pointer-events:none;background:var(--tipbg);border:1px solid var(--line);border-radius:10px;padding:8px 10px;font-size:12px;min-width:150px;z-index:5;box-shadow:0 10px 30px rgba(0,0,0,.5);display:none}
.tip b{display:block;margin-bottom:5px;color:var(--dim);font-weight:600}
.tip div{display:flex;justify-content:space-between;gap:14px;font-variant-numeric:tabular-nums}
.tip div i{width:8px;height:8px;border-radius:2px;display:inline-block;margin-right:6px}
.sec{position:relative}
.sec>.grip{position:absolute;top:-9px;left:50%;transform:translateX(-50%);z-index:5;width:64px;height:18px;border-radius:9px;background:var(--panel);border:1px solid var(--line);color:var(--mut,#8aa);font-size:11px;line-height:16px;text-align:center;cursor:grab;opacity:0;transition:.15s;user-select:none;letter-spacing:2px}
.sec:hover>.grip{opacity:1;border-color:rgba(34,211,238,.6)}
.grip:active{cursor:grabbing}
.dragging{opacity:.35;transform:scale(.98)}
.card[draggable=true]{cursor:grab}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(400px,1fr));gap:14px}
.card{display:flex;flex-direction:column;position:relative;overflow:hidden;background:var(--panel);border:1px solid var(--line);border-radius:18px;padding:16px;cursor:pointer;transition:.2s;backdrop-filter:blur(10px);-webkit-backdrop-filter:blur(10px)}
.card:hover{transform:translateY(-2px);border-color:rgba(34,211,238,.5);box-shadow:0 14px 40px rgba(0,0,0,.45),0 0 30px rgba(34,211,238,.1)}
.card.offline{opacity:.6;filter:saturate(.4)}
.ch{display:flex;justify-content:space-between;align-items:flex-start;gap:10px}
.name{font-size:18px;font-weight:800;letter-spacing:.01em}
.sub{font-size:12px;color:var(--dim);margin-top:3px;font-family:ui-monospace,SFMono-Regular,Menlo,monospace}
.pill{font-size:11px;font-weight:700;letter-spacing:.06em;padding:4px 10px;border-radius:999px;border:1px solid var(--line);white-space:nowrap}
.pill.ok{color:var(--green);border-color:rgba(52,211,153,.4);background:rgba(52,211,153,.1)}
.pill.warn{color:var(--amber);border-color:rgba(251,191,36,.4);background:rgba(251,191,36,.1)}
.pill.bad{color:var(--red);border-color:rgba(251,113,133,.4);background:rgba(251,113,133,.1)}
.cb{display:flex;gap:16px;align-items:center;margin-top:12px}
.ring{position:relative;width:132px;height:132px;flex:none}
.ring svg{width:100%;height:100%;transform:rotate(-90deg)}
.ring circle{fill:none;stroke-width:9;stroke-linecap:round}
.ring .bg{stroke:var(--ov)}
.ring .fg{stroke:url(#g1);stroke-dasharray:326.7;stroke-dashoffset:326.7;transition:stroke-dashoffset .8s cubic-bezier(.2,.8,.2,1);filter:drop-shadow(0 0 6px rgba(34,211,238,.6))}
.rt{position:absolute;inset:0;display:flex;flex-direction:column;align-items:center;justify-content:center;text-align:center}
.rt b{font-size:28px;font-weight:800;font-variant-numeric:tabular-nums;line-height:1}
.rt span{font-size:11px;color:var(--dim);font-weight:700;letter-spacing:.1em;margin-top:3px}
.rt i{font-style:normal;font-size:11px;color:var(--cyan);margin-top:4px}
.stats{flex:1;display:grid;grid-template-columns:repeat(2,1fr);gap:9px 14px}
.st .k{font-size:10.5px;letter-spacing:.1em;text-transform:uppercase;color:var(--dim);font-weight:700}
.st .x{font-size:16px;font-weight:700;margin-top:1px;font-variant-numeric:tabular-nums}
.spark{width:100%;height:56px;display:block;margin-top:10px}
.foot{margin-top:auto!important;padding-top:8px;display:flex;justify-content:space-between;gap:10px;flex-wrap:wrap;font-size:12px;color:var(--dim);margin-top:8px}
.foot b{color:var(--txt);font-weight:600}
.foot [data-k=pool]{flex:0 0 100%;min-height:15px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.heat{position:absolute;left:0;bottom:0;height:3px;width:0;background:var(--green);transition:width .6s,background .6s;box-shadow:0 0 12px currentColor}
.modal{position:fixed;inset:0;background:var(--scrim);backdrop-filter:blur(6px);-webkit-backdrop-filter:blur(6px);display:none;align-items:flex-start;justify-content:center;overflow:auto;padding:30px 16px;z-index:50}
.modal.on{display:flex}
.sheet{width:100%;max-width:1100px;background:var(--sheet);border:1px solid var(--line);border-radius:20px;padding:20px;box-shadow:0 30px 80px rgba(0,0,0,.6)}
.sh{display:flex;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:14px}
.sh h3{margin:0;font-size:22px}
.mgrid{display:grid;grid-template-columns:repeat(auto-fit,minmax(460px,1fr));gap:12px}
.mc{background:var(--panel2);border:1px solid var(--line);border-radius:14px;padding:12px}
.mc h4{margin:0 0 6px;font-size:12px;letter-spacing:.1em;text-transform:uppercase;color:var(--dim)}
.facts{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:10px;margin-bottom:14px}
.fact{background:var(--panel2);border:1px solid var(--line);border-radius:12px;padding:10px 12px}
.fact .k{font-size:10.5px;letter-spacing:.1em;text-transform:uppercase;color:var(--dim);font-weight:700}
.fact .x{font-size:15px;font-weight:700;margin-top:2px;word-break:break-all}
.empty{display:none;text-align:center;padding:70px 20px}
.empty.on{display:block}
.radar{width:150px;height:150px;border-radius:50%;margin:0 auto 22px;border:1px solid rgba(34,211,238,.35);background:radial-gradient(circle,transparent 30%,rgba(34,211,238,.07) 31%,transparent 32%,transparent 60%,rgba(34,211,238,.07) 61%,transparent 62%);position:relative;overflow:hidden}
.radar::after{content:"";position:absolute;inset:0;background:conic-gradient(from 0deg,rgba(34,211,238,.55),transparent 28%);animation:sweep 2.4s linear infinite}
@keyframes sweep{to{transform:rotate(360deg)}}
.empty h2{margin:0 0 6px}
.empty p{color:var(--dim);margin:0 0 18px}
.pills{display:flex;gap:6px;align-items:center;flex-wrap:wrap;justify-content:flex-end}
.pill.solo{color:#f7931a;border-color:rgba(247,147,26,.5);background:rgba(247,147,26,.13)}
.pill.pooled{color:#60a5fa;border-color:rgba(96,165,250,.5);background:rgba(96,165,250,.13)}
.pill.unk{color:var(--dim)}
.banner{position:relative;overflow:hidden;border-radius:18px;padding:18px 20px;margin-bottom:14px;color:#1b1200;background:linear-gradient(110deg,#ffd76a,#f7931a 45%,#ffd76a);background-size:200% 100%;animation:shine 3s linear infinite;box-shadow:0 0 50px rgba(247,147,26,.5)}
@keyframes shine{to{background-position:-200% 0}}
.banner h2{margin:0 0 6px;font-size:26px;letter-spacing:.08em}
.banner p{margin:4px 0;font-size:14px;font-weight:600}
.banner .brow{display:flex;gap:8px;flex-wrap:wrap;margin-top:10px}
.banner .btn{background:rgba(255,255,255,.6);color:#1b1200;border-color:rgba(0,0,0,.2)}
.banner a{color:#1b1200;font-weight:800}
#confetti{position:fixed;inset:0;pointer-events:none;z-index:60}
.rgrid{display:grid;grid-template-columns:repeat(auto-fit,minmax(310px,1fr));gap:12px}
.rc{position:relative;background:var(--panel2);border:1px solid var(--line);border-radius:14px;padding:14px}
.rc h4{margin:0 0 8px;font-size:12px;letter-spacing:.1em;text-transform:uppercase;color:var(--dim)}
.rc .big{font-size:22px;font-weight:800;line-height:1.25}
.rc .big small{font-size:13px;color:var(--dim);font-weight:600}
.rc .row{display:flex;justify-content:space-between;gap:12px;font-size:13px;padding:5px 0;border-bottom:1px dashed var(--line)}
.rc .row:last-child{border-bottom:0}
.rc .row span:first-child{color:var(--dim);white-space:nowrap}
.rc .row b{font-weight:600;text-align:right;word-break:break-all}
.rc p{margin:8px 0 0;font-size:12.5px;color:var(--dim);line-height:1.45}
.rc a{color:var(--cyan)}
.addr{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px}
.poolbox{padding:10px 0;border-bottom:1px solid var(--line)}
.poolbox:first-of-type{padding-top:0}
.poolbox:last-of-type{border-bottom:0}
.poolbox .t{display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:4px;font-weight:700}

@media(max-width:560px){.kpis{grid-template-columns:repeat(2,1fr)}.kpi .v{font-size:24px}.grid{grid-template-columns:1fr}.mgrid{grid-template-columns:1fr}.cb{flex-direction:column;align-items:stretch}.ring{margin:0 auto}.add input{width:120px}.kpi .v{font-size:26px}}

.tabs{display:flex;gap:6px;margin:6px 0 14px}
.tabs button{appearance:none;border:1px solid var(--line);background:var(--panel);color:var(--dim);padding:9px 20px;border-radius:12px;font-size:14px;font-weight:700;letter-spacing:.04em;cursor:pointer;transition:.18s}
.tabs button:hover{color:var(--txt);border-color:rgba(34,211,238,.5)}
.tabs button.on{color:var(--txt);background:linear-gradient(135deg,rgba(34,211,238,.28),rgba(167,139,250,.26));border-color:rgba(34,211,238,.5)}
#tunerView{display:none}
body.tab-tuner #tunerView{display:block}
body.tab-tuner #main,body.tab-tuner #empty{display:none!important}
body.tab-tuner #resetLayout{display:none}
.tgrid{display:grid;grid-template-columns:minmax(300px,420px) 1fr;gap:18px}
@media(max-width:900px){.tgrid{grid-template-columns:1fr}}
.tf label{display:block;font-size:11px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:var(--dim);margin:14px 0 6px}
.tf label:first-child{margin-top:0}
.tf select,.tf input[type=number]{width:100%;background:var(--panel2);border:1px solid var(--line);color:var(--txt);padding:9px 10px;border-radius:10px;font-size:14px}
.tf .hint{font-size:12.5px;color:var(--dim);margin-top:6px}
.tf .chk{display:flex;gap:8px;align-items:center;font-size:13.5px;color:var(--txt);margin:10px 0 0;text-transform:none;letter-spacing:0;font-weight:500}
.tf details{margin-top:14px;border:1px solid var(--line);border-radius:10px;padding:8px 12px;background:var(--panel2)}
.tf summary{cursor:pointer;font-size:13px;font-weight:600;color:var(--dim)}
.tf .two{display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px}
.tbtns{display:flex;gap:8px;flex-wrap:wrap;margin-top:18px}
.btn.bad{border-color:rgba(251,113,133,.5);color:var(--red)}
.btn[disabled]{opacity:.45;cursor:not-allowed}
.tl .stat{display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:10px}
.tl .act{font-size:17px;font-weight:700;margin:4px 0 8px;line-height:1.35}
.tl .note{font-size:13px;color:var(--dim);line-height:1.5}
.tl .chips{display:flex;gap:8px;flex-wrap:wrap;margin:10px 0}
.tl .chip{background:var(--panel2);border:1px solid var(--line);border-radius:10px;padding:8px 12px;font-size:12.5px;color:var(--dim)}
.tl .chip b{display:block;font-size:18px;color:var(--txt)}
.tchart{height:260px;position:relative;margin-top:8px}
.tchart canvas{width:100%;height:100%;display:block}
.tres{overflow-x:auto}
.tres table{border-collapse:collapse;width:100%;font-size:13px}
.tres th{text-align:right;font-size:11px;letter-spacing:.07em;text-transform:uppercase;color:var(--dim);padding:6px 10px;border-bottom:1px solid var(--line);white-space:nowrap}
.tres td{text-align:right;padding:6px 10px;border-bottom:1px dashed var(--line);white-space:nowrap}
.tres th:first-child,.tres td:first-child{text-align:left}
.tres tr.best td{background:rgba(34,211,238,.1)}
.tres td.ok{color:var(--green)}.tres td.no{color:var(--red)}
#tLog{margin:0;max-height:340px;overflow:auto;background:var(--panel2);border:1px solid var(--line);border-radius:12px;padding:12px;font:12px/1.5 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;color:var(--txt);white-space:pre-wrap;word-break:break-word}

.icon .cmp{display:none}
.icon .exp{display:block}
html.compact .icon .cmp{display:block}
html.compact .icon .exp{display:none}
html.compact .wrap{max-width:none;padding:8px 12px 16px}
html.compact .top{gap:8px;margin-bottom:8px}
html.compact .brand{font-size:13px}
html.compact .brand svg{width:22px;height:22px}
html.compact .brand small{display:none}
html.compact .btn{padding:5px 10px;font-size:12px}
html.compact .seg button{padding:5px 9px;font-size:12px}
html.compact .icon{width:32px;height:30px;padding:0}
html.compact .tabs{margin:2px 0 8px}
html.compact .tabs button{padding:5px 14px;font-size:13px}
html.compact .panel{padding:10px 12px;margin-bottom:8px;border-radius:14px}
html.compact .ph{margin-bottom:6px;gap:8px}
html.compact .ph h2{font-size:12px}
html.compact .kpis{grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:8px;margin-bottom:8px}
html.compact .kpi{padding:8px 10px;border-radius:12px}
html.compact .kpi.sp2{padding-bottom:8px}
html.compact .kpi canvas{display:none}
html.compact .kpi .l{font-size:10px}
html.compact .kpi .v{font-size:20px;margin-top:2px}
html.compact .kpi .v small{font-size:11px}
html.compact .kpi .s{font-size:10.5px;min-height:0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
html.compact .grid{grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:8px;margin-bottom:8px}
html.compact .card{padding:10px 12px;border-radius:14px}
html.compact .name{font-size:15px}
html.compact .cb{margin-top:6px;gap:10px}
html.compact .ring{width:84px;height:84px}
html.compact .rt b{font-size:18px}
html.compact .rt span,html.compact .rt i{font-size:9px;margin-top:1px}
html.compact .stats{gap:4px 10px}
html.compact .st .k{font-size:9.5px}
html.compact .st .x{font-size:13px}
html.compact .spark{display:none}
html.compact .foot{font-size:11px;margin-top:4px}
html.compact .rgrid{grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:8px}
html.compact .rc{padding:8px 10px}
html.compact .rc h4{font-size:11px;margin-bottom:4px}
html.compact .rc .big{font-size:17px}
html.compact .rc .row{font-size:12px;padding:3px 0}
html.compact .rc p{display:none}
html.compact .legend{margin:0 0 4px;font-size:11.5px}
html.compact .chart canvas{height:170px}
html.compact .banner{padding:8px 12px;margin-bottom:8px;border-radius:12px}
html.compact .banner h2{font-size:16px;margin-bottom:2px}
html.compact .banner p{font-size:12.5px;margin:2px 0}
html.compact .banner .brow{margin-top:4px}
html.compact .tgrid{gap:12px}
html.compact #tLog{max-height:200px}
html.compact .tchart{height:200px}

.pgrid{display:grid;grid-template-columns:repeat(auto-fit,minmax(230px,1fr));gap:12px}
.pc{background:var(--panel2);border:1px solid var(--line);border-radius:14px;padding:14px;position:relative}
.pc.act{border-color:rgba(52,211,153,.6);box-shadow:0 0 22px rgba(52,211,153,.12)}
.pc h4{margin:0 0 2px;font-size:15px;letter-spacing:.02em}
.pc .hs{font-size:12px;color:var(--dim);margin-bottom:8px}
.pc .set{font-size:20px;font-weight:800;font-variant-numeric:tabular-nums}
.pc .row{display:flex;justify-content:space-between;font-size:12.5px;padding:3px 0;border-bottom:1px dashed var(--line)}
.pc .row span{color:var(--dim)}
.pc .row:last-of-type{border-bottom:0}
.pc .btn{width:100%;margin-top:10px}
.pc .tag{position:absolute;top:12px;right:12px}
html.compact .pc{padding:8px 10px}
html.compact .pc .set{font-size:16px}

#sections>.panel,#sections>.grid,#sections>.kpis{margin-bottom:20px}
html.compact #sections>.panel,html.compact #sections>.grid,html.compact #sections>.kpis{margin-bottom:10px}
.trashed{display:none!important}
.xbtn{position:absolute;top:7px;right:7px;z-index:6;width:22px;height:22px;border-radius:50%;border:1px solid var(--line);background:var(--panel);color:var(--dim);font-size:13px;line-height:20px;text-align:center;cursor:pointer;opacity:0;transition:.15s;padding:0}
.xbtn:hover{color:var(--red);border-color:rgba(251,113,133,.6)}
.card:hover>.xbtn,.kpi:hover>.xbtn,.rc:hover>.xbtn,.panel:hover>.xbtn{opacity:1}
.card .ch{padding-right:26px}
.kpi .l{padding-right:20px}
.rc h4{padding-right:22px}
.panel>.ph{padding-right:26px}
.trashbtn{position:relative}
.trashbtn .bdg{position:absolute;top:-5px;right:-5px;min-width:17px;height:17px;border-radius:9px;background:var(--orange);color:#1b1200;font-size:10.5px;font-weight:800;line-height:17px;text-align:center;padding:0 4px;display:none}
.trashbtn.has .bdg{display:block}
.trashpop{position:fixed;top:64px;right:14px;z-index:50;width:min(360px,calc(100vw - 28px));max-height:70vh;overflow:auto;background:var(--sheet);border:1px solid var(--line);border-radius:14px;padding:12px;box-shadow:0 20px 60px rgba(0,0,0,.5);display:none}
.trashpop.on{display:block}
.trashpop h3{margin:0 0 8px;font-size:13px;letter-spacing:.1em;text-transform:uppercase;color:var(--dim);display:flex;align-items:center;gap:8px}
.trashpop .it{display:flex;align-items:center;gap:10px;padding:7px 0;border-bottom:1px dashed var(--line);font-size:13.5px}
.trashpop .it span{flex:1}
.trashpop .it small{display:block;color:var(--dim);font-size:11.5px}
.trashpop .empty2{color:var(--dim);font-size:13px;padding:8px 0}

.soundbtn .mut{display:none}
.soundbtn.muted .spk{display:none}
.soundbtn.muted .mut{display:block}
.soundbtn.on,.bell.on{color:var(--green);border-color:rgba(52,211,153,.55);background:rgba(52,211,153,.1)}
.soundbtn.muted,.bell:not(.on){color:var(--orange);border-color:rgba(247,147,26,.5);background:rgba(247,147,26,.08)}
#soundPop{right:14px}

html.compact .ring{width:auto;height:auto;min-width:76px}
html.compact .ring svg{display:none}
html.compact .rt{position:static;align-items:flex-start;text-align:left}
html.compact .rt b{font-size:26px}
html.compact .rt span{margin-top:2px}
html.compact .heat{display:none}
html.compact .card,html.compact .kpi,html.compact .panel,html.compact .rc{backdrop-filter:none;-webkit-backdrop-filter:none;box-shadow:none}
html.compact .card:hover{transform:none;box-shadow:none}
html.compact .stats{grid-template-columns:repeat(4,minmax(0,1fr));gap:4px 12px}
html.compact .grid{grid-template-columns:repeat(auto-fill,minmax(480px,1fr))}
html.compact #trends{display:none}
html.compact .tchart{display:none}
html.compact #sections>.sec>.grip{display:none}
</style>
</head>
<body>
<svg width="0" height="0" style="position:absolute"><defs><linearGradient id="g1" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="#22d3ee"/><stop offset="1" stop-color="#a78bfa"/></linearGradient></defs></svg>
<div class="wrap">
  <div class="top">
    <div class="brand">
      <svg viewBox="0 0 32 32"><path d="M16 2l12 7v14l-12 7L4 23V9z" fill="none" stroke="#f7931a" stroke-width="2"/><path d="M12 11h6.2a2.6 2.6 0 010 5.2H12zm0 5.2h7a2.9 2.9 0 010 5.8h-7zM14.5 9v14M17.5 9v14" fill="none" stroke="#f7931a" stroke-width="1.7" stroke-linecap="round"/></svg>
      <div>BITAXE MONITOR<small>live miner dashboard</small></div>
    </div>
    <div class="sp"></div>
    <div class="live"><span class="dot" id="dot"></span><span id="liveTxt">connecting</span></div>
    <div class="seg" id="unitSeg"><button data-u="F">&deg;F</button><button data-u="C">&deg;C</button></div>
    <button class="btn icon" id="themeBtn" title="Switch between dark and light mode" aria-label="Toggle dark or light mode"><svg class="moon" viewBox="0 0 24 24"><path d="M21 12.8A9 9 0 1111.2 3a7 7 0 009.8 9.8z"/></svg><svg class="sun" viewBox="0 0 24 24"><circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4"/></svg></button>
    <button class="btn icon" id="viewBtn" title="Switch between full and compact view" aria-label="Toggle full or compact view"><svg class="cmp" viewBox="0 0 24 24"><path d="M9 4v5H4M15 4v5h5M9 20v-5H4M15 20v-5h5"/></svg><svg class="exp" viewBox="0 0 24 24"><path d="M4 9V4h5M20 9V4h-5M4 15v5h5M20 15v5h-5"/></svg></button>
    <button class="btn icon trashbtn" id="trashBtn" title="Trash: cards you removed from the dashboard" aria-label="Open trash"><svg viewBox="0 0 24 24"><path d="M4 7h16M10 11v6M14 11v6M6 7l1 13h10l1-13M9 7V4h6v3"/></svg><span class="bdg" id="trashN">0</span></button>
    <button class="btn icon soundbtn" id="soundBtn" title="Block alert sound" aria-label="Block alert sound"><svg class="spk" viewBox="0 0 24 24"><path d="M11 5L6 9H3v6h3l5 4zM15.5 8.5a5 5 0 010 7M18.5 5.5a9 9 0 010 13"/></svg><svg class="mut" viewBox="0 0 24 24"><path d="M11 5L6 9H3v6h3l5 4zM22 9l-6 6M16 9l6 6"/></svg></button>
    <button class="btn icon bell" id="bellBtn" title="Browser alerts when a block is found" aria-label="Toggle block found alerts"><svg viewBox="0 0 24 24"><path d="M6 8a6 6 0 0112 0c0 7 3 9 3 9H3s3-2 3-9"/><path d="M10.3 21a1.94 1.94 0 003.4 0"/></svg></button>
    <form class="add" id="addForm"><input id="addIp" placeholder="add miner IP" autocomplete="off"><button class="btn" type="submit">Add</button></form>
    <button class="btn pri" id="scanBtn" title="Scan the network again for new miners">&#8635; Refresh</button>
    <button class="btn" id="resetLayout" title="Put cards and panels back in the default order">Reset layout</button>
  </div>
  <div class="tabs" id="tabs"><button data-t="monitor" class="on">Monitor</button><button data-t="tuner">Tuner</button></div>
  <div class="trashpop" id="trashPop"></div>
  <div class="trashpop" id="soundPop"></div>
  <div class="scanbar" id="scanbar"><i id="scanfill"></i></div>
  <div class="msg" id="msg"></div>

  <div class="empty" id="empty">
    <div class="radar"></div>
    <h2>No miners yet</h2>
    <p id="emptyTxt">Scanning your network for Bitaxe and AxeOS miners</p>
    <button class="btn pri" id="scanBtn2">&#8635; Refresh</button>
  </div>

  <div id="main" style="display:none">
    <div id="blockBanner"></div>
    <div id="sections">
    <div class="kpis" id="kpis"></div>
    <div class="grid" id="cards"></div>
    <div class="panel" id="rewards"><div class="ph"><h2>Mining mode &amp; rewards</h2></div><div class="rgrid" id="rgrid"></div></div>
    <div class="panel" id="trends">
      <div class="ph">
        <h2>Trends</h2>
        <div class="seg" id="metricSeg"></div>
        <div class="sp"></div>
        <div class="seg" id="rangeSeg"></div>
      </div>
      <div class="legend" id="legend"></div>
      <div class="chart"><canvas id="mainChart"></canvas><div class="tip"></div></div>
    </div>
    </div>
  </div>
  <div id="tunerView">
    <div class="panel"><div class="ph"><h2>Tune a miner</h2></div>
      <div class="tgrid"><div class="tf" id="tForm"></div><div class="tl" id="tLive"></div></div>
    </div>
    <div class="panel" id="tProfPanel"><div class="ph"><h2>Profiles</h2><div class="sp"></div><button class="btn" id="tRebuild" title="Build profiles from this miner's saved tune results">Rebuild from saved results</button></div><div id="tProf"></div></div>
    <div class="panel" id="tResPanel" style="display:none"><div class="ph"><h2>Results</h2></div><div class="tchart"><canvas id="tChart"></canvas></div><div class="tres" id="tRes"></div></div>
    <div class="panel" id="tLogPanel" style="display:none"><div class="ph"><h2>Details</h2><div class="sp"></div><button class="btn" id="tLogBtn">Hide</button></div><pre id="tLog"></pre></div>
  </div>
</div>

<canvas id="confetti"></canvas>
<div class="modal" id="modal"><div class="sheet" id="sheet"></div></div>

<script>
const $=(s,el)=>(el||document).querySelector(s);
const $$=(s,el)=>Array.from((el||document).querySelectorAll(s));
const COLORS=["#22d3ee","#f7931a","#a78bfa","#34d399","#f472b6","#60a5fa","#fbbf24","#fb7185"];
const RANGES=[[300,"5 min"],[900,"15 min"],[3600,"1 hour"],[21600,"6 hours"],[86400,"24 hours"],[604800,"7 days"],[2592000,"30 days"]];
function store(k,v){try{if(v===undefined)return localStorage.getItem(k);localStorage.setItem(k,v)}catch(e){return null}}
const S={drawn:false,notified:{},network:{},pools:[],events:[],miners:[],hist:null,range:Number(store("range"))||3600,metric:store("metric")||"hash",unit:store("unit")||"F",open:null,lastOk:0,scan:{},cards:{}};
function theme(){const cs=getComputedStyle(document.documentElement);const g=k=>cs.getPropertyValue(k).trim();return{dim:g("--dim"),grid:g("--gridc"),cross:g("--cross"),bg:g("--bg")}}
function setTheme(t,save){document.documentElement.setAttribute("data-theme",t);if(save)store("theme",t)}
setTheme(store("theme")==="light"?"light":"dark",false);
document.documentElement.classList.toggle("compact",store("compact")==="1");
const tC=c=>S.unit==="F"?c*9/5+32:c;
const tU=()=>"°"+S.unit;
const METRICS={
hash:{label:"Hashrate",unit:"TH/s",stack:true,zero:true,get:(h,i)=>h.hash[i]==null?null:h.hash[i]/1000,fmt:v=>v.toFixed(2)},
temp:{label:"ASIC temp",unit:()=>tU(),zero:false,span:()=>S.unit==="F"?8:4,get:(h,i)=>h.temp[i]==null?null:tC(h.temp[i]),fmt:v=>v.toFixed(1)},
vr:{label:"VR temp",unit:()=>tU(),zero:false,span:()=>S.unit==="F"?8:4,get:(h,i)=>h.vr[i]==null?null:tC(h.vr[i]),fmt:v=>v.toFixed(1)},
power:{label:"Power",unit:"W",stack:true,zero:true,get:(h,i)=>h.power[i],fmt:v=>v.toFixed(1)},
eff:{label:"Efficiency",unit:"J/TH",zero:false,span:()=>1,get:(h,i)=>(h.hash[i]>0&&h.power[i]!=null)?h.power[i]/(h.hash[i]/1000):null,fmt:v=>v.toFixed(2)},
err:{label:"Error rate",unit:"%",zero:true,span:()=>1,get:(h,i)=>h.err[i],fmt:v=>v.toFixed(2)},
volt:{label:"Input volts",unit:"V",zero:false,span:()=>.3,get:(h,i)=>h.volt[i]==null?null:h.volt[i]/1000,fmt:v=>v.toFixed(2)}
};
const mUnit=m=>typeof m.unit==="function"?m.unit():m.unit;
function fmtN(n,d){if(n==null||isNaN(n))return"-";return Number(n).toLocaleString("en-US",{minimumFractionDigits:d||0,maximumFractionDigits:d||0})}
function fmtDiff(v){if(v==null||v==="")return"-";if(typeof v==="string"&&isNaN(Number(v)))return v;let n=Number(v);const u=["","K","M","G","T","P","E"];let i=0;while(n>=1000&&i<u.length-1){n/=1000;i++}return n.toFixed(n>=100?0:n>=10?1:2)+u[i]}
function esc(t){return String(t==null?"":t).replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]))}
function parseDiff(v){if(v==null||v==="")return null;if(typeof v==="number")return v;const m=String(v).trim().match(/^([\d.,]+)\s*([KMGTPE]?)$/i);if(!m)return null;const n=Number(m[1].replace(/,/g,""));return n*Math.pow(1000,{"":0,K:1,M:2,G:3,T:4,P:5,E:6}[m[2].toUpperCase()])}
function pctStr(p){return(p>=1?p.toFixed(1):p.toPrecision(2))+"%"}
function usdFmt(v){return v>=100?fmtN(v,0):v>=1?v.toFixed(2):v.toFixed(4)}
function satsStr(sats){return sats>=1e7?(sats/1e8).toFixed(4)+" BTC":sats>=10?fmtN(Math.round(sats))+" sats":sats.toFixed(2)+" sats"}
function money(sats){const p=S.network&&S.network.usd;const b=satsStr(sats);return p?b+" ($"+usdFmt(sats/1e8*p)+")":b}
function fmtH(h){const u=["H/s","KH/s","MH/s","GH/s","TH/s","PH/s","EH/s","ZH/s"];let i=0;while(h>=1000&&i<u.length-1){h/=1000;i++}return h.toFixed(h>=100?0:h>=10?1:2)+" "+u[i]}
function bigN(n){return n>=1e9?(n/1e9).toFixed(1)+" billion":n>=1e6?(n/1e6).toFixed(1)+" million":fmtN(Math.round(n))}
function spanStr(days){if(days<1)return plural(Math.max(1,Math.round(days*24)),"hour");if(days<60)return plural(Math.round(days),"day");const y=days/365.25;if(y<2)return plural(Math.round(days/30.4),"month");return fmtN(Math.round(y))+" years"}
function modeLabel(m){return m==="solo"?"Solo mining":m==="pool"?"Pool mining":"Unknown"}
function plural(n,w){return n+" "+w+(n===1?"":"s")}
function dur(s){s=Math.max(0,Math.floor(s||0));const d=Math.floor(s/86400),h=Math.floor(s%86400/3600),m=Math.floor(s%3600/60);if(d)return plural(d,"day")+" "+plural(h,"hour");if(h)return plural(h,"hour")+" "+plural(m,"minute");if(m)return plural(m,"minute");return plural(s,"second")}
function clock(t,sec){return new Date(t*1000).toLocaleTimeString("en-US",sec?{hour:"numeric",minute:"2-digit",second:"2-digit"}:{hour:"numeric",minute:"2-digit"})}
function dayStr(t){return new Date(t*1000).toLocaleDateString("en-US",{month:"short",day:"numeric"})}
function colorOf(ip){const i=S.miners.findIndex(m=>m.ip===ip);return COLORS[(i<0?0:i)%COLORS.length]}
function nameOf(ip){const m=S.miners.find(m=>m.ip===ip);return(m&&m.info&&m.info.hostname)||ip}
function tween(el,val,fmt){const from=el._v==null?val:el._v;el._v=val;if(el._raf)cancelAnimationFrame(el._raf);if(val==null||from===val){el.textContent=val==null?"-":fmt(val);return}const t0=performance.now();const step=now=>{const k=Math.min(1,(now-t0)/450);const e=1-Math.pow(1-k,3);el.textContent=fmt(from+(val-from)*e);if(k<1)el._raf=requestAnimationFrame(step)};el._raf=requestAnimationFrame(step)}

function niceTicks(min,max,n){const span=max-min||1;const raw=span/n;const p=Math.pow(10,Math.floor(Math.log10(raw)));const f=raw/p;const st=(f<1.5?1:f<3?2:f<7?5:10)*p;const out=[];for(let v=Math.ceil(min/st)*st;v<=max+st*1e-6;v+=st)out.push(Math.round(v/st)*st);return out}
const STEPS=[5,10,15,30,60,120,300,600,900,1800,3600,7200,10800,21600,43200,86400,172800,345600];
function drawChart(canvas,cfg,hoverT){
  const dpr=window.devicePixelRatio||1,w=canvas.clientWidth,h=canvas.clientHeight;
  if(!w||!h)return;
  if(canvas.width!==Math.round(w*dpr)||canvas.height!==Math.round(h*dpr)){canvas.width=Math.round(w*dpr);canvas.height=Math.round(h*dpr)}
  const ctx=canvas.getContext("2d");ctx.setTransform(dpr,0,0,dpr,0,0);ctx.clearRect(0,0,w,h);
  canvas._cfg=cfg;
  const L=46,R=12,T=10,B=24,pw=w-L-R,ph=h-T-B;
  const x0=cfg.x0,x1=cfg.x1;
  const X=t=>L+(t-x0)/(x1-x0)*pw;
  let tset=new Set();cfg.series.forEach(s=>s.pts.forEach(p=>tset.add(p[0])));
  const ts=Array.from(tset).sort((a,b)=>a-b);
  const val=new Map();cfg.series.forEach(s=>{const m=new Map();s.pts.forEach(p=>m.set(p[0],p[1]));val.set(s,m)});
  let layers=null,tot=null;
  if(cfg.stack){
    tot=ts.map(()=>0);
    layers=cfg.series.map(s=>{const m=val.get(s);return ts.map((t,i)=>{const lo=tot[i];const v=m.get(t);const up=lo+(v==null?0:v);return[lo,up]})});
    layers.forEach((ly,li)=>ly.forEach((p,i)=>{if(li===cfg.series.length-1)tot[i]=p[1]}));
    const sums=ts.map(()=>0);cfg.series.forEach(s=>{const m=val.get(s);ts.forEach((t,i)=>{const v=m.get(t);sums[i]+=v==null?0:v})});
    tot=sums;
    layers=[];let acc=ts.map(()=>0);
    cfg.series.forEach(s=>{const m=val.get(s);const lo=acc.slice();const up=ts.map((t,i)=>lo[i]+(m.get(t)==null?0:m.get(t)));layers.push(ts.map((t,i)=>[lo[i],up[i]]));acc=up});
  }
  let vmin=Infinity,vmax=-Infinity;
  if(cfg.stack){tot.forEach(v=>{vmax=Math.max(vmax,v);vmin=0})}
  else cfg.series.forEach(s=>s.pts.forEach(p=>{if(p[1]!=null){vmin=Math.min(vmin,p[1]);vmax=Math.max(vmax,p[1])}}));
  if(!isFinite(vmax)){vmin=0;vmax=1}
  if(cfg.zero)vmin=0;
  if(vmax===vmin){vmax=vmin+1}
  if(cfg.span&&vmax-vmin<cfg.span){const mid=(vmax+vmin)/2;vmin=mid-cfg.span/2;vmax=mid+cfg.span/2}
  const padv=(vmax-vmin)*.1;
  let lo=cfg.zero?0:Math.max(0,vmin-padv),hi=vmax+padv;
  if(cfg.zero&&vmax<=0)hi=1;
  const Y=v=>T+ph-(v-lo)/(hi-lo)*ph;
  const th=theme();
  ctx.font="11px -apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif";ctx.textBaseline="middle";
  const yt=niceTicks(lo,hi,4);
  ctx.strokeStyle=th.grid;ctx.lineWidth=1;ctx.fillStyle=th.dim;ctx.textAlign="right";
  yt.forEach(v=>{const y=Math.round(Y(v))+.5;ctx.beginPath();ctx.moveTo(L,y);ctx.lineTo(w-R,y);ctx.stroke();ctx.fillText(cfg.axis?cfg.axis(v):String(+v.toFixed(2)),L-7,y)});
  const span=x1-x0;let step=STEPS.find(s=>span/s<=Math.max(3,pw/95))||STEPS[STEPS.length-1];
  const off=-new Date().getTimezoneOffset()*60;ctx.textAlign="center";ctx.textBaseline="top";
  let lastDay="";
  for(let t=Math.ceil((x0+off)/step)*step-off;t<=x1;t+=step){const x=X(t);if(x<L+14||x>w-R-14)continue;ctx.globalAlpha=.6;ctx.strokeStyle=th.grid;ctx.beginPath();ctx.moveTo(Math.round(x)+.5,T);ctx.lineTo(Math.round(x)+.5,T+ph);ctx.stroke();ctx.globalAlpha=1;
    let lab=span>129600?dayStr(t)+" "+new Date(t*1000).toLocaleTimeString("en-US",{hour:"numeric"}):clock(t,span<=900);
    if(span>129600&&step>=86400)lab=dayStr(t);
    ctx.fillStyle=th.dim;ctx.fillText(lab,x,T+ph+7)}
  const gap=cfg.bucket*3.5;
  function trace(pts,fn){let open=false,prev=null;ctx.beginPath();pts.forEach(p=>{const v=fn(p);if(v==null){open=false;prev=null;return}const x=X(p[0]),y=Y(v);if(!open||(prev!=null&&p[0]-prev>gap)){ctx.moveTo(x,y);open=true}else ctx.lineTo(x,y);prev=p[0]})}
  if(cfg.stack){
    for(let li=cfg.series.length-1;li>=0;li--){
      const s=cfg.series[li],ly=layers[li];
      ctx.beginPath();ly.forEach((p,i)=>{const x=X(ts[i]),y=Y(p[1]);i?ctx.lineTo(x,y):ctx.moveTo(x,y)});
      for(let i=ly.length-1;i>=0;i--)ctx.lineTo(X(ts[i]),Y(ly[i][0]));
      ctx.closePath();const g=ctx.createLinearGradient(0,T,0,T+ph);g.addColorStop(0,s.color+"88");g.addColorStop(1,s.color+"10");ctx.fillStyle=g;ctx.fill();
      ctx.beginPath();ly.forEach((p,i)=>{const x=X(ts[i]),y=Y(p[1]);i?ctx.lineTo(x,y):ctx.moveTo(x,y)});
      ctx.strokeStyle=s.color;ctx.lineWidth=2;ctx.shadowColor=s.color;ctx.shadowBlur=9;ctx.stroke();ctx.shadowBlur=0}
  }else{
    cfg.series.forEach(s=>{
      if(cfg.series.length<=3){let first=null,lastp=null;const pts=s.pts.filter(p=>p[1]!=null);
        if(pts.length>1){ctx.beginPath();pts.forEach((p,i)=>{const x=X(p[0]),y=Y(p[1]);if(i===0||p[0]-pts[i-1][0]>gap){if(i>0){ctx.lineTo(X(pts[i-1][0]),T+ph);ctx.lineTo(first,T+ph);ctx.closePath()}ctx.moveTo(x,y);first=x}else ctx.lineTo(x,y);lastp=p});ctx.lineTo(X(lastp[0]),T+ph);ctx.lineTo(first,T+ph);ctx.closePath();const g=ctx.createLinearGradient(0,T,0,T+ph);g.addColorStop(0,s.color+"44");g.addColorStop(1,s.color+"00");ctx.fillStyle=g;ctx.fill()}}
      trace(s.pts,p=>p[1]);ctx.strokeStyle=s.color;ctx.lineWidth=2;ctx.lineJoin="round";ctx.shadowColor=s.color;ctx.shadowBlur=9;ctx.stroke();ctx.shadowBlur=0});
  }
  const tipEl=canvas.parentNode.querySelector(".tip");
  if(hoverT!=null&&ts.length){
    let bi=0,bd=Infinity;ts.forEach((t,i)=>{const d=Math.abs(t-hoverT);if(d<bd){bd=d;bi=i}});
    const t=ts[bi],x=X(t);
    ctx.strokeStyle=th.cross;ctx.lineWidth=1;ctx.beginPath();ctx.moveTo(Math.round(x)+.5,T);ctx.lineTo(Math.round(x)+.5,T+ph);ctx.stroke();
    let html="<b>"+(span>86400?dayStr(t)+", ":"")+clock(t,true)+"</b>";
    cfg.series.forEach((s,li)=>{const v=val.get(s).get(t);if(v==null)return;const yy=cfg.stack?Y(layers[li][bi][1]):Y(v);ctx.fillStyle=s.color;ctx.beginPath();ctx.arc(x,yy,4,0,7);ctx.fill();ctx.strokeStyle=th.bg;ctx.lineWidth=2;ctx.stroke();html+='<div><span><i style="background:'+s.color+'"></i>'+s.name+"</span><span>"+cfg.fmt(v)+" "+cfg.unit+"</span></div>"});
    if(cfg.stack&&cfg.series.length>1)html+='<div style="margin-top:4px;border-top:1px solid var(--line);padding-top:4px"><span>Total</span><span>'+cfg.fmt(tot[bi])+" "+cfg.unit+"</span></div>";
    tipEl.innerHTML=html;tipEl.style.display="block";
    const tw=tipEl.offsetWidth;let left=x+14;if(left+tw>w)left=x-tw-14;tipEl.style.left=Math.max(0,left)+"px";tipEl.style.top="12px";
  }else if(tipEl)tipEl.style.display="none";
}
function bindHover(canvas){
  if(canvas._bound)return;canvas._bound=true;
  const mv=e=>{const c=canvas._cfg;if(!c)return;const r=canvas.getBoundingClientRect();const px=(e.clientX-r.left);const t=c.x0+(px-46)/(r.width-58)*(c.x1-c.x0);drawChart(canvas,c,t)};
  canvas.addEventListener("pointermove",mv);canvas.addEventListener("pointerdown",mv);
  canvas.addEventListener("pointerleave",()=>{if(canvas._cfg)drawChart(canvas,canvas._cfg,null)});
}
function spark(canvas,pts,color){
  const dpr=window.devicePixelRatio||1,w=canvas.clientWidth,h=canvas.clientHeight;if(!w||!h)return;
  canvas.width=Math.round(w*dpr);canvas.height=Math.round(h*dpr);
  const ctx=canvas.getContext("2d");ctx.setTransform(dpr,0,0,dpr,0,0);ctx.clearRect(0,0,w,h);
  const v=pts.filter(p=>p[1]!=null);if(v.length<2)return;
  const t0=v[0][0],t1=v[v.length-1][0];let mn=Infinity,mx=-Infinity;v.forEach(p=>{mn=Math.min(mn,p[1]);mx=Math.max(mx,p[1])});if(mx===mn){mx+=1;mn-=1}
  const pad=(mx-mn)*.15;mn-=pad;mx+=pad;
  const X=t=>(t-t0)/((t1-t0)||1)*w,Y=y=>h-3-(y-mn)/(mx-mn)*(h-8);
  ctx.beginPath();v.forEach((p,i)=>i?ctx.lineTo(X(p[0]),Y(p[1])):ctx.moveTo(X(p[0]),Y(p[1])));
  const line=new Path2D();
  ctx.strokeStyle=color;ctx.lineWidth=1.8;ctx.lineJoin="round";ctx.shadowColor=color;ctx.shadowBlur=7;ctx.stroke();ctx.shadowBlur=0;
  ctx.lineTo(w,h);ctx.lineTo(0,h);ctx.closePath();const g=ctx.createLinearGradient(0,0,0,h);g.addColorStop(0,color+"3a");g.addColorStop(1,color+"00");ctx.fillStyle=g;ctx.fill();
}
function histSeries(ip,key){const h=S.hist&&S.hist.miners[ip];if(!h)return[];const m=METRICS[key];return h.t.map((t,i)=>[t,m.get(h,i)])}
function totalPts(key){
  if(!S.hist)return[];const m=new Map();
  Object.keys(S.hist.miners).forEach(ip=>{const h=S.hist.miners[ip];h.t.forEach((t,i)=>{const v=METRICS[key].get(h,i);if(v!=null)m.set(t,(m.get(t)||0)+v)})});
  return Array.from(m.entries()).sort((a,b)=>a[0]-b[0]);
}
function cfgFor(key,ips,single){
  const m=METRICS[key];
  return{x0:S.hist.start,x1:S.hist.end,bucket:S.hist.bucket,stack:!!m.stack&&!single,zero:m.zero,span:m.span?m.span():0,fmt:m.fmt,axis:v=>m.fmt(v),unit:mUnit(m),series:ips.map(ip=>({name:nameOf(ip),color:colorOf(ip),pts:histSeries(ip,key)}))};
}

function buildKpis(){
  const defs=[["hash","Total hashrate","hero"],["power","Power draw",""],["eff","Efficiency",""],["online","Miners online",""],["shares","Shares accepted",""],["best","Best difficulty",""],["blocks","Blocks found",""],["hot","Hottest ASIC",""]];
  const kp=$("#kpis");
  kp.innerHTML=defs.map(d=>'<div class="kpi '+d[2]+(["hash","power","eff"].includes(d[0])?" sp2":"")+'" data-k="'+d[0]+'"><div class="l">'+d[1]+'</div><div class="v"><span class="n">-</span><small></small></div><div class="s"></div>'+(["hash","power","eff"].includes(d[0])?"<canvas></canvas>":"")+"</div>").join("");
  $$(".kpi",kp).forEach(e=>{e.draggable=true;e.style.cursor="grab"});
  applyOrder(kp,$$(".kpi",kp),"layout:kpis",e=>e.dataset.k);
  makeSortable(kp,".kpi","layout:kpis",e=>e.dataset.k,null);
}
function kset(k,num,fmt,unit,sub){const el=$('.kpi[data-k="'+k+'"]');if(!el)return;const n=$(".n",el);if(typeof num==="number")tween(n,num,fmt);else n.textContent=num;$("small",el).textContent=unit||"";$(".s",el).textContent=sub||""}
function updateKpis(){
  const on=S.miners.filter(m=>m.online&&m.info);
  const hash=on.reduce((a,m)=>a+(Number(m.info.hashRate)||0),0)/1000;
  const pw=on.reduce((a,m)=>a+(Number(m.info.power)||0),0);
  const acc=S.miners.reduce((a,m)=>a+(m.info?Number(m.info.sharesAccepted)||0:0),0);
  const rej=S.miners.reduce((a,m)=>a+(m.info?Number(m.info.sharesRejected)||0:0),0);
  let best=-1,bestRaw=null;S.miners.forEach(m=>{if(m.info&&m.info.bestDiff!=null){const v=parseDiff(m.info.bestDiff);if(v!=null&&v>best){best=v;bestRaw=m.info.bestDiff}}});
  const nd=S.network&&S.network.difficulty;
  const blocks=S.miners.reduce((a,m)=>a+(m.block?m.block.count:0),0);const reporting=S.miners.some(m=>m.block&&m.block.reports);
  const hot=on.reduce((a,m)=>Math.max(a,Number(m.info.temp)||0),0);
  kset("hash",hash,v=>v.toFixed(2),"TH/s",on.length?"expected "+(on.reduce((a,m)=>a+(Number(m.info.expectedHashrate)||0),0)/1000).toFixed(2)+" TH/s":"");
  kset("power",pw,v=>v.toFixed(1),"W",fmtN(pw*730/1000,1)+" kWh per month");
  kset("eff",hash>0?pw/hash:null,v=>v.toFixed(2),"J/TH","lower is better");
  kset("online",on.length+" / "+S.miners.length,0,"",S.miners.length-on.length?plural(S.miners.length-on.length,"miner")+" offline":"all responding");
  kset("shares",acc,v=>fmtN(v),"",rej?fmtN(rej)+" rejected ("+(rej/(acc+rej)*100).toFixed(2)+"%)":"none rejected");
  kset("best",bestRaw==null?"-":fmtDiff(bestRaw),0,"",nd&&best>0?pctStr(best/nd*100)+" of a block":"across all miners");
  kset("blocks",blocks,v=>fmtN(v),"",reporting?(blocks?"check your pool page":"none yet"):"needs AxeOS 2.11 or newer");
  kset("hot",hot?tC(hot):"-",v=>v.toFixed(1),hot?tU():"",hot?"limit is about "+tC(70).toFixed(0)+tU():"");
}
function updateKpiSparks(){
  [["hash","#22d3ee"],["power","#f7931a"],["eff","#a78bfa"]].forEach(([k,c])=>{
    const cv=$('.kpi[data-k="'+k+'"] canvas');if(!cv)return;
    let pts;if(k==="eff"){const hh=new Map(totalPts("hash")),pp=new Map(totalPts("power"));pts=Array.from(hh.keys()).sort((a,b)=>a-b).map(t=>[t,hh.get(t)>0&&pp.has(t)?pp.get(t)/hh.get(t):null])}else pts=totalPts(k);
    spark(cv,pts,c)});
}
const CARD=`<div class="ch"><div><div class="name" data-k="name"></div><div class="sub"><span data-k="ip"></span> &middot; <span data-k="model"></span></div></div><div class="pills"><span class="pill" data-k="mode"></span><span class="pill" data-k="pill"></span></div></div>
<div class="cb"><div class="ring"><svg viewBox="0 0 120 120"><circle class="bg" cx="60" cy="60" r="52"/><circle class="fg" cx="60" cy="60" r="52" data-k="arc"/></svg><div class="rt"><b data-k="hash">0.00</b><span>TH/S</span><i data-k="pct"></i></div></div>
<div class="stats">
<div class="st"><div class="k">ASIC temp</div><div class="x" data-k="temp"></div></div>
<div class="st"><div class="k">VR temp</div><div class="x" data-k="vr"></div></div>
<div class="st"><div class="k">Power</div><div class="x" data-k="power"></div></div>
<div class="st"><div class="k">Efficiency</div><div class="x" data-k="eff"></div></div>
<div class="st"><div class="k">Clock</div><div class="x" data-k="freq"></div></div>
<div class="st"><div class="k">Core volts</div><div class="x" data-k="core"></div></div>
<div class="st"><div class="k">Error rate</div><div class="x" data-k="err"></div></div>
<div class="st"><div class="k">Input volts</div><div class="x" data-k="volt"></div></div>
</div></div>
<canvas class="spark"></canvas>
<div class="foot"><span>Shares <b data-k="shares"></b></span><span>Up <b data-k="up"></b></span><span data-k="pool"></span></div>
<div class="heat" data-k="heat"></div>`;

const LAYOUT={
  get(k){try{return JSON.parse(store(k)||"[]")}catch(e){return[]}},
  set(k,v){store(k,JSON.stringify(v))}
};
function applyOrder(container,items,key,idOf){
  const saved=LAYOUT.get(key);
  const rank=el=>{const i=saved.indexOf(idOf(el));return i<0?1e6:i};
  items.slice().sort((a,b)=>rank(a)-rank(b)).forEach(el=>container.appendChild(el));
}
function makeSortable(container,itemSel,key,idOf,handleSel){
  if(container._sortable)return;container._sortable=true;
  let drag=null;
  const items=()=>Array.from(container.children).filter(e=>e.matches(itemSel));
  container.addEventListener("dragstart",e=>{
    let el=e.target;
    while(el&&el.parentNode!==container)el=el.parentNode;
    if(!el||!el.matches(itemSel))return;
    drag=el;el.classList.add("dragging");
    try{e.dataTransfer.effectAllowed="move";e.dataTransfer.setData("text/plain",idOf(el))}catch(x){}
  });
  container.addEventListener("dragover",e=>{
    if(!drag)return;e.preventDefault();
    const others=items().filter(x=>x!==drag&&x.offsetParent!==null);
    let best=null,bd=1e12;
    others.forEach(o=>{const r=o.getBoundingClientRect();const cx=r.left+r.width/2,cy=r.top+r.height/2;const d=Math.hypot(e.clientX-cx,(e.clientY-cy)*1.5);if(d<bd){bd=d;best=o}});
    if(!best)return;
    const r=best.getBoundingClientRect();
    const after=handleSel?(e.clientY>r.top+r.height/2):(e.clientY>r.bottom||(e.clientY>r.top&&e.clientX>r.left+r.width/2));
    const ref=after?best.nextSibling:best;
    if(ref!==drag&&drag.nextSibling!==ref)container.insertBefore(drag,ref);
  });
  container.addEventListener("drop",e=>{if(drag)e.preventDefault()});
  container.addEventListener("dragend",()=>{
    if(!drag)return;
    drag.classList.remove("dragging");
    LAYOUT.set(key,items().map(idOf));
    drag=null;
  });
  if(handleSel){
    items().forEach(el=>{
      if(el.querySelector(":scope>.grip"))return;
      el.classList.add("sec");
      const g=document.createElement("div");g.className="grip";g.textContent="\u2807\u2807\u2807";g.title="Drag to move";g.draggable=true;
      g.addEventListener("dragstart",ev=>{try{ev.dataTransfer.setDragImage(el,20,20)}catch(x){}});
      el.prepend(g);
    });
  }
}
function initSections(){
  const sec=$("#sections");if(!sec||sec._ready)return;sec._ready=true;
  applyOrder(sec,Array.from(sec.children),"layout:sections",e=>e.id);
  makeSortable(sec,"div","layout:sections",e=>e.id,true);
}
function resetLayout(){
  try{localStorage.removeItem("layout:sections");localStorage.removeItem("layout:cards");localStorage.removeItem("layout:kpis");localStorage.removeItem("layout:rewards")}catch(e){}
  const kp=$("#kpis");["hash","power","eff","online","shares","best","blocks","hot"].forEach(k=>{const e=$('.kpi[data-k="'+k+'"]',kp);if(e)kp.appendChild(e)});
  const rg=$("#rgrid");["r1","r2","r3"].forEach(id=>{const e=$("#"+id,rg);if(e)rg.appendChild(e)});
  const sec=$("#sections");["kpis","cards","rewards","trends"].forEach(id=>sec.appendChild($("#"+id)));
  S.miners.forEach(m=>$("#cards").appendChild(S.cards[m.ip]));
}
document.addEventListener("click",e=>{if(e.target.closest("#resetLayout"))resetLayout()});

const KPI_NAMES={hash:"Total hashrate",power:"Power draw",eff:"Efficiency",online:"Miners online",shares:"Shares accepted",best:"Best difficulty",blocks:"Blocks found",hot:"Hottest ASIC"};
const BOX_NAMES={r1:"How you are mining",r2:"Reward per block",r3:"Your odds and average earnings"};
const PANEL_NAMES={kpis:"Stat tiles",rewards:"Mining mode and rewards",trends:"Trends"};
function trashList(){try{return JSON.parse(store("trash")||"[]")}catch(e){return[]}}
function trashSave(l){store("trash",JSON.stringify(l))}
function trashLabel(id){
  const ci=id.indexOf(":");const t=id.slice(0,ci),k=id.slice(ci+1);
  if(t==="card")return[nameOf(k),"Miner card"];
  if(t==="kpi")return[KPI_NAMES[k]||k,"Stat tile"];
  if(t==="box")return[BOX_NAMES[k]||k,"Rewards box"];
  return[PANEL_NAMES[k]||k,"Panel"];
}
function markTrash(){
  const tag=(el,id)=>{
    el.dataset.tid=id;
    if(!el.querySelector(":scope>.xbtn")){
      const x=document.createElement("button");x.className="xbtn";x.type="button";x.textContent="×";x.title="Move to trash";x.setAttribute("aria-label","Move to trash");
      x.addEventListener("click",e=>{e.stopPropagation();e.preventDefault();trashAdd(el.dataset.tid)});
      x.addEventListener("mousedown",e=>e.stopPropagation());
      el.appendChild(x);
    }
  };
  $$("#cards .card").forEach(e=>tag(e,"card:"+e.dataset.ip));
  $$("#kpis .kpi").forEach(e=>tag(e,"kpi:"+e.dataset.k));
  $$("#rgrid .rc").forEach(e=>tag(e,"box:"+e.id));
  ["rewards","trends"].forEach(id=>{const e=$("#"+id);if(e)tag(e,"panel:"+id)});
  const tl=trashList();
  $$("[data-tid]").forEach(e=>e.classList.toggle("trashed",tl.includes(e.dataset.tid)));
  const n=tl.length;$("#trashN").textContent=n;$("#trashBtn").classList.toggle("has",n>0);
  if($("#trashPop").classList.contains("on"))renderTrash();
}
function trashAdd(id){const l=trashList();if(!l.includes(id)){l.push(id);trashSave(l)}markTrash();if(S.hist&&S.miners.length)renderAll()}
function trashRestore(id){trashSave(trashList().filter(x=>x!==id));markTrash();setTimeout(()=>{if(S.hist&&S.miners.length)renderAll()},60)}
function renderTrash(){
  const l=trashList();const pop=$("#trashPop");
  pop.innerHTML='<h3>Trash<span style="flex:1"></span>'+(l.length?'<button class="btn" id="trashAll">Restore all</button>':"")+"</h3>"+(l.length?l.map(id=>{const t=trashLabel(id);return '<div class="it"><span>'+esc(t[0])+"<small>"+esc(t[1])+'</small></span><button class="btn" data-restore="'+esc(id)+'">Restore</button></div>'}).join(""):'<div class="empty2">Nothing in the trash. Hover over a card and click the x to remove it from the dashboard.</div>');
}
$("#trashBtn").onclick=e=>{e.stopPropagation();const pop=$("#trashPop");$("#soundPop").classList.remove("on");pop.classList.toggle("on");if(pop.classList.contains("on"))renderTrash()};
$("#trashPop").addEventListener("click",e=>{
  e.stopPropagation();
  const r=e.target.closest("[data-restore]");if(r){trashRestore(r.dataset.restore);return}
  if(e.target.closest("#trashAll")){trashSave([]);markTrash();setTimeout(()=>{if(S.hist&&S.miners.length)renderAll()},60)}
});
document.addEventListener("click",()=>$("#trashPop").classList.remove("on"));

const SND={ctx:null,repeat:null,count:0};
function soundOn(){return store("sound")!=="0"}
function sndCtx(){
  try{
    if(!SND.ctx){const A=window.AudioContext||window.webkitAudioContext;if(!A)return null;SND.ctx=new A()}
    if(SND.ctx.state==="suspended")SND.ctx.resume();
    return SND.ctx;
  }catch(e){return null}
}
function sndNote(c,t,f,d,v,type){
  const o=c.createOscillator(),g=c.createGain();
  o.type=type||"triangle";o.frequency.value=f;
  g.gain.setValueAtTime(0.0001,t);g.gain.exponentialRampToValueAtTime(v,t+0.015);g.gain.exponentialRampToValueAtTime(0.0001,t+d);
  o.connect(g);g.connect(c.destination);o.start(t);o.stop(t+d+0.05);
}
function playFanfare(){
  const c=sndCtx();if(!c)return false;
  const t=c.currentTime+0.05;
  const run=[523.25,659.25,783.99,1046.5,783.99,1046.5,1318.5];
  run.forEach((f,i)=>{const at=t+i*0.13;sndNote(c,at,f,0.45,0.22,"triangle");sndNote(c,at,f*2,0.3,0.06,"sine")});
  const end=t+run.length*0.13+0.05;
  [523.25,659.25,783.99,1046.5].forEach(f=>sndNote(c,end,f,1.3,0.12,"triangle"));
  for(let k=0;k<3;k++){const at=end+0.5+k*0.28;sndNote(c,at,2093,0.5,0.07,"sine")}
  return true;
}
function alertSound(){if(soundOn())playFanfare()}
function alertRepeat(on){
  if(on&&soundOn()){
    if(!SND.repeat){SND.count=0;SND.repeat=setInterval(()=>{if(!soundOn()){alertRepeat(false);return}playFanfare();confetti()},60000)}
  }else if(SND.repeat){clearInterval(SND.repeat);SND.repeat=null}
}
function soundSync(){
  const on=soundOn();const b=$("#soundBtn");
  b.classList.toggle("on",on);b.classList.toggle("muted",!on);
  const pop=$("#soundPop");
  pop.innerHTML='<h3>Block alert sound</h3><div class="it"><span>Sound<small>'+(on?"Plays when a block is found and again every minute until you dismiss the banner":"Muted")+'</small></span><button class="btn '+(on?"pri":"")+'" id="sndToggle">'+(on?"On":"Off")+'</button></div><div class="it"><span>Hear it first<small>Plays the alert once so you know what to expect</small></span><button class="btn" id="sndTest">Play test</button></div><div class="empty2">Your browser only allows sound after you have clicked on the page once, so keep this tab open and click anywhere after loading it.</div>';
}
$("#soundBtn").onclick=e=>{e.stopPropagation();const pop=$("#soundPop");$("#trashPop").classList.remove("on");pop.classList.toggle("on");soundSync()};
$("#soundPop").addEventListener("click",e=>{
  e.stopPropagation();
  if(e.target.closest("#sndToggle")){store("sound",soundOn()?"0":"1");if(!soundOn())alertRepeat(false);soundSync();if(soundOn())playFanfare()}
  if(e.target.closest("#sndTest")){if(!playFanfare())say("This browser could not play sound");confetti()}
});
document.addEventListener("click",()=>{$("#soundPop").classList.remove("on");sndCtx()});
document.addEventListener("keydown",()=>{sndCtx()},{once:true});
soundSync();
function heatColor(c){return c>=68?"#fb7185":c>=64?"#fbbf24":c>=40?"#34d399":"#22d3ee"}
function updateCard(m){
  let el=S.cards[m.ip];
  if(!el){el=document.createElement("article");el.className="card";el.draggable=true;el.dataset.ip=m.ip;el.innerHTML=CARD;el.addEventListener("click",()=>openModal(m.ip));$("#cards").appendChild(el);S.cards[m.ip]=el}
  const q=k=>$('[data-k="'+k+'"]',el);const i=m.info||{};
  el.classList.toggle("offline",!m.online);
  q("name").textContent=i.hostname||m.ip;q("ip").textContent=m.ip;q("model").textContent=[i.ASICModel,i.boardVersion?"board "+i.boardVersion:""].filter(Boolean).join(" ");
  const pill=q("pill");
  if(!m.online){pill.textContent=m.info?"OFFLINE":"NO ANSWER";pill.className="pill bad"}
  else if(m.block&&m.block.count>0){pill.textContent="BLOCK FOUND";pill.className="pill solo"}
  else if(i.isUsingFallbackStratum){pill.textContent="FALLBACK POOL";pill.className="pill warn"}
  else if(Number(i.errorPercentage)>1.5||Number(i.temp)>=68){pill.textContent="ATTENTION";pill.className="pill warn"}
  else{pill.textContent="MINING";pill.className="pill ok"}
  if((S.tuning||[]).includes(m.ip)){pill.textContent="TUNING";pill.className="pill warn"}
  const mp=q("mode"),pm=m.pool?m.pool.mode:"unknown";
  mp.textContent=pm==="solo"?"SOLO":pm==="pool"?"POOL":"MODE UNKNOWN";mp.className="pill "+(pm==="solo"?"solo":pm==="pool"?"pooled":"unk");
  const hr=(Number(i.hashRate)||0)/1000,ex=(Number(i.expectedHashrate)||0)/1000;
  tween(q("hash"),m.online?hr:0,v=>v.toFixed(2));
  const frac=ex>0?Math.min(1.1,hr/ex)/1.1:Math.min(1,hr/2.5);
  q("arc").style.strokeDashoffset=String(326.7*(1-(m.online?frac:0)));
  q("pct").textContent=ex>0&&m.online?Math.round(hr/ex*100)+"%":"";
  q("pct").title="of expected hashrate";
  const temp=Number(i.temp)||0;
  q("temp").textContent=i.temp==null?"-":tC(temp).toFixed(1)+tU();q("temp").style.color=heatColor(temp);
  q("vr").textContent=i.vrTemp==null?"-":tC(Number(i.vrTemp)).toFixed(1)+tU();
  q("power").textContent=i.power==null?"-":Number(i.power).toFixed(1)+" W";
  q("eff").textContent=hr>0&&i.power!=null?(Number(i.power)/hr).toFixed(2)+" J/TH":"-";
  q("freq").textContent=i.frequency==null?"-":fmtN(i.frequency)+" MHz";
  q("core").textContent=i.coreVoltage==null?"-":fmtN(i.coreVoltage)+" mV";
  const er=Number(i.errorPercentage);q("err").textContent=i.errorPercentage==null?"-":er.toFixed(2)+"%";q("err").style.color=er>1.5?"#fb7185":er>0.5?"#fbbf24":"";
  q("volt").textContent=i.voltage==null?"-":(Number(i.voltage)/1000).toFixed(2)+" V";
  q("shares").textContent=i.sharesAccepted==null?"-":fmtN(i.sharesAccepted)+(Number(i.sharesRejected)?" ("+fmtN(i.sharesRejected)+" rejected)":"");
  q("up").textContent=i.uptimeSeconds==null?"-":dur(i.uptimeSeconds);
  q("pool").textContent=i.stratumURL?String(i.stratumURL).replace(/^stratum\+tcp:\/\//,"")+(i.stratumPort?":"+i.stratumPort:""):"";
  const heat=q("heat");heat.style.width=Math.max(0,Math.min(100,(temp-30)/45*100))+"%";heat.style.background=heatColor(temp);heat.style.color=heatColor(temp);
  const dots=Number(m.last_seen);if(!m.online&&dots)q("up").textContent="last seen "+clock(dots);
}
function updateSparks(){S.miners.forEach(m=>{const el=S.cards[m.ip];if(el)spark($(".spark",el),histSeries(m.ip,"hash"),colorOf(m.ip))})}
function renderLegend(){
  $("#legend").innerHTML=S.miners.map(m=>'<span><i style="background:'+colorOf(m.ip)+'"></i>'+nameOf(m.ip)+"</span>").join("");
}
function renderMain(){
  if(!S.hist)return;
  const ips=S.miners.map(m=>m.ip).filter(ip=>S.hist.miners[ip]);
  const cv=$("#mainChart");bindHover(cv);drawChart(cv,cfgFor(S.metric,ips,false),null);
}
function renderAll(){renderMain();updateSparks();updateKpiSparks();if(S.open)renderModalCharts()}
function buildSegs(){
  $("#metricSeg").innerHTML=Object.keys(METRICS).map(k=>'<button data-m="'+k+'">'+METRICS[k].label+"</button>").join("");
  $("#rangeSeg").innerHTML=RANGES.map(r=>'<button data-r="'+r[0]+'">'+r[1]+"</button>").join("");
  const sync=()=>{$$("#metricSeg button").forEach(b=>b.classList.toggle("on",b.dataset.m===S.metric));$$("#rangeSeg button").forEach(b=>b.classList.toggle("on",Number(b.dataset.r)===S.range));$$("#unitSeg button").forEach(b=>b.classList.toggle("on",b.dataset.u===S.unit))};
  $("#metricSeg").onclick=e=>{const b=e.target.closest("button");if(!b)return;S.metric=b.dataset.m;store("metric",S.metric);sync();renderMain()};
  $("#rangeSeg").onclick=e=>{const b=e.target.closest("button");if(!b)return;S.range=Number(b.dataset.r);store("range",String(S.range));sync();loadHist()};
  $("#unitSeg").onclick=e=>{const b=e.target.closest("button");if(!b)return;S.unit=b.dataset.u;store("unit",S.unit);sync();apply(null);renderAll()};
  sync();
}
function poolFacts(m){
  const pl=m.pool||{},i=m.info||{};const f=[["Mining mode",modeLabel(pl.mode)],["Payout address",pl.address||pl.account||"-"]];
  if(pl.mode==="solo"&&pl.address&&i.coinbaseOutputs!=null){const hit=JSON.stringify(i.coinbaseOutputs).includes(pl.address);f.push(["Address in block template",hit?"yes":"not found"])}
  f.push(["Pool page",pl.page?'<a href="'+esc(pl.page)+'" target="_blank" rel="noopener" style="color:var(--cyan)">Open pool page</a>':"-",true]);
  return f;
}
function modalFacts(m){
  const i=m.info||{};
  const f=poolFacts(m).concat([["Address",m.ip],["Firmware",i.version||"-"],["Pool",i.stratumURL?i.stratumURL+(i.stratumPort?":"+i.stratumPort:""):"-"],["Worker",i.stratumUser||"-"],["Pool difficulty",fmtDiff(i.poolDifficulty)],["Best difficulty",fmtDiff(i.bestDiff)],["Best this session",fmtDiff(i.bestSessionDiff)],["Hashrate 1 min",i.hashRate_1m==null?"-":(i.hashRate_1m/1000).toFixed(2)+" TH/s"],["Hashrate 10 min",i.hashRate_10m==null?"-":(i.hashRate_10m/1000).toFixed(2)+" TH/s"],["Hashrate 1 hour",i.hashRate_1h==null?"-":(i.hashRate_1h/1000).toFixed(2)+" TH/s"],["Fan",i.fanrpm==null?"-":fmtN(i.fanrpm)+" RPM ("+fmtN(i.fanspeed)+"%)"],["Wi-Fi signal",i.wifiRSSI==null?"-":i.wifiRSSI+" dBm"],["Core volts actual",i.coreVoltageActual==null?"-":fmtN(i.coreVoltageActual)+" mV"],["Input current",i.current==null?"-":(i.current/1000).toFixed(2)+" A"],["Uptime",i.uptimeSeconds==null?"-":dur(i.uptimeSeconds)],["Power per month",i.power==null?"-":fmtN(i.power*730/1000,1)+" kWh"]]);
  return f.map(x=>'<div class="fact"><div class="k">'+x[0]+'</div><div class="x">'+(x[2]?x[1]:esc(x[1]))+"</div></div>").join("");
}
const MODAL_CHARTS=[["hash","Hashrate"],["temp","Temperatures"],["power","Power"],["eff","Efficiency"],["err","Error rate"],["volt","Input voltage"]];
function openModal(ip){
  S.open=ip;const m=S.miners.find(x=>x.ip===ip);if(!m)return;
  $("#sheet").innerHTML='<div class="sh"><h3>'+nameOf(ip)+'</h3><span class="pill '+(m.online?"ok":"bad")+'">'+(m.online?"ONLINE":"OFFLINE")+'</span><div class="sp"></div><div class="seg" id="mRange">'+RANGES.map(r=>'<button data-r="'+r[0]+'">'+r[1]+"</button>").join("")+'</div><button class="btn" id="mRemove">Remove</button><button class="btn" id="mClose">Close</button></div><div class="facts" id="mFacts"></div><div class="mgrid">'+MODAL_CHARTS.map(c=>'<div class="mc"><h4>'+c[1]+'</h4><div class="chart sm"><canvas data-c="'+c[0]+'"></canvas><div class="tip"></div></div></div>').join("")+"</div>";
  $("#modal").classList.add("on");document.body.style.overflow="hidden";
  $("#mClose").onclick=closeModal;
  $("#mRemove").onclick=async()=>{if(!confirm("Stop monitoring "+nameOf(ip)+"? Its saved history stays in the database."))return;await fetch("/api/remove",{method:"POST",body:JSON.stringify({host:ip})});closeModal();const el=S.cards[ip];if(el){el.remove();delete S.cards[ip]}tick(true)};
  $("#mRange").onclick=e=>{const b=e.target.closest("button");if(!b)return;S.range=Number(b.dataset.r);store("range",String(S.range));$$("#rangeSeg button").forEach(x=>x.classList.toggle("on",Number(x.dataset.r)===S.range));loadHist()};
  renderModalCharts();
}
function renderModalCharts(){
  const ip=S.open;if(!ip||!$("#mFacts"))return;const m=S.miners.find(x=>x.ip===ip);if(!m)return;
  $("#mFacts").innerHTML=modalFacts(m);
  $$("#mRange button").forEach(b=>b.classList.toggle("on",Number(b.dataset.r)===S.range));
  if(!S.hist)return;
  $$("#sheet canvas").forEach(cv=>{
    bindHover(cv);const k=cv.dataset.c;let cfg;
    if(k==="temp"){const a=cfgFor("temp",[ip],true),b=cfgFor("vr",[ip],true);a.series[0].name="ASIC";a.series[0].color="#f7931a";b.series[0].name="VRM";b.series[0].color="#a78bfa";a.series.push(b.series[0]);cfg=a}
    else{cfg=cfgFor(k,[ip],true);cfg.series[0].color=k==="hash"?"#22d3ee":k==="power"?"#f7931a":k==="eff"?"#a78bfa":k==="err"?"#fb7185":"#34d399"}
    drawChart(cv,cfg,null)});
}
function renderBanner(){
  const el=$("#blockBanner");const found=S.miners.filter(m=>m.block&&m.block.count>m.block.acked);
  if(!found.length){el.innerHTML="";if(document.title.startsWith("BLOCK FOUND"))document.title="Bitaxe Monitor";alertRepeat(false);return}
  alertRepeat(true);
  const net=S.network||{},reward=net.reward_sats||312500000;
  let h='<div class="banner"><h2>BLOCK FOUND</h2>';
  found.forEach(m=>{
    const pl=m.pool||{},ev=(S.events||[]).find(e=>e.ip===m.ip&&e.kind==="block");
    const when=ev?" Detected "+dayStr(ev.ts)+" at "+clock(ev.ts)+".":"";
    h+="<p>"+esc(nameOf(m.ip))+" reports "+plural(m.block.count,"block")+" found."+when+"</p>";
    h+="<p>"+(pl.mode==="pool"?"Your pool decides the payout for this block.":"Reward: about "+esc(money(reward))+(pl.address?" to "+esc(pl.address):""))+"</p>";
    h+='<div class="brow">';
    if(pl.page)h+='<a class="btn" href="'+esc(pl.page)+'" target="_blank" rel="noopener">Open pool page</a>';
    if(pl.address)h+='<a class="btn" href="https://mempool.space/address/'+esc(pl.address)+'" target="_blank" rel="noopener">View address on mempool.space</a>';
    h+='<button class="btn" data-ack="'+esc(m.ip)+'">Dismiss here</button></div>';
    const key=m.ip+":"+m.block.count;
    if(!S.notified[key]){S.notified[key]=true;confetti();notify("Block found",nameOf(m.ip)+" reports a block found");alertSound()}
  });
  h+='<p style="font-weight:500;font-size:12.5px;margin-top:10px">Confirm it on your pool page and mempool.space. Dismissing hides this banner in the dashboard only and does not change the miner.</p></div>';
  el.innerHTML=h;document.title="BLOCK FOUND - Bitaxe Monitor";
}
function notify(t,b){try{if("Notification"in window&&Notification.permission==="granted"&&store("alerts")==="1")new Notification(t,{body:b})}catch(e){}}
function bellSync(){const on="Notification"in window&&Notification.permission==="granted"&&store("alerts")==="1";$("#bellBtn").classList.toggle("on",on)}
function confetti(){
  const c=$("#confetti");c.width=innerWidth;c.height=innerHeight;const ctx=c.getContext("2d");
  const cols=["#f7931a","#ffd76a","#22d3ee","#a78bfa","#34d399","#f472b6"];
  const ps=Array.from({length:180},()=>({x:Math.random()*c.width,y:-20-Math.random()*c.height*.5,vx:(Math.random()-.5)*4,vy:2+Math.random()*4,s:5+Math.random()*7,r:Math.random()*6,vr:(Math.random()-.5)*.3,c:cols[Math.floor(Math.random()*cols.length)]}));
  const t0=performance.now();
  (function f(now){ctx.clearRect(0,0,c.width,c.height);ps.forEach(p=>{p.x+=p.vx;p.y+=p.vy;p.r+=p.vr;ctx.save();ctx.translate(p.x,p.y);ctx.rotate(p.r);ctx.fillStyle=p.c;ctx.fillRect(-p.s/2,-p.s/3,p.s,p.s*.6);ctx.restore()});if(now-t0<7000)requestAnimationFrame(f);else ctx.clearRect(0,0,c.width,c.height)})(t0);
}
function renderRewards(){
  const net=S.network||{},on=S.miners.filter(m=>m.online&&m.info);
  const hs=m=>(Number(m.info.hashRate)||0)*1e9;
  const allH=on.reduce((a,m)=>a+hs(m),0),soloH=on.filter(m=>m.pool&&m.pool.mode==="solo").reduce((a,m)=>a+hs(m),0);
  const reward=net.reward_sats||312500000,netH=net.hashrate;
  let c1='<div class="rc"><h4>How you are mining</h4>';
  (S.pools||[]).forEach(g=>{
    const mode=g.mode,cls=mode==="solo"?"solo":mode==="pool"?"pooled":"unk";
    c1+='<div class="poolbox"><div class="t"><span class="pill '+cls+'">'+(mode==="solo"?"SOLO":mode==="pool"?"POOL":"UNKNOWN")+"</span><span>"+esc(g.host||"no pool")+(g.port?":"+g.port:"")+"</span></div>";
    c1+='<div class="row"><span>Miners</span><b>'+g.miners.map(ip=>esc(nameOf(ip))).join(", ")+"</b></div>";
    if(g.address)c1+='<div class="row"><span>Rewards go to</span><b class="addr">'+esc(g.address)+"</b></div>";
    else if(g.account)c1+='<div class="row"><span>Pool account</span><b>'+esc(g.account)+"</b></div>";
    if(g.stats){const st=g.stats;if(st.workers!=null)c1+='<div class="row"><span>Workers seen by pool</span><b>'+esc(st.workers)+"</b></div>";if(st.best!=null&&st.best!=="")c1+='<div class="row"><span>Best share (pool view)</span><b>'+esc(fmtDiff(st.best))+"</b></div>";if(st.shares!=null)c1+='<div class="row"><span>Shares (pool view)</span><b>'+esc(fmtN(st.shares))+"</b></div>"}
    const links=[];if(g.page)links.push('<a href="'+esc(g.page)+'" target="_blank" rel="noopener">Pool page</a>');if(g.address)links.push('<a href="https://mempool.space/address/'+esc(g.address)+'" target="_blank" rel="noopener">Address on mempool.space</a>');
    if(links.length)c1+='<div class="row"><span>Links</span><b>'+links.join(" &middot; ")+"</b></div>";
    c1+="<p>"+(mode==="solo"?"Solo: nothing is paid until one of your miners finds a whole block. Then the entire block reward goes straight to your address.":mode==="pool"?"Pool: the pool combines everyone's hashrate and shares each block reward by the work you contributed. Payouts are credited to your pool account, which this dashboard cannot see, so check the pool for your balance.":"Could not tell whether this is solo or pool mining from the pool address. Check your pool's website.")+"</p></div>";
  });
  if(!(S.pools||[]).length)c1+="<p>Waiting for miner data.</p>";
  if(!S.miners.some(m=>m.block&&m.block.reports))c1+="<p>These miners do not report block finds (needs AxeOS 2.11 or newer), so only your pool page can confirm one.</p>";
  c1+="</div>";
  let c2='<div class="rc"><h4>Reward per block</h4>';
  c2+='<div class="big">'+esc(money(reward))+"</div><p style=\"margin-top:2px\">"+(net.reward_sats?"block subsidy plus recent average fees":"block subsidy only, fees not included")+"</p>";
  if(net.usd)c2+='<div class="row"><span>Bitcoin price</span><b>$'+esc(fmtN(net.usd,0))+"</b></div>";
  if(net.height)c2+='<div class="row"><span>Block height</span><b>'+esc(fmtN(net.height))+"</b></div>";
  if(net.difficulty)c2+='<div class="row"><span>Network difficulty</span><b>'+esc(fmtDiff(net.difficulty))+"</b></div>";
  if(netH)c2+='<div class="row"><span>Network hashrate</span><b>'+esc(fmtH(netH))+"</b></div>";
  if(S.offline)c2+="<p>Offline mode is on, so price and network data are not loaded.</p>";
  else if(!netH&&!net.usd)c2+="<p>Waiting for network data from mempool.space.</p>";
  c2+="</div>";
  let c3='<div class="rc"><h4>Your odds and average earnings</h4>';
  if(netH&&allH>0){
    const share=allH/netH,perDay=1-Math.exp(144*Math.log1p(-share));
    c3+='<div class="row"><span>Your hashrate</span><b>'+esc(fmtH(allH))+"</b></div>";
    c3+='<div class="row"><span>Share of the network</span><b>'+esc(share.toExponential(2).replace("e-"," x 10^-"))+"</b></div>";
    if(soloH>0){const sShare=soloH/netH,sDay=1-Math.exp(144*Math.log1p(-sShare)),sYear=1-Math.exp(365*144*Math.log1p(-sShare));
      c3+='<div class="row"><span>Chance of a block per day (solo)</span><b>1 in '+esc(bigN(1/sDay))+"</b></div>";
      c3+='<div class="row"><span>Average wait</span><b>about '+esc(spanStr(1/sDay))+"</b></div>";
      c3+='<div class="row"><span>Chance within a year (solo)</span><b>'+esc(pctStr(sYear*100))+"</b></div>"}
    const perDaySats=share*144*reward;
    c3+='<div class="row"><span>Average earnings per day</span><b>'+esc(money(perDaySats))+"</b></div>";
    c3+='<div class="row"><span>Average earnings per month</span><b>'+esc(money(perDaySats*30.44))+"</b></div>";
    c3+="<p>"+(soloH>0?"These are averages. Solo mining pays nothing most days and the whole reward on the lucky day, so real results vary hugely. ":"")+"Earnings are before any pool fee.</p>";
  }else c3+="<p>"+(S.offline?"Offline mode is on, so odds cannot be calculated.":"Odds appear once network data and miner hashrate are available.")+"</p>";
  c3+="</div>";
  const rg=$("#rgrid");
  [["r1",c1],["r2",c2],["r3",c3]].forEach(([id,h])=>{
    let el=$("#"+id,rg);
    if(!el){el=document.createElement("div");el.className="rc";el.id=id;el.draggable=true;rg.appendChild(el)}
    el.innerHTML=h.slice('<div class="rc">'.length,h.length-6);
  });
  if(!rg._ordered){rg._ordered=true;applyOrder(rg,Array.from(rg.children),"layout:rewards",e=>e.id);makeSortable(rg,".rc","layout:rewards",e=>e.id,null)}
}
function closeModal(){S.open=null;$("#modal").classList.remove("on");document.body.style.overflow=""}
$("#modal").addEventListener("click",e=>{if(e.target.id==="modal")closeModal()});
document.addEventListener("keydown",e=>{if(e.key==="Escape")closeModal()});

function apply(s){
  if(s){S.miners=s.miners;S.scan=s.scan;S.interval=s.interval;S.network=s.network||{};S.pools=s.pools||[];S.events=s.events||[];S.offline=!!s.offline;S.tuning=s.tuning||[]}
  const has=S.miners.length>0;
  $("#main").style.display=has?"block":"none";$("#empty").classList.toggle("on",!has);
  if(!has){$("#emptyTxt").textContent=S.scan&&S.scan.scanning?"Scanning "+S.scan.subnet+" ("+S.scan.done+" of "+S.scan.total+" addresses checked)":""+(S.scan&&S.scan.last?"Scanned "+S.scan.subnet+" from "+S.scan.local_ip+" and no Bitaxe or AxeOS miner answered. If your miners are on another network, or a VPN is on, add one by IP address above.":"Scanning your network for Bitaxe and AxeOS miners")}
  if(!has)updateScanUi();
  if(has){
    if(!$(".kpi"))buildKpis();
    Object.keys(S.cards).forEach(ip=>{if(!S.miners.find(m=>m.ip===ip)){S.cards[ip].remove();delete S.cards[ip]}});
    S.miners.forEach(updateCard);
    const cg=$("#cards");
    if(!cg._ordered||cg._count!==Object.keys(S.cards).length){cg._ordered=true;cg._count=Object.keys(S.cards).length;applyOrder(cg,$$("#cards .card"),"layout:cards",e=>e.dataset.ip);makeSortable(cg,".card","layout:cards",e=>e.dataset.ip,null)}
    initSections();
    updateKpis();renderLegend();renderBanner();renderRewards();markTrash();
    if(S.open){const m=S.miners.find(x=>x.ip===S.open);if(m&&$("#mFacts"))$("#mFacts").innerHTML=modalFacts(m)}
  }
  updateScanUi();
}
function updateScanUi(){
  const sc=S.scan||{};const bar=$("#scanbar"),fill=$("#scanfill");
  bar.classList.toggle("on",!!sc.scanning);
  fill.style.width=sc.total?Math.round(sc.done/sc.total*100)+"%":"0";
  $("#scanBtn").textContent=sc.scanning?"Scanning "+(sc.total?Math.round(sc.done/sc.total*100):0)+"%":"\u21bb Refresh";
  $("#scanBtn").disabled=!!sc.scanning;$("#scanBtn2").disabled=!!sc.scanning;
}
function say(t){$("#msg").textContent=t||"";if(t)setTimeout(()=>{if($("#msg").textContent===t)$("#msg").textContent=""},6000)}
async function tick(once){
  try{
    const r=await fetch("/api/state");const s=await r.json();S.lastOk=Date.now();const was=S.scan&&S.scan.scanning;apply(s);
    if(S.hist&&S.miners.length&&!S.drawn){S.drawn=true;renderAll()}
    if(was&&!s.scan.scanning){loadHist();say(s.scan.found?"Scan finished, "+plural(s.miners.length,"miner")+" monitored":(s.miners.length?"Scan finished, no miners answered the scan (still monitoring "+s.miners.length+")":"Scan finished, no miners found"))}
    $("#dot").classList.remove("off");$("#liveTxt").textContent="Live · updated "+clock(s.now,true);
  }catch(e){$("#dot").classList.add("off");$("#liveTxt").textContent="Disconnected"}
  if(!once)setTimeout(tick,S.scan&&S.scan.scanning?1000:3000);
}
async function loadHist(){
  try{const r=await fetch("/api/history?range="+S.range);S.hist=await r.json();if(S.miners.length){S.drawn=true;renderAll()}}catch(e){}
}
async function startScan(){
  try{const r=await fetch("/api/scan",{method:"POST",body:"{}"});const j=await r.json();if(!j.ok)say(j.error||"could not start the scan");else{S.scan={scanning:true,done:0,total:1};updateScanUi()}}catch(e){say("could not reach the server")}
}
$("#scanBtn").onclick=startScan;$("#scanBtn2").onclick=startScan;
$("#addForm").onsubmit=async e=>{e.preventDefault();const v=$("#addIp").value.trim();if(!v)return;
  try{const r=await fetch("/api/add",{method:"POST",body:JSON.stringify({host:v})});const j=await r.json();if(j.ok){$("#addIp").value="";say("Added "+v);tick(true)}else say(j.error||"could not add that address")}catch(x){say("could not reach the server")}};
function setCompact(on,save){document.documentElement.classList.toggle("compact",on);if(save)store("compact",on?"1":"0")}
$("#viewBtn").onclick=()=>{setCompact(!document.documentElement.classList.contains("compact"),true);setTimeout(()=>{if(S.hist&&S.miners.length)renderAll();if(document.body.classList.contains("tab-tuner"))tnRender()},60)};
$("#themeBtn").onclick=()=>{setTheme(document.documentElement.getAttribute("data-theme")==="light"?"dark":"light",true);if(S.hist&&S.miners.length)renderAll()};
$("#blockBanner").addEventListener("click",async e=>{const b=e.target.closest("[data-ack]");if(!b)return;try{await fetch("/api/ack",{method:"POST",body:JSON.stringify({host:b.dataset.ack})})}catch(x){}tick(true)});
$("#bellBtn").onclick=async()=>{
  if(!("Notification"in window)){say("This browser does not support notifications");return}
  if(Notification.permission==="denied"){say("Notifications are blocked for this page in your browser settings");return}
  if(Notification.permission!=="granted"){const r=await Notification.requestPermission();if(r!=="granted"){bellSync();return}store("alerts","1");say("Alerts on: you will get a notification when a block is found")}
  else{const on=store("alerts")==="1";store("alerts",on?"0":"1");say(on?"Alerts off":"Alerts on: you will get a notification when a block is found")}
  bellSync()};
bellSync();
window.addEventListener("resize",()=>{if(S.hist&&S.miners.length)renderAll()});

const TN={data:null,built:false,goal:store("tn_goal")||"balanced",len:store("tn_len")||"normal",logOpen:store("tn_log")!=="0",ips:""};
const TN_GOALS={balanced:["Balanced","Best hashrate without wasting power"],efficient:["Efficient","Lowest watts for each terahash"],fast:["Fast","Highest hashrate that stays safe"]};
const TN_LEN={quick:["Quick","about 5 minutes per setting"],normal:["Normal","about 9 minutes per setting"],thorough:["Thorough","about 15 minutes per setting"]};
function tnInvT(v){return S.unit==="F"?(v-32)*5/9:v}
function showTab(t){
  document.body.classList.toggle("tab-tuner",t==="tuner");
  $$("#tabs button").forEach(b=>b.classList.toggle("on",b.dataset.t===t));
  store("tab",t);
  if(t==="tuner"){tnBuild();tnPoll()}else{renderAll()}
}
function tnSeg(id,defs,cur){return '<div class="seg" id="'+id+'">'+Object.keys(defs).map(k=>'<button data-v="'+k+'" class="'+(k===cur?"on":"")+'">'+defs[k][0]+"</button>").join("")+"</div>"}
function tnBuild(){
  const ips=S.miners.map(m=>m.ip).join(",");
  if(TN.built&&TN.ips===ips)return;
  const f=$("#tForm");
  if(!S.miners.length){f.innerHTML='<p class="note">No miners found yet. Add one on the Monitor tab first.</p>';TN.built=false;TN.ips="";return}
  const keep=TN.built?($("#tIp")||{}).value:store("tn_ip");
  TN.ips=ips;TN.built=true;
  const adv=store("tn_adv")?JSON.parse(store("tn_adv")):{};
  f.innerHTML='<label>Miner</label><select id="tIp">'+S.miners.map(m=>'<option value="'+esc(m.ip)+'">'+esc(nameOf(m.ip))+" ("+esc(m.ip)+")</option>").join("")+'</select>'+
  '<label>Goal</label>'+tnSeg("tGoal",TN_GOALS,TN.goal)+'<div class="hint" id="tGoalHint"></div>'+
  '<label>Test length</label>'+tnSeg("tLen",TN_LEN,TN.len)+'<div class="hint" id="tLenHint"></div>'+
  '<label>Temperature limit (<span id="tUnit"></span>)</label><input type="number" id="tTemp" step="1">'+
  '<label class="chk"><input type="checkbox" id="tFresh"> Start fresh and ignore saved results</label>'+
  '<label class="chk"><input type="checkbox" id="tFine"> Finer voltage steps (slower)</label>'+
  '<details><summary>Advanced limits</summary><div class="two"><div><label>Max watts</label><input type="number" id="tWatts" min="0" placeholder="none"></div><div><label>Max MHz</label><input type="number" id="tFmax" min="0" placeholder="auto"></div><div><label>Max mV</label><input type="number" id="tVmax" min="0" placeholder="auto"></div></div></details>'+
  '<div class="tbtns"><button class="btn" id="tPrev">Preview plan</button><button class="btn pri" id="tStart">Start tuning</button><button class="btn bad" id="tStop" style="display:none">Stop and restore</button><button class="btn" id="tRevert">Revert to original</button></div>';
  if(keep&&S.miners.find(m=>m.ip===keep))$("#tIp").value=keep;
  $("#tTemp").value=Math.round(tC(adv.temp!=null?adv.temp:66));
  if(adv.watts)$("#tWatts").value=adv.watts;
  $("#tUnit").textContent=tU();
  const hints=()=>{$("#tGoalHint").textContent=TN_GOALS[TN.goal][1];$("#tLenHint").textContent=TN_LEN[TN.len][1]+". Each setting is held and checked for errors."};
  hints();
  const bind=(id,key,defs)=>$("#"+id).addEventListener("click",e=>{const b=e.target.closest("button");if(!b)return;TN[key]=b.dataset.v;store(key==="goal"?"tn_goal":"tn_len",TN[key]);$$("#"+id+" button").forEach(x=>x.classList.toggle("on",x===b));hints()});
  bind("tGoal","goal");bind("tLen","len");
  $("#tIp").addEventListener("change",e=>{store("tn_ip",e.target.value);tnRender()});
  $("#tPrev").onclick=()=>tnRun("preview");
  $("#tStart").onclick=()=>tnRun("tune");
  $("#tRevert").onclick=()=>tnRun("revert");
  $("#tStop").onclick=async()=>{const ip=$("#tIp").value;if(!confirm("Stop now? The miner goes back to the settings it had before tuning."))return;await fetch("/api/tuner/stop",{method:"POST",body:JSON.stringify({host:ip})});tnPoll()};
  $("#tLogBtn").onclick=()=>{TN.logOpen=!TN.logOpen;store("tn_log",TN.logOpen?"1":"0");tnRender()};
  tnRender();
}
async function tnRun(mode){
  const ip=$("#tIp").value;
  const tempC=tnInvT(Number($("#tTemp").value)||tC(66));
  const opts={goal:TN.goal,length:TN.len,max_temp:Math.round(tempC*10)/10,fresh:$("#tFresh").checked,fine:$("#tFine").checked,max_watts:Number($("#tWatts").value)||0,freq_max:Number($("#tFmax").value)||0,volt_max:Number($("#tVmax").value)||0};
  store("tn_adv",JSON.stringify({temp:tempC,watts:opts.max_watts}));
  if(mode==="tune"&&!confirm("Start tuning "+nameOf(ip)+"?\n\nIt will change the clock and voltage and restart the miner many times. When it finishes it applies the best setting. Stop puts the original settings back."))return;
  if(mode==="revert"&&!confirm("Put "+nameOf(ip)+" back to the settings saved before the last tune?"))return;
  try{
    const r=await fetch("/api/tuner/start",{method:"POST",body:JSON.stringify({host:ip,mode,opts})});const j=await r.json();
    if(!j.ok)say(j.error||"could not start");
  }catch(e){say("could not reach the server")}
  tnPoll();
}
async function tnPoll(){
  if(!document.body.classList.contains("tab-tuner"))return;
  try{const r=await fetch("/api/tuner");TN.data=await r.json();tnBuild();tnRender()}catch(e){}
}
function tnClean(l){return String(l).replace(/^\[[^\]]*\]\s*(\[[^\]]*\]\s*)?/,"")}
function tnActivity(job){
  if(!job)return "";
  const ls=job.lines.filter(l=>l.trim());
  if(job.status==="running"){
    for(let i=ls.length-1;i>=0;i--){const t=tnClean(ls[i]);if(/^(testing|searching|PASS|FAIL|screen passed|measuring|cached|resuming|limits|applying|verifying|restor|current settings)/i.test(t)||t.length>3)return t}
  }
  if(job.status==="done"&&(job.mode==="tune")){
    for(let i=ls.length-1;i>=0;i--){const t=tnClean(ls[i]);if(/^(applied|left |no fully validated|could not find)|already at/i.test(t))return t.replace(/ to \d+\.\d+\.\d+\.\d+(:\d+)?$/,"")}
    return "Finished";
  }
  return ls.length?tnClean(ls[ls.length-1]):"";
}
function tnRows(job){
  const rows=[];const res=(job&&job.results)||{};
  Object.keys(res).forEach(k=>{const r=res[k];const [f,v]=k.split("/").map(Number);rows.push(Object.assign({f,v},r))});
  rows.sort((a,b)=>a.f-b.f||a.v-b.v);return rows;
}
function tnRender(){
  if(!TN.built||!TN.data)return;
  const d=TN.data;const ip=$("#tIp").value;const job=d.jobs[ip];
  const live=$("#tLive");
  if(!d.available){
    live.innerHTML='<div class="stat"><span class="pill warn">TUNER MISSING</span></div><p class="note">bitaxe-tuner.py was not found next to the monitor. It is needed to run tests.</p><div class="tbtns"><button class="btn pri" id="tDl">Download bitaxe-tuner.py from GitHub</button></div>';
    $("#tDl").onclick=async()=>{const r=await fetch("/api/tuner/download",{method:"POST",body:"{}"});const j=await r.json();say(j.ok?"Downloaded the tuner":(j.error||"download failed"));tnPoll()};
    $$("#tPrev,#tStart,#tRevert").forEach(b=>b.disabled=true);return;
  }
  const running=job&&job.status==="running";
  $$("#tPrev,#tStart,#tRevert").forEach(b=>b.disabled=!!running);
  $("#tStop").style.display=running?"":"none";
  tnProfiles();
  const rows=tnRows(job);
  const pass=rows.filter(r=>r.ok).length,fail=rows.length-pass;
  let h="";
  if(!job){
    h='<div class="stat"><span class="pill">IDLE</span></div><p class="note">Pick a goal and press Start tuning. Preview plan shows what would be tested and changes nothing.</p><p class="note">Tuning changes the clock and voltage and restarts the miner many times, so hashrate drops while it runs. When it finishes it applies the best setting. You can stop at any time and the original settings come back.</p>';
  }else{
    const cls={running:"warn",done:"ok",stopped:"warn",failed:"bad"}[job.status];
    const label={running:"RUNNING",done:"FINISHED",stopped:"STOPPED",failed:"FAILED"}[job.status];
    const modeName={tune:"Tuning",preview:"Preview",revert:"Revert",watch:"Watch"}[job.mode]||job.mode;
    const end=job.ended||(Date.now()/1000);
    h='<div class="stat"><span class="pill '+cls+'">'+label+'</span><b>'+modeName+' '+esc(nameOf(ip))+'</b><span class="note">'+(running?"running for ":"took ")+dur(end-job.started)+'</span></div>';
    h+='<div class="act">'+esc(tnActivity(job)||"Starting")+'</div>';
    if(job.mode==="tune"||job.mode==="preview")h+='<div class="chips"><div class="chip"><b>'+rows.length+'</b>settings tested</div><div class="chip"><b>'+pass+'</b>passed</div><div class="chip"><b>'+fail+'</b>failed</div></div>';
    if(running&&job.mode==="tune")h+='<p class="note">The miner restarts between tests, so it will show offline for short moments. Results appear below as each one finishes.</p>';
    if(job.status==="stopped")h+='<p class="note">Stopped. The original settings were restored.</p>';
    if(job.status==="failed")h+='<p class="note">The run ended with an error. Details are below.</p>';
  }
  live.innerHTML=h;
  const rp=$("#tResPanel");
  if(rows.length){rp.style.display="";tnTable(rows);tnChart(rows)}else rp.style.display="none";
  const lp=$("#tLogPanel");
  if(job&&job.lines.length){
    lp.style.display="";const pre=$("#tLog");pre.style.display=TN.logOpen?"":"none";$("#tLogBtn").textContent=TN.logOpen?"Hide":"Show";
    if(TN.logOpen){const atEnd=pre.scrollTop+pre.clientHeight>=pre.scrollHeight-30;const txt=job.lines.join("\n");if(pre.textContent!==txt){pre.textContent=txt;if(atEnd)pre.scrollTop=pre.scrollHeight}}
  }else lp.style.display="none";
}

const PROF_NAMES={balanced:"Balanced",efficient:"Efficient",fast:"Fast"};
function tnProfiles(){
  const d=TN.data;if(!d)return;const ip=$("#tIp").value;
  const rec=(d.profiles||{})[ip];const ap=(d.applies||{})[ip];
  const m=S.miners.find(x=>x.ip===ip);const cur=m&&m.info?{f:Number(m.info.frequency),v:Number(m.info.coreVoltage)}:null;
  const job=d.jobs[ip];const busy=(job&&job.status==="running")||(ap&&ap.status==="applying");
  let h="";
  if(!rec){
    h='<p class="note">No profiles yet for '+esc(nameOf(ip))+'. Profiles are saved automatically when a tune finishes. If you tuned this miner before, use Rebuild from saved results.</p>';
  }else{
    h='<div class="pgrid">'+["balanced","efficient","fast"].map(g=>{
      const p=rec.profiles[g];if(!p)return"";
      const on=cur&&cur.f===p.f&&cur.v===p.v;
      return '<div class="pc'+(on?" act":"")+'">'+(on?'<span class="pill ok tag">ACTIVE</span>':"")+'<h4>'+PROF_NAMES[g]+'</h4><div class="hs">'+esc(TN_GOALS[g][1])+'</div><div class="set">'+p.f+' MHz / '+p.v+' mV</div>'+
        '<div class="row"><span>Hashrate</span><b>'+Number(p.hash).toFixed(2)+' TH/s</b></div><div class="row"><span>Power</span><b>'+Number(p.power).toFixed(1)+' W</b></div><div class="row"><span>Efficiency</span><b>'+Number(p.jth).toFixed(2)+' J/TH</b></div><div class="row"><span>Tested temp</span><b>'+tC(Number(p.temp)).toFixed(1)+tU()+'</b></div>'+
        '<button class="btn'+(on?"":" pri")+'" data-g="'+g+'"'+(on||busy?" disabled":"")+'>'+(on?"In use":"Apply")+'</button></div>'}).join("")+"</div>";
    h+='<p class="note" style="margin-top:8px">Saved '+new Date(rec.updated*1000).toLocaleString()+' from '+rec.tested+' fully tested settings. Applying a profile restarts the miner.</p>';
  }
  if(ap)h+='<p class="note" style="margin-top:6px"><b>'+esc(ap.msg)+(ap.status==="applying"?"...":"")+"</b></p>";
  $("#tProf").innerHTML=h;
  $("#tRebuild").disabled=!!busy;
}
async function tnApply(g){
  const ip=$("#tIp").value;const rec=(TN.data.profiles||{})[ip];const p=rec&&rec.profiles[g];if(!p)return;
  if(!confirm("Apply the "+PROF_NAMES[g]+" profile to "+nameOf(ip)+"?\n\n"+p.f+" MHz / "+p.v+" mV\n\nThe miner restarts and is offline for about a minute."))return;
  try{const r=await fetch("/api/profiles/apply",{method:"POST",body:JSON.stringify({host:ip,goal:g})});const j=await r.json();if(!j.ok)say(j.error||"could not apply")}catch(e){say("could not reach the server")}
  tnPoll();
}
document.addEventListener("click",async e=>{
  const b=e.target.closest("#tProf [data-g]");if(b){tnApply(b.dataset.g);return}
  if(e.target.closest("#tRebuild")){
    const ip=$("#tIp").value;
    try{const r=await fetch("/api/profiles/rebuild",{method:"POST",body:JSON.stringify({host:ip,max_temp:tnInvT(Number($("#tTemp").value)||tC(66))})});const j=await r.json();say(j.ok?"Profiles rebuilt":(j.error||"could not rebuild"))}catch(x){say("could not reach the server")}
    tnPoll();
  }
});
function tnTable(rows){
  const full=rows.filter(r=>r.ok&&r.stage==="full");
  const best=full.length?full.reduce((a,b)=>b.jth<a.jth?b:a):null;
  $("#tRes").innerHTML='<table><thead><tr><th>Setting</th><th>Result</th><th>TH/s</th><th>J/TH</th><th>Errors</th><th>Temp</th><th>Watts</th></tr></thead><tbody>'+rows.map(r=>'<tr'+(best&&r===best?' class="best"':"")+'><td>'+r.f+" MHz / "+r.v+' mV</td><td class="'+(r.ok?"ok":"no")+'">'+(r.ok?"pass":"fail")+(r.ok?"":" ("+esc(r.reason||r.kind||"")+")")+'</td><td>'+Number(r.hash).toFixed(2)+'</td><td>'+Number(r.jth).toFixed(2)+'</td><td>'+Number(r.err).toFixed(2)+'%</td><td>'+tC(Number(r.temp)).toFixed(1)+tU()+'</td><td>'+Number(r.power).toFixed(1)+'</td></tr>').join("")+"</tbody></table>"+(best?'<p class="note" style="margin-top:8px">Highlighted row is the most efficient setting that passed the full test.</p>':"");
}
function tnChart(rows){
  const cv=$("#tChart");const box=cv.parentNode;const dpr=window.devicePixelRatio||1;
  const W=box.clientWidth,H=box.clientHeight;if(!W||!H)return;
  cv.width=W*dpr;cv.height=H*dpr;const g=cv.getContext("2d");g.scale(dpr,dpr);g.clearRect(0,0,W,H);
  const th=theme();const cs=getComputedStyle(document.documentElement);const col=k=>cs.getPropertyValue(k).trim();
  const L=52,R=16,T=14,B=38;
  const xs=rows.map(r=>Number(r.hash)),ys=rows.map(r=>Number(r.jth));
  let x0=Math.min(...xs),x1=Math.max(...xs),y0=Math.min(...ys),y1=Math.max(...ys);
  const px=(x1-x0)||0.2,py=(y1-y0)||1;x0-=px*.1;x1+=px*.1;y0-=py*.12;y1+=py*.12;
  const X=v=>L+(v-x0)/(x1-x0)*(W-L-R),Y=v=>T+(1-(v-y0)/(y1-y0))*(H-T-B);
  g.font="11px system-ui,sans-serif";g.fillStyle=th.dim;g.strokeStyle=th.grid;g.lineWidth=1;
  for(let i=0;i<=4;i++){const yv=y0+(y1-y0)*i/4,yy=Y(yv);g.beginPath();g.moveTo(L,yy);g.lineTo(W-R,yy);g.stroke();g.textAlign="right";g.fillText(yv.toFixed(py<2?2:1),L-6,yy+4)}
  for(let i=0;i<=4;i++){const xv=x0+(x1-x0)*i/4,xx=X(xv);g.textAlign="center";g.fillText(xv.toFixed(2),xx,H-B+16)}
  g.textAlign="center";g.fillText("Hashrate (TH/s), further right is faster",L+(W-L-R)/2,H-6);
  g.save();g.translate(12,T+(H-T-B)/2);g.rotate(-Math.PI/2);g.fillText("J/TH, lower is better",0,0);g.restore();
  const full=rows.filter(r=>r.ok&&r.stage==="full");const best=full.length?full.reduce((a,b)=>b.jth<a.jth?b:a):null;
  rows.forEach(r=>{
    const x=X(Number(r.hash)),y=Y(Number(r.jth));
    g.beginPath();g.arc(x,y,r===best?7:5,0,6.2832);
    g.fillStyle=r===best?col("--orange"):r.ok?col("--cyan"):col("--red");g.globalAlpha=r.ok?.9:.55;g.fill();g.globalAlpha=1;
    if(r===best||rows.length<=12){g.fillStyle=th.dim;g.textAlign="left";g.fillText(r.f+"/"+r.v,x+9,y+4)}
  });
}
$("#tabs").addEventListener("click",e=>{const b=e.target.closest("button");if(b)showTab(b.dataset.t)});
setInterval(tnPoll,2000);
window.addEventListener("resize",()=>{if(document.body.classList.contains("tab-tuner"))tnRender()});

buildSegs();buildKpis();tick();if(store("tab")==="tuner")showTab("tuner");loadHist();setInterval(loadHist,10000);
</script>
</body>
</html>
'''


if __name__ == "__main__":
    main()
