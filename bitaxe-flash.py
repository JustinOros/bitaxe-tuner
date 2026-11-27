#!/usr/bin/env python3
import argparse
import glob
import hashlib
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
import venv
import webbrowser
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
HOME = os.path.join(os.path.expanduser("~"), ".bitaxe-flash")
VENV = os.path.join(HOME, "venv")
BACKUP_DIR = os.path.join(HERE, "bitaxe-backups")
DOWNLOAD_DIR = os.path.join(HERE, "bitaxe-firmware")
RELEASE_API = "https://api.github.com/repos/bitaxeorg/ESP-Miner/releases/latest"
UA = {"User-Agent": "bitaxe-flash"}
BAUD = "460800"
POOL_HOST = "public-pool.io"
POOL_PORT = 21496
CHARSET = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"
PY = sys.executable


def ask(prompt, default=None):
    suffix = " [%s]" % default if default else ""
    try:
        value = input("%s%s: " % (prompt, suffix)).strip()
    except EOFError:
        value = ""
    return value or (default or "")


def yes_no(prompt, default=True):
    hint = "Y/n" if default else "y/N"
    while True:
        value = ask("%s (%s)" % (prompt, hint)).lower()
        if not value:
            return default
        if value in ("y", "yes"):
            return True
        if value in ("n", "no"):
            return False


def venv_python():
    sub = "Scripts" if os.name == "nt" else "bin"
    exe = "python.exe" if os.name == "nt" else "python"
    return os.path.join(VENV, sub, exe)


def have_esptool(py):
    try:
        r = subprocess.run([py, "-m", "esptool", "--help"], capture_output=True, text=True)
    except OSError:
        return False
    return r.returncode == 0


def ensure_esptool():
    global PY
    if have_esptool(sys.executable):
        PY = sys.executable
        return
    vpy = venv_python()
    if os.path.exists(vpy) and have_esptool(vpy):
        PY = vpy
        return
    print("The esptool flashing tool is needed and is not installed.")
    if not yes_no("Install it into a private environment at %s?" % VENV):
        sys.exit("Cannot continue without esptool.")
    os.makedirs(HOME, exist_ok=True)
    if not os.path.exists(vpy):
        try:
            venv.EnvBuilder(with_pip=True).create(VENV)
        except Exception as e:
            sys.exit("Could not create the environment: %s\nOn Debian or Ubuntu run: sudo apt install python3-venv" % e)
    r = subprocess.run([vpy, "-m", "pip", "install", "--upgrade", "esptool"])
    if r.returncode != 0 or not have_esptool(vpy):
        sys.exit("Installing esptool failed.")
    PY = vpy


def list_ports():
    code = (
        "import serial.tools.list_ports as p\n"
        "for x in p.comports():\n"
        "    print(x.device + '\\t' + (x.description or ''))\n"
    )
    r = subprocess.run([PY, "-c", code], capture_output=True, text=True)
    ports = []
    for line in r.stdout.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        dev = parts[0]
        desc = parts[1] if len(parts) > 1 else ""
        low = (dev + desc).lower()
        if "bluetooth" in low or "debug-console" in low or dev.startswith("/dev/tty."):
            continue
        ports.append((dev, desc))
    return ports


def pick_port(arg):
    if arg:
        return arg
    ports = list_ports()
    if not ports:
        print("No serial device found.")
        print("Connect the miner with a data capable USB-C cable.")
        print("If it still does not show up, hold the BOOT button while plugging it in.")
        manual = ask("Enter a port manually, or press Enter to quit")
        if not manual:
            sys.exit(1)
        return manual
    print("Serial devices found:")
    for i, (dev, desc) in enumerate(ports, 1):
        print("  %d) %s  %s" % (i, dev, desc))
    while True:
        choice = ask("Pick the miner", "1")
        if choice.isdigit() and 1 <= int(choice) <= len(ports):
            return ports[int(choice) - 1][0]


def esp(port, *args, capture=False):
    cmd = [PY, "-m", "esptool", "--chip", "esp32s3", "--port", port, "--baud", BAUD] + list(args)
    if capture:
        return subprocess.run(cmd, capture_output=True, text=True)
    return subprocess.run(cmd)


def inspect_chip(port):
    print("\nReading the chip...")
    r = esp(port, "flash_id", capture=True)
    out = (r.stdout or "") + (r.stderr or "")
    if r.returncode != 0:
        print(out)
        sys.exit(
            "Could not talk to the chip. Try another cable or port, or hold BOOT while plugging it in."
        )
    for line in out.splitlines():
        low = line.lower()
        if low.startswith("chip") or low.startswith("mac") or "flash size" in low or "features" in low:
            print("  " + line.strip())
    if "ESP32-S3" not in out:
        print("\nWARNING: this does not look like an ESP32-S3, which a Bitaxe Gamma uses.")
        if not yes_no("Continue anyway?", False):
            sys.exit(1)
    m = re.search(r"flash size:\s*(\d+)\s*(MB|KB)", out, re.I)
    if not m:
        return None
    unit = 1024 * 1024 if m.group(2).upper() == "MB" else 1024
    return int(m.group(1)) * unit


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def backup_firmware(port, size):
    if not size:
        mb = ask("Flash size in MB was not detected. Enter it", "8")
        size = int(mb) * 1024 * 1024
    os.makedirs(BACKUP_DIR, exist_ok=True)
    stamp = datetime.now().strftime("%Y-%m-%d-%I%M%p")
    path = os.path.join(BACKUP_DIR, "original-firmware-%s.bin" % stamp)
    print("\nBacking up the original firmware. This takes about 3 minutes.")
    r = esp(port, "read_flash", "0x0", hex(size), path)
    if r.returncode != 0 or not os.path.exists(path) or os.path.getsize(path) != size:
        sys.exit("Backup failed, so nothing was changed on the miner.")
    print("Backup saved: %s" % path)
    print("SHA256: %s" % sha256_file(path))
    return path


def http_json(url):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode())


def download_asset(asset):
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    dest = os.path.join(DOWNLOAD_DIR, asset["name"])
    req = urllib.request.Request(asset["browser_download_url"], headers=UA)
    print("Downloading %s" % asset["name"])
    with urllib.request.urlopen(req, timeout=60) as r, open(dest, "wb") as f:
        while True:
            chunk = r.read(65536)
            if not chunk:
                break
            f.write(chunk)
    digest = asset.get("digest") or ""
    if digest.startswith("sha256:"):
        if sha256_file(dest) != digest[7:]:
            os.remove(dest)
            sys.exit("Checksum mismatch on the download. It was deleted.")
        print("Checksum verified.")
    else:
        print("The release does not publish a checksum for this file.")
    return dest


def get_firmware(rev, binpath):
    if binpath:
        if not os.path.exists(binpath):
            sys.exit("File not found: %s" % binpath)
        return binpath
    try:
        data = http_json(RELEASE_API)
    except Exception as e:
        print("Could not reach GitHub: %s" % e)
        path = ask("Path to a factory .bin you downloaded yourself")
        if not path or not os.path.exists(path):
            sys.exit(1)
        return path
    print("\nLatest official ESP-Miner release: %s" % data.get("tag_name", "unknown"))
    assets = [
        a for a in data.get("assets", [])
        if a["name"].lower().endswith(".bin") and "factory" in a["name"].lower()
    ]
    pattern = re.compile(r"(?<!\d)%s(?!\d)" % re.escape(rev))
    matches = [a for a in assets if pattern.search(a["name"])]
    if not matches:
        print("No factory image for revision %s was found. Factory files in this release:" % rev)
        for a in assets:
            print("  " + a["name"])
        path = ask("Path to the correct factory .bin you downloaded yourself")
        if not path or not os.path.exists(path):
            sys.exit(1)
        return path
    if len(matches) == 1:
        return download_asset(matches[0])
    for i, a in enumerate(matches, 1):
        print("  %d) %s" % (i, a["name"]))
    while True:
        choice = ask("Pick the image", "1")
        if choice.isdigit() and 1 <= int(choice) <= len(matches):
            return download_asset(matches[int(choice) - 1])


def confirm_flash(rev, path):
    print("\n" + "=" * 62)
    print("About to ERASE the miner and install: %s" % os.path.basename(path))
    print("Before you continue, make sure of the following:")
    print("  - The silkscreen near the power jack reads %s" % rev)
    print("  - The ASIC chip is a BM1370")
    print("  - You power it only with the adapter made for this board")
    print("    (a real Gamma runs on 5V, so never use a 12V supply on it)")
    print("  - A backup of the original firmware was saved above")
    print("=" * 62)
    return ask("Type FLASH to continue, or anything else to stop") == "FLASH"


def write_image(port, path):
    print("\nFlashing. This takes about 2 minutes. Do not unplug the cable.")
    r = esp(port, "write_flash", "--erase-all", "0x0", path)
    if r.returncode != 0:
        print("\nFlashing failed. Try holding the BOOT button while plugging in the cable, then run this again.")
        print("Your original firmware backup is in %s" % BACKUP_DIR)
        sys.exit(1)
    print("\nFlash complete.")


def bech32_polymod(values):
    gen = [0x3B6A57B2, 0x26508E6D, 0x1EA119FA, 0x3D4233DD, 0x2A1462B3]
    chk = 1
    for v in values:
        b = chk >> 25
        chk = ((chk & 0x1FFFFFF) << 5) ^ v
        for i in range(5):
            if (b >> i) & 1:
                chk ^= gen[i]
    return chk


def valid_address(addr):
    addr = addr.strip()
    if addr != addr.lower() and addr != addr.upper():
        return False
    addr = addr.lower()
    if len(addr) not in (42, 62) or not addr.startswith("bc1"):
        return False
    pos = addr.rfind("1")
    hrp, data = addr[:pos], addr[pos + 1:]
    try:
        vals = [CHARSET.index(c) for c in data]
    except ValueError:
        return False
    pm = bech32_polymod([ord(c) >> 5 for c in hrp] + [0] + [ord(c) & 31 for c in hrp] + vals)
    return pm in (1, 0x2BC830A3)


def api(ip, path, method="GET", body=None, timeout=8):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        "http://%s%s" % (ip, path),
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read().decode()
    try:
        return json.loads(raw) if raw else {}
    except ValueError:
        return {}


def wait_for_miner(ip, limit=300):
    print("Waiting for the miner at %s (up to 5 minutes)..." % ip)
    end = time.time() + limit
    while time.time() < end:
        try:
            return api(ip, "/api/system/info")
        except Exception:
            time.sleep(5)
    return None


def show_info(info):
    for label, key in (
        ("Hostname", "hostname"),
        ("ASIC", "ASICModel"),
        ("Board", "boardVersion"),
        ("Firmware", "version"),
        ("Hashrate (GH/s)", "hashRate"),
        ("Pool", "stratumURL"),
        ("Pool user", "stratumUser"),
    ):
        if key in info:
            print("  %-16s %s" % (label, info[key]))


def configure_pool(ip, address, worker):
    user = "%s.%s" % (address, worker)
    body = {"stratumURL": POOL_HOST, "stratumPort": POOL_PORT, "stratumUser": user}
    try:
        api(ip, "/api/system", "PATCH", body)
    except Exception as e:
        print("Could not set the pool through the API: %s" % e)
        return False
    try:
        api(ip, "/api/system/restart", "POST")
    except Exception:
        pass
    time.sleep(15)
    info = wait_for_miner(ip, 120)
    if info and info.get("stratumUser") == user:
        print("Pool saved: %s:%s as %s" % (POOL_HOST, POOL_PORT, user))
        return True
    print("The miner did not confirm the new pool settings.")
    return False


def after_flash(ip_arg):
    print("\nNext: power the miner with its proper adapter, wait about 1 minute, then join the")
    print("setup Wi-Fi network it creates and enter your home Wi-Fi details.")
    ip = ip_arg or ask("Enter the miner's IP address once it joins your network (Enter to skip)")
    if not ip:
        print("Run again with --ip to finish setup later.")
        return
    info = wait_for_miner(ip)
    if not info:
        print("The miner did not answer at %s." % ip)
        return
    print("\nThe miner is up:")
    show_info(info)
    webbrowser.open("http://%s" % ip)
    if not yes_no("\nSet the pool and payout address now?"):
        return
    while True:
        addr = ask("Your bitcoin payout address (starts with bc1)")
        if valid_address(addr):
            break
        print("That address did not pass the checksum. Check it and try again.")
    worker = ask("Worker name", "BTCminer03")
    if not configure_pool(ip, addr.strip(), worker):
        print("\nSet these by hand in AxeOS under Pool:")
        print("  Host: %s" % POOL_HOST)
        print("  Port: %s" % POOL_PORT)
        print("  User: %s.%s" % (addr.strip(), worker))
    print("\nTo watch it in the monitor, add %s to the miner list." % ip)


def pick_backup():
    files = sorted(glob.glob(os.path.join(BACKUP_DIR, "*.bin")), key=os.path.getmtime, reverse=True)
    if not files:
        sys.exit("No backups found in %s" % BACKUP_DIR)
    print("\nAvailable backups (newest first):")
    for i, f in enumerate(files, 1):
        st = os.stat(f)
        when = datetime.fromtimestamp(st.st_mtime).strftime("%b %d, %Y %I:%M %p")
        print("  %d) %s  (%s, %.1f MB)" % (i, os.path.basename(f), when, st.st_size / 1048576))
    while True:
        choice = ask("Pick a backup to restore", "1")
        if choice.isdigit() and 1 <= int(choice) <= len(files):
            return files[int(choice) - 1]


def main():
    ap = argparse.ArgumentParser(description="Back up, flash and set up a Bitaxe Gamma over USB-C")
    ap.add_argument("--port", help="serial port of the miner")
    ap.add_argument("--rev", help="board revision printed on the silkscreen, for example 601")
    ap.add_argument("--bin", help="flash this local factory .bin instead of downloading one")
    ap.add_argument(
        "--restore",
        nargs="?",
        const="",
        default=None,
        help="write a backup back to the miner; give a file, or leave it blank to choose from a list",
    )
    ap.add_argument("--ip", help="the miner's IP address, for the final setup step")
    args = ap.parse_args()

    ensure_esptool()
    port = pick_port(args.port)
    size = inspect_chip(port)

    if args.restore is not None:
        target = args.restore or pick_backup()
        if not os.path.exists(target):
            sys.exit("File not found: %s" % target)
        if size and os.path.getsize(target) != size:
            print("WARNING: this file is %d bytes but the miner's flash is %d bytes." % (os.path.getsize(target), size))
            if not yes_no("Continue anyway?", False):
                sys.exit(1)
        if ask("Type RESTORE to write %s back to the miner" % os.path.basename(target)) != "RESTORE":
            sys.exit(1)
        write_image(port, target)
        return

    backup_firmware(port, size)

    rev = args.rev or ""
    while not re.fullmatch(r"\d{3}", rev):
        rev = ask("Board revision printed on the silkscreen near the power jack", "601")
    path = get_firmware(rev, args.bin)

    if not confirm_flash(rev, path):
        sys.exit("Stopped. Nothing was changed.")
    write_image(port, path)
    after_flash(args.ip)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped.")
        sys.exit(130)
