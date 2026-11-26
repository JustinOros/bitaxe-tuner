# bitaxe-tuner

A single-file Python tool that finds good frequency and core voltage settings for Bitaxe style Bitcoin miners running AxeOS. It tests combinations on the miner itself, throws out anything unstable or too hot, and lets you pick from the results.

Built and tested on Bitaxe Gamma 601 boards (BM1370). Other chips run with conservative limits and a warning.

## What it does

- Finds miners on your network, or takes IP addresses directly
- Starts from the miner's current settings and searches for the lowest stable voltage at each frequency
- Climbs frequency in steps until it hits a temperature, power, input voltage or stability limit
- Rejects any combination with high error rate, rejected shares, low hashrate or too much heat
- Shows a menu at the end: recommended, fastest, most efficient, power saver, keep current, or any tested combo
- Confirms the setting you pick by holding it for a few minutes and falls back to a lower power setting if it fails
- Prints a before and after table with hashrate, watts, J/TH, temperature and monthly kWh
- Restores the original settings if you stop it, close the terminal or lose connection

## Requirements

- Python 3.9 or newer (standard library only, nothing to install)
- Miners reachable over your local network
- Optional: `nmap` for scanning with `--nmap` (the built-in scanner is the default)

## Windows

Paste this into PowerShell. It downloads the helper script, which finds Python (and offers to install it if missing, as a portable copy in the folder or system wide with winget), downloads the tuner, then scans your network for miners:

```
mkdir $HOME\bitaxe-tuner -Force | Out-Null; cd $HOME\bitaxe-tuner; iwr https://raw.githubusercontent.com/JustinOros/bitaxe-tuner/main/bitaxe-tuner.ps1 -OutFile bitaxe-tuner.ps1; powershell -ExecutionPolicy Bypass -File .\bitaxe-tuner.ps1 --scan
```

To run it again later:

```
cd $HOME\bitaxe-tuner; powershell -ExecutionPolicy Bypass -File .\bitaxe-tuner.ps1 --scan
```

Every option in this README works the same way after `bitaxe-tuner.ps1`. The helper keeps the PC awake while it runs. If `nmap` is missing, `--scan` offers to install it with winget.

Note: If you already know the IP address of your miner, run this command to get started (replace the example address with yours):

```
cd $HOME\bitaxe-tuner; powershell -ExecutionPolicy Bypass -File .\bitaxe-tuner.ps1 192.168.1.109
```

## Quick start

Find your miners and pick one:

```
python3 bitaxe-tuner.py --scan
```

Tune a specific miner:

```
python3 bitaxe-tuner.py 192.168.1.109
```

See the plan without changing anything:

```
python3 bitaxe-tuner.py 192.168.1.109 --dry-run
```

Go for the highest hashrate and apply it without prompts:

```
python3 bitaxe-tuner.py 192.168.1.109 --goal fast --yes
```

Tune several miners at once:

```
python3 bitaxe-tuner.py 192.168.1.103 192.168.1.109
```

Undo the last tune:

```
python3 bitaxe-tuner.py 192.168.1.109 --revert
```

Keep a miner cool through hot weather (runs until you press Ctrl+C):

```
python3 bitaxe-tuner.py 192.168.1.103 --watch
```

## Live dashboard

`bitaxe-monitor.py` is a web dashboard for watching your miners in real time. It scans your network, finds every Bitaxe and AxeOS miner, and shows live hashrate, temperature, power, efficiency, error rate, input voltage and shares, with charts over time. The Monitor tab only reads from your miners. The Tuner tab can change their settings, and only when you press Start tuning or Revert.

```
python3 bitaxe-monitor.py
```

It opens in your browser at `http://127.0.0.1:8765/`. Add `--lan` to open it from your phone or another computer on your network.

- Click Refresh to scan the network again after adding a miner, or type an address in the add box.
- Click a miner card for detailed charts and every stat.
- Pick a time range from 5 minutes to 30 days. History is saved in `bitaxe-monitor.db`, so it keeps building across restarts.
- Switch between Fahrenheit and Celsius, and between dark and light mode.
- Each miner is labeled SOLO or POOL based on the pool it is connected to. The Mining mode and rewards panel shows where rewards go, the current block reward in BTC and USD, your odds of finding a block, and your average earnings. Pools it does not recognize are labeled unknown.
- Block found alerts: if a miner reports a block (AxeOS 2.11 or newer), a banner and confetti appear, and the bell button turns on browser notifications. Confirm any find on your pool page and mempool.space.
- Block alert sound: a short fanfare plays when a block is found and repeats every minute until you dismiss the banner. The speaker button in the top bar turns it on or off and has a Play test button. Browsers only allow sound after you have clicked on the page once.
- Bitcoin price, network data and pool statistics come from mempool.space and your pool's public pages. Use `--offline` to turn that off.
- Compact view: the button next to the dark mode button switches to a dense text-only layout with no gauges, graphs or glow, so everything fits on one screen. The Trends panel is hidden in this view. The choice is remembered.
- Trash: hover over any card or panel and click the x to take it off the dashboard. The trash button in the top bar lists everything you removed, with Restore for each item and Restore all. This only hides it, and the miner keeps being monitored.
- Drag any card or panel to reorder it. Your layout is remembered, and Reset layout restores the default.
- Tuner tab: run `bitaxe-tuner.py` from the browser. Pick a miner, a goal and a test length, preview the plan, then start. It shows live progress, a results table and chart, and the full log. Stop puts the original settings back. Each finished tune saves Balanced, Efficient and Fast profiles for that miner in `bitaxe-monitor-profiles.json`. The Profiles section shows them with an Apply button, marks the one in use, and Rebuild from saved results creates profiles from a tune you ran earlier. Keep `bitaxe-tuner.py` in the same folder as the monitor, or the tab offers to download it. There is no password, so only use `--lan` on a network you trust.
- Options: `--port`, `--lan`, `--interval`, `--subnet`, `--no-scan`, `--no-browser`, `--max-days`, `--offline`.

## How a tune works

1. Reads the miner's model, current frequency, voltage, power and input voltage.
2. Saves the original settings to a backup file.
3. Checks the current settings first. If the miner has run for at least 10 minutes with clean numbers, it uses that live data and skips the restart.
4. Binary-searches voltage at the current frequency, then steps frequency up and repeats. Each new frequency starts from the last good voltage, so most steps take one or two tests.
5. Each test has a short screen. Only combinations that pass it get the full hold. A test ends early on rejected shares, high error or high temperature.
6. Stops climbing when no voltage passes, the ceiling for the chip is reached, or the temperature is within a few degrees of the limit.
7. Restores the original settings, prints the results, and shows the menu.

## Options

Selecting miners

| Option | Meaning |
|---|---|
| `hosts` | One or more miner IP addresses |
| `--scan` | Find miners on the network and choose from a list |
| `--subnet` | Subnet to scan, for example `192.168.1.0/24` (default: your own /24) |
| `--rescan` | Ignore the saved miner list and sweep the whole subnet |
| `--nmap` | Use nmap for scanning, asks before installing it |
| `--sequential` | Tune miners one at a time instead of together |

What to optimize

| Option | Default | Meaning |
|---|---|---|
| `--goal` | `balanced` | `balanced`, `efficient` or `fast` picks the recommended option |
| `--saver-pct` | 85 | Power saver keeps at least this percent of the fastest hashrate |
| `--headroom` | 2 | Recommended pick must be this many degrees under the temperature limit |
| `--max-watts` | off | Reject combinations above this power draw |

Limits and safety (temperatures in Celsius)

| Option | Default | Meaning |
|---|---|---|
| `--max-temp` | 66 | ASIC temperature a test may not exceed |
| `--hard-temp` | 69 | Immediate abort temperature |
| `--hard-vr-temp` | 90 | Immediate abort for the voltage regulator |
| `--temp-margin` | 3 | Stop climbing when this close to the limit |
| `--max-error` | 1.0 | Highest average error percentage allowed |
| `--min-input` | auto | Lowest input voltage in millivolts, default is 93 percent of nominal |
| `--freq-max` | chip default | Highest frequency to try |
| `--volt-max` | chip default | Highest core voltage to try |

Test timing

| Option | Default | Meaning |
|---|---|---|
| `--settle` | 150 s | Longest wait for temperature to stabilize after a change |
| `--settle-min` | 45 s | Shortest wait |
| `--screen` | 90 s | Quick first pass |
| `--hold` | 300 s | Full test for combinations that pass the screen |
| `--confirm` | 300 s | Final check on the setting you pick, 0 skips it |
| `--poll` | 15 s | Seconds between readings |
| `--freq-step` | 25 | Frequency step in MHz |
| `--volt-step` | 10 | Voltage step in mV |
| `--fine` | off | Use 5 mV voltage steps |

Files and output

| Option | Meaning |
|---|---|
| `--fresh` | Ignore saved results and start over |
| `--max-age` | Discard saved results older than this many days (default 7, 0 keeps them) |
| `--state`, `--csv` | Override the state and results file names (single miner only) |
| `--json` | Write a JSON summary for each miner |
| `--fahrenheit` | Show temperatures in Fahrenheit (limit options stay in Celsius) |
| `--yes` | Skip prompts and take the recommended pick |
| `--force` | Tune even if the miner is not hashing |
| `--dry-run` | Print the plan and change nothing |

Other modes

| Option | Meaning |
|---|---|
| `--revert` | Restore the settings saved before the last tune |
| `--watch` | Keep running and lower frequency when the ASIC stays hot, raise it again when it cools |
| `--watch-temp` | Watch mode temperature limit (default 64) |
| `--watch-cooldown` | Seconds between watch mode changes (default 600) |
| `--watch-gap` | Degrees below the limit before stepping back up (default 6) |
| `--no-restart` | Experimental: apply settings live without restarting the miner |

## Files it creates

These are written in the folder you run the tool from.

| File | Contents |
|---|---|
| `bitaxe-tuner-<ip>.json` | Saved test results, used to resume |
| `bitaxe-tuner-<ip>.csv` | Results table |
| `bitaxe-tuner-backup-<ip>.json` | Original settings, used by `--revert` and crash recovery |
| `bitaxe-tuner-<ip>-summary.json` | Summary, only with `--json` |
| `bitaxe-tuner-hosts.json` | Miners found by `--scan` |

## Things worth knowing

- Overclocking can damage hardware and shorten its life. Use it at your own risk.
- Results depend on room temperature. A tune done on a cool morning will run hotter in summer. Use `--watch`, or retune when the weather changes.
- The usual limit on a stock Gamma is the 5V supply, not the cooler. If error rises as input voltage sags, a stronger supply helps more than different settings.
- Efficiency is fairly flat across a wide range, so more hashrate mostly costs proportionally more power. The menu shows watts and kWh per month for each choice so you can decide.
- Short holds mean combinations within a percent or two of each other can swap places between runs.
- Verified only on BM1370 boards. Other chips use conservative limits. Watch mode and `--no-restart` have been tested only against a simulated miner.

## License

Add the license of your choice before sharing.
