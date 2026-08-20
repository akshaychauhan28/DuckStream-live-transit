# Deploying the capture script to the always-on VM

The capture script needs Python 3 and `requests`. Nothing else — no Docker, no
broker, no project dependencies. That minimalism is the point (see
docs/DECISIONS.md #3).

## Before you deploy

Run the smoke test **locally** first. It costs two API calls and tells you
whether the key works, what the payload looks like, and — importantly — whether
the API returns rate-limit headers:

```bash
python capture/smoke_test.py
```

Do not start continuous capture until you know your quota. See
docs/DECISIONS.md #1.

## Start collecting today, move to the VM later

Cloud signup and provisioning can take hours or days (identity verification,
capacity errors). Every hour spent waiting is archive you don't get.

So: run capture on the laptop **now**, and move it to the VM when it's ready.
Laptop-uptime-only data is worth more than no data, and the format is identical
so the archives merge cleanly.

```bash
# Windows PowerShell, from the repo root
$env:OC_TRANSPO_PRIMARY_KEY = "<your key>"
python capture/capture.py
```

## On the VM

```bash
sudo apt update && sudo apt install -y python3 python3-pip python3-venv rsync
git clone <your repo> ~/duckstream
cd ~/duckstream
python3 -m venv .venv
.venv/bin/pip install requests

mkdir -p ~/duckstream-raw
```

Put the key somewhere systemd can read but other users cannot:

```bash
sudo mkdir -p /etc/duckstream
printf 'OC_TRANSPO_PRIMARY_KEY=%s\nCAPTURE_INTERVAL_SECONDS=30\nCAPTURE_OUTPUT_DIR=/home/ubuntu/duckstream-raw\n' '<your key>' | sudo tee /etc/duckstream/capture.env >/dev/null
sudo chmod 600 /etc/duckstream/capture.env
```

Install the service:

```bash
sudo cp capture/duckstream-capture.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now duckstream-capture
systemctl status duckstream-capture
journalctl -u duckstream-capture -f
```

## Pulling the archive down to the laptop

From the laptop, whenever it's awake:

```bash
rsync -avz --partial ubuntu@<vm-ip>:duckstream-raw/ ./raw/
```

Run it often. Until a file is on the laptop it exists in exactly one place, and
the VM is the least durable part of this system.

## Monitoring — do not skip this

The failure that actually bites is a full disk, silently. Check periodically:

```bash
df -h ~
du -sh ~/duckstream-raw
ls -lt ~/duckstream-raw | head
```

Expect very roughly 100–250 MB/day at 30s polling, so ~3–8 GB over 30 days —
but measure your first real day rather than trusting that estimate, since it
depends entirely on fleet size and payload verbosity.

Once you've rsynced files down and verified them, deleting the VM-side copies
older than a few days keeps the disk clear.
