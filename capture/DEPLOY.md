# Running capture on Android with Termux

The collector runs on an Android phone in Termux. It polls both OC Transpo
GTFS-Realtime feeds and writes the original responses as compressed frames.
The laptop pulls those files over the local network when it is available. The
phone does not need the laptop to be awake while it collects.

## Set up Termux

Install Termux from its official F-Droid or GitHub distribution, open it, then
install Python and Git:

```sh
pkg update
pkg install python git
git clone https://github.com/akshaychauhan28/DuckStream-live-transit.git duckstream
cd duckstream
python -m pip install requests
mkdir -p raw
```

Keep the checkout and archive in Termux's private home directory (`~/duckstream`)
so Android shared-storage permissions do not interfere with writes. Set the
OC Transpo key and capture output directory in the Termux session:

```sh
export OC_TRANSPO_PRIMARY_KEY='<your key>'
export CAPTURE_OUTPUT_DIR="$HOME/duckstream/raw"
```

The default polling intervals are 45 seconds for VehiclePositions and 45
seconds for TripUpdates. Set the measured TripUpdates interval to 120 seconds:

```sh
export CAPTURE_INTERVAL_TRIP_UPDATES_SECONDS=120
```

Before leaving capture running, run the smoke test if desired. It needs only the
API key; protobuf decoding is optional there:

```sh
python capture/smoke_test.py
```

Start collection:

```sh
python capture/capture.py
```

Leave that Termux session running. To collect through Android screen-off,
disable battery optimization for Termux in Android's app settings and use
`termux-wake-lock` if it is available in your Termux installation. Check the
capture log output periodically and make sure the phone has free storage.
Ctrl-C stops capture cleanly after the current poll.

## Let the laptop pull the archive

In a second Termux session, serve the raw folder on the local network:

```sh
cd "$HOME/duckstream/raw"
python -m http.server 8000 --bind 0.0.0.0
```

Keep the phone and laptop on the same trusted Wi-Fi network. Find the phone's
LAN address in Android's Wi-Fi settings and set `PHONE_RAW_URL` in the laptop's
`.env`, for example:

```text
PHONE_RAW_URL=http://192.168.1.5:8000
```

Reserve the phone's address in the router if possible. Whenever the laptop is
awake, run this from the laptop's repository checkout:

```sh
python scripts/pull_from_phone.py
```

The pull script downloads new files and refreshes the current file as it grows.
It skips completed files that already match. After pulling, check the archive
with `python capture/inspect_archive.py` and rebuild derived data with
`python storage/writer.py` as needed.

## Operational notes

- Keep the phone plugged in for long collection runs and monitor free space.
- The raw files are the durable source of truth. Pull them to another machine
  regularly; data still only on the phone has no second copy.
- Both feeds use 45 seconds unless overridden. TripUpdates at 120 seconds is
  the measured setting; see [DECISIONS #1](../docs/DECISIONS.md).
- The capture process stores failed polls as frames too, allowing later checks
  to distinguish API failures from times when capture was not running.
