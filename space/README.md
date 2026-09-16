---
title: DuckStream Capture
emoji: 🦆
colorFrom: blue
colorTo: gray
sdk: docker
app_port: 7860
pinned: false
---

# DuckStream capture

Continuously polls the OC Transpo GTFS-Realtime feeds and appends the raw,
undecoded responses to a private Dataset repo on the Hub.

This Space exists because GTFS-Realtime is a snapshot feed: it reports where
buses are right now, and there is no endpoint that returns last Tuesday. Data
not collected is gone permanently, so collection has to run somewhere that
doesn't sleep.

The page it serves is a status readout — uptime, file count, and how long ago
the last poll was written. Free Spaces sleep after 48 hours without a visitor,
so a scheduled ping keeps it awake.

Source: https://github.com/akshaychauhan28/DuckStream-live-transit
