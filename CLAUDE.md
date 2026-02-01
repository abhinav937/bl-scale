# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

BLE Scale Monitor - A Raspberry Pi service that connects to a Chipsea-BLE bathroom scale via Bluetooth Low Energy and uploads weight measurements to an Apple Health API.

## Commands

### Setup
```bash
python3 -m venv venv
./venv/bin/pip install bleak aiohttp
```

### Run Manually
```bash
./venv/bin/python scale_client.py
```

### Systemd Service
```bash
# Install/update service
sudo cp scale-monitor.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable scale-monitor
sudo systemctl start scale-monitor

# View logs
journalctl -u scale-monitor -f
```

## Architecture

Single-file Python application (`scale_client.py`) using asyncio:

- **ScaleClient class**: Manages BLE connection lifecycle with state machine (IDLE → SCANNING → CONNECTING → CONNECTED)
- **BLE Protocol**: Chipsea scale uses FFF0 service with FFF1/FFF4 (notify) and FFF2/FFF5 (write) characteristics
- **Weight Parsing**: Protocol bytes `CA A0 [type] 02 [indicator] [high] [low]` where type F4 + indicator 05 = stable reading
- **Upload**: POSTs `{date, weight}` to Health API with X-API-Key header

## Configuration

Edit constants at top of `scale_client.py`:
- `SCALE_ADDRESS`: BLE MAC address of your scale
- `HEALTH_API_URL`: API endpoint for weight uploads
- `HEALTH_API_KEY`: Authentication key

## Key Patterns

- Uses `bleak` library for cross-platform BLE (optimized for Linux/BlueZ)
- Clears BlueZ cache before connecting to avoid "InProgress" errors
- Immediate connection on detection (no delay) mimics iOS behavior
- Single upload per stable reading (flag reset on disconnect)
