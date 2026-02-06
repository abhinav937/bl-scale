# BLE Scale Monitor

Raspberry Pi service that connects to a Chipsea-BLE bathroom scale and uploads weight measurements to an Apple Health API.

## How It Works

1. The script continuously scans for the BLE scale
2. When you step on the scale, it connects and reads weight data
3. Once the weight stabilizes, it uploads to the Apple Health API
4. The scale disconnects after measurement, and the script waits for the next use

## Setup

### Install Dependencies

```bash
python3 -m venv venv
./venv/bin/pip install bleak aiohttp
```

### Configure

Edit `scale_client.py` to set your scale's MAC address and API credentials:

```python
SCALE_ADDRESS = "DE:E7:54:8A:87:0A"  # Your scale's BLE address
HEALTH_API_URL = "https://ai-reply-bot.vercel.app/api/health-api"
HEALTH_API_KEY = "your-api-key"
```

### Run Manually

```bash
./venv/bin/python scale_client.py
```

## Systemd Service (24/7 Operation)

### Install Service

```bash
sudo cp scale-monitor.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable scale-monitor
sudo systemctl start scale-monitor
```

### Service Commands

| Command | Description |
|---------|-------------|
| `sudo systemctl status scale-monitor` | Check if running |
| `sudo systemctl start scale-monitor` | Start service |
| `sudo systemctl stop scale-monitor` | Stop service |
| `sudo systemctl restart scale-monitor` | Restart service |
| `sudo systemctl enable scale-monitor` | Enable on boot |
| `sudo systemctl disable scale-monitor` | Disable on boot |

### View Logs

```bash
# Live logs
journalctl -u scale-monitor -f

# Last 50 lines
journalctl -u scale-monitor -n 50

# Logs from last hour
journalctl -u scale-monitor --since "1 hour ago"

# Logs from today
journalctl -u scale-monitor --since today
```

## After Updating Code

The service must be restarted to pick up code changes:

```bash
sudo systemctl restart scale-monitor
```

If you modify `scale-monitor.service`:

```bash
sudo cp scale-monitor.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl restart scale-monitor
```

## Troubleshooting

### Scale not found

- Step on the scale briefly to wake it up (it sleeps to save battery)
- Check Bluetooth is enabled: `bluetoothctl power on`
- Verify the MAC address matches your scale

### Connection issues

If you get "InProgress" or pairing errors:

```bash
bluetoothctl
> remove DE:E7:54:8A:87:0A
> scan on
# Wait for scale to appear
> pair DE:E7:54:8A:87:0A
> quit
```

### Service won't start

```bash
# Check for errors
journalctl -u scale-monitor -n 100

# Verify Python path
ls -la /home/abhinav/bl-scale/venv/bin/python
```

## Files

| File | Description |
|------|-------------|
| `scale_client.py` | Main Python script |
| `scale-monitor.service` | Systemd service file |
| `venv/` | Python virtual environment |

## API Payload

When a stable weight is detected, the script POSTs:

```json
{
  "date": "2026-01-18",
  "weight": 71.25
}
```

Headers:
- `X-API-Key: <your-api-key>`
- `Content-Type: application/json`
