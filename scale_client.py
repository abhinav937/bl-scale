import asyncio
from typing import Optional
from enum import Enum
from datetime import datetime

import aiohttp
from bleak import BleakClient, BleakScanner
from bleak.backends.device import BLEDevice
from bleak.exc import BleakError, BleakDBusError

# ────────────────────────────────────────────────
# Chipsea-BLE scale constants
# ────────────────────────────────────────────────

SERVICE_UUID = "0000fff0-0000-1000-8000-00805f9b34fb"
SCALE_NAME = "Chipsea-BLE"
SCALE_ADDRESS = "DE:E7:54:8A:87:0A"

INIT_COMMAND = bytearray([
    0xCA, 0xA0, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
])

# ────────────────────────────────────────────────
# Apple Health API configuration
# ────────────────────────────────────────────────
HEALTH_API_URL     = "https://ai-reply-bot.vercel.app/api/health-api"
ACTIVE_PROFILE_URL = "https://ai-reply-bot.vercel.app/api/active-profile"
HEALTH_API_KEY     = "bzEMsdAELNtAZo4OliH8POjhdOxDzhR_s1dOKSWO7K0"
DEFAULT_PROFILE    = "default"

# ────────────────────────────────────────────────
# Connection constants
# ────────────────────────────────────────────────
SCAN_TIMEOUT = 60.0      # How long to wait for scale to appear
CONNECT_TIMEOUT = 10.0   # Connection timeout
RETRY_DELAY = 2.0        # Delay only on failure (like iOS)
MAX_RETRIES = 3


class ConnectionState(Enum):
    IDLE = "idle"
    SCANNING = "scanning"
    CONNECTING = "connecting"
    CONNECTED = "connected"


async def clear_bluez_cache(address: str):
    """Clear stale BlueZ device state to avoid 'InProgress' errors."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "bluetoothctl", "remove", address,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL
        )
        await asyncio.wait_for(proc.wait(), timeout=5.0)
        await asyncio.sleep(0.3)
    except Exception:
        pass  # Non-fatal - device may not exist in cache


class ScaleClient:
    """
    Manages BLE connection to a Chipsea-based bathroom scale.

    Uses iOS-style immediate connection: continuous scanning with
    instant connection when the scale is detected.
    """

    def __init__(self, device_address: str = SCALE_ADDRESS):
        self.device_address = device_address
        self.state = ConnectionState.IDLE
        self.client: Optional[BleakClient] = None

        # Weight tracking
        self.current_weight_kg: Optional[float] = None
        self._stable_weight_uploaded = False

    async def scan_and_connect(self) -> Optional[BleakClient]:
        """
        Continuous scan that connects IMMEDIATELY when scale is detected.
        This is the key fix - no delay between detection and connection.
        """
        # Clear BlueZ cache to avoid stale state
        print("Clearing BlueZ cache...")
        await clear_bluez_cache(self.device_address)

        self.state = ConnectionState.SCANNING
        print(f"Scanning for scale... (step on scale now)")

        # Event and storage for detection callback
        found_event = asyncio.Event()
        found_device: list[Optional[BLEDevice]] = [None]

        def on_detected(device: BLEDevice, adv_data):
            # Match by address or name
            if device.address == self.device_address or \
               (device.name and "Chipsea" in device.name):
                if not found_event.is_set():
                    found_device[0] = device
                    found_event.set()

        # Start continuous scanning
        scanner = BleakScanner(detection_callback=on_detected)

        try:
            await scanner.start()

            # Wait for detection or timeout
            try:
                await asyncio.wait_for(found_event.wait(), timeout=SCAN_TIMEOUT)
            except asyncio.TimeoutError:
                print("Scan timeout - scale not detected")
                self.state = ConnectionState.IDLE
                return None
            finally:
                # CRITICAL: Stop scanner BEFORE connecting
                await scanner.stop()

            device = found_device[0]
            if not device:
                self.state = ConnectionState.IDLE
                return None

            print(f"Found: {device.name or 'Unnamed'} ({device.address})")

            # Connect IMMEDIATELY (no sleep!)
            return await self._connect_to_device(device)

        except Exception as e:
            print(f"Scan error: {e}")
            self.state = ConnectionState.IDLE
            return None

    async def _connect_to_device(self, device: BLEDevice) -> Optional[BleakClient]:
        """Connect to device with retry logic. Retry delay only on failure."""
        self.state = ConnectionState.CONNECTING

        for attempt in range(MAX_RETRIES):
            suffix = f" (attempt {attempt + 1})" if attempt > 0 else ""
            print(f"Connecting to {device.address}...{suffix}")

            client = None
            try:
                client = BleakClient(
                    device,
                    disconnected_callback=self._on_disconnect,
                    timeout=CONNECT_TIMEOUT
                )

                await client.connect()

                if client.is_connected:
                    self.client = client
                    self.state = ConnectionState.CONNECTED
                    print("Connected!")
                    return client

            except BleakDBusError as e:
                error_str = str(e)
                if "InProgress" in error_str:
                    print("BlueZ busy - clearing cache...")
                    await clear_bluez_cache(self.device_address)
                else:
                    print(f"DBus error: {e}")

            except (BleakError, TimeoutError, asyncio.TimeoutError) as e:
                print(f"Connection failed: {e}")

            except Exception as e:
                print(f"Error: {type(e).__name__}: {e}")

            # Cleanup failed client
            if client:
                try:
                    await client.disconnect()
                except Exception:
                    pass

            # Retry delay only on FAILURE (like iOS)
            if attempt < MAX_RETRIES - 1:
                print(f"Retrying in {RETRY_DELAY}s...")
                await asyncio.sleep(RETRY_DELAY)

        print("Failed to connect after retries")
        self.state = ConnectionState.IDLE
        return None

    def _on_disconnect(self, client: BleakClient):
        """Handle disconnection."""
        if self.state == ConnectionState.CONNECTED:
            print("Disconnected from scale")
        self.state = ConnectionState.IDLE
        self.client = None
        self._stable_weight_uploaded = False

    async def setup_and_monitor(self, client: BleakClient):
        """Setup notifications and monitor for weight data."""
        try:
            # Discover services
            services = client.services

            # Find FFF0 service
            target_service = None
            for service in services:
                if "fff0" in str(service.uuid).lower():
                    target_service = service
                    break

            if not target_service:
                print("FFF0 service not found")
                return

            # Find characteristics
            notify_char = None
            write_char = None

            for char in target_service.characteristics:
                uuid = str(char.uuid).lower()
                if "fff1" in uuid or "fff4" in uuid:
                    if "notify" in char.properties:
                        notify_char = char
                if "fff2" in uuid or "fff5" in uuid:
                    if "write" in char.properties or "write-without-response" in char.properties:
                        write_char = char

            if not notify_char or not write_char:
                print("Required characteristics not found")
                return

            # Enable notifications
            await client.start_notify(notify_char.uuid, self._on_notification)
            print("Notifications enabled")

            # Send init command
            await client.write_gatt_char(write_char.uuid, INIT_COMMAND, response=True)
            print("Ready - waiting for weight...")

            # Monitor until disconnected
            while client.is_connected:
                await asyncio.sleep(0.5)

        except Exception as e:
            print(f"Communication error: {e}")

    def _on_notification(self, sender, data: bytearray):
        """Handle weight data notifications."""
        weight, is_stable = self._parse_weight(data)

        if weight is not None:
            self.current_weight_kg = weight

            if is_stable:
                print(f"Stable weight: {weight:.2f} kg")

                # Upload once per measurement session
                if not self._stable_weight_uploaded:
                    self._stable_weight_uploaded = True
                    asyncio.create_task(self._upload_weight(weight))

    def _parse_weight(self, data: bytearray) -> tuple[Optional[float], bool]:
        """Parse weight from BLE packet. Returns (weight_kg, is_stable)."""
        if len(data) < 8:
            return None, False

        # Chipsea protocol: CA A0 [type] 02 [indicator] [high] [low] ...
        # type: F2=intermediate, F3=real-time, F4=stable
        # indicator: 04=measuring, 05=stable
        if (data[0] == 0xCA and data[1] == 0xA0 and
            data[2] in (0xF2, 0xF3, 0xF4) and
            data[3] == 0x02 and data[4] in (0x04, 0x05)):

            raw = (data[5] << 8) | data[6]
            kg = raw / 100.0

            is_stable = (data[2] == 0xF4 and data[4] == 0x05)

            if 0.0 < kg < 300.0:
                return kg, is_stable

        return None, False

    async def _get_active_profile(self, session: aiohttp.ClientSession) -> str:
        """Fetch who's next from the active-profile API. Falls back to default."""
        try:
            async with session.get(ACTIVE_PROFILE_URL) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    profile = data.get("profileId", DEFAULT_PROFILE)
                    print(f"Active profile: {profile}")
                    return profile
        except Exception as e:
            print(f"Could not fetch active profile (using default): {e}")
        return DEFAULT_PROFILE

    async def _reset_active_profile(self, session: aiohttp.ClientSession):
        """Reset active profile back to default after a successful upload."""
        try:
            headers = {
                "X-API-Key": HEALTH_API_KEY,
                "Content-Type": "application/json"
            }
            async with session.post(
                ACTIVE_PROFILE_URL,
                json={"profileId": DEFAULT_PROFILE},
                headers=headers
            ) as resp:
                if resp.status == 200:
                    print("Active profile reset to default")
        except Exception as e:
            print(f"Could not reset active profile: {e}")

    async def _upload_weight(self, weight_kg: float):
        """Upload weight to Apple Health API under the active profile."""
        payload = {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "weight": round(weight_kg, 2)
        }
        headers = {
            "X-API-Key": HEALTH_API_KEY,
            "Content-Type": "application/json"
        }

        try:
            async with aiohttp.ClientSession() as session:
                # 1. Who's stepping on?
                profile_id = await self._get_active_profile(session)

                # 2. Upload to that profile
                url = f"{HEALTH_API_URL}?profileId={profile_id}"
                async with session.post(url, json=payload, headers=headers) as resp:
                    if resp.status == 200:
                        print(f"Uploaded: {weight_kg:.2f} kg → profile '{profile_id}'")
                        # 3. Reset so next person isn't misrouted
                        await self._reset_active_profile(session)
                    else:
                        text = await resp.text()
                        print(f"Upload failed ({resp.status}): {text}")
        except Exception as e:
            print(f"Upload error: {e}")


# ────────────────────────────────────────────────
# Main entry point
# ────────────────────────────────────────────────

async def main():
    scale = ScaleClient(device_address=SCALE_ADDRESS)

    print("="*50)
    print("BLE Scale Monitor")
    print("="*50)
    print(f"Target: {SCALE_ADDRESS}")
    print("Step on the scale to take a measurement")
    print()

    try:
        while True:
            if scale.state == ConnectionState.IDLE:
                client = await scale.scan_and_connect()

                if client:
                    await scale.setup_and_monitor(client)
                else:
                    # Only wait on failure
                    await asyncio.sleep(RETRY_DELAY)
            else:
                await asyncio.sleep(0.5)

    except KeyboardInterrupt:
        print("\nStopped by user")
    finally:
        if scale.client and scale.client.is_connected:
            await scale.client.disconnect()
        print("Cleanup done.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Terminated.")
