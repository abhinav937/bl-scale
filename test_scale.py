#!/usr/bin/env python3
"""
test_scale.py — Mock test for BLE scale upload pipeline.

Tests the full flow WITHOUT needing the physical scale:
  1.  BLE packet parsing (offline)
  2.  Active-profile API (GET + POST)
  3.  Weight upload
  4.  Active-profile reset
  5.  Verify data in GET response
  6.  List all profiles
  7.  HTTP timeout behaviour (new)
  8.  Upload task tracking / no fire-and-forget loss (new)
  9.  Bluetooth adapter detection (new)
  10. Bad API key rejected (new)
  11. Invalid profile ID rejected (new)

Usage:
    ./venv/bin/python test_scale.py
    ./venv/bin/python test_scale.py --weight 72.5
    ./venv/bin/python test_scale.py --profile abhinav --weight 72.5
    ./venv/bin/python test_scale.py --dry-run   # print requests, don't send
"""

import asyncio
import argparse
import json
import subprocess
import sys
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp

# ────────────────────────────────────────────────
# Config (mirrors scale_client.py)
# ────────────────────────────────────────────────
HEALTH_API_URL     = "https://ai-reply-bot.vercel.app/api/health-api"
ACTIVE_PROFILE_URL = "https://ai-reply-bot.vercel.app/api/active-profile"
HEALTH_API_KEY     = "bzEMsdAELNtAZo4OliH8POjhdOxDzhR_s1dOKSWO7K0"
DEFAULT_PROFILE    = "default"

HEADERS = {
    "X-API-Key": HEALTH_API_KEY,
    "Content-Type": "application/json",
}


def section(title: str):
    print(f"\n{'─'*50}")
    print(f"  {title}")
    print(f"{'─'*50}")

def ok(msg: str):    print(f"  ✓  {msg}")
def fail(msg: str):  print(f"  ✗  {msg}")
def info(msg: str):  print(f"  ·  {msg}")


# ════════════════════════════════════════════════
# OFFLINE TESTS (no network)
# ════════════════════════════════════════════════

# ────────────────────────────────────────────────
# Test 1: BLE packet parsing
# ────────────────────────────────────────────────

def test_packet_parsing():
    """Verify the weight-parsing logic against known packets."""
    section("1 · BLE packet parsing (offline)")

    from scale_client import ScaleClient
    client = ScaleClient.__new__(ScaleClient)

    cases = [
        # (description, bytes,                                                  expected_kg, expected_stable)
        ("Stable 72.58 kg",  bytearray([0xCA, 0xA0, 0xF4, 0x02, 0x05, 0x1C, 0x5A, 0x00]), 72.58, True),
        ("Live   68.02 kg",  bytearray([0xCA, 0xA0, 0xF3, 0x02, 0x04, 0x1A, 0x92, 0x00]), 68.02, False),
        ("Inter  70.00 kg",  bytearray([0xCA, 0xA0, 0xF2, 0x02, 0x04, 0x1B, 0x58, 0x00]), 70.00, False),
        ("Bad magic byte",   bytearray([0xAA, 0xA0, 0xF4, 0x02, 0x05, 0x1C, 0x5A, 0x00]), None,  False),
        ("Bad header[1]",    bytearray([0xCA, 0xBB, 0xF4, 0x02, 0x05, 0x1C, 0x5A, 0x00]), None,  False),
        ("Bad type byte",    bytearray([0xCA, 0xA0, 0xFF, 0x02, 0x05, 0x1C, 0x5A, 0x00]), None,  False),
        ("Bad indicator",    bytearray([0xCA, 0xA0, 0xF4, 0x02, 0x99, 0x1C, 0x5A, 0x00]), None,  False),
        ("Too short",        bytearray([0xCA, 0xA0, 0xF4, 0x02]),                           None,  False),
        ("Zero weight",      bytearray([0xCA, 0xA0, 0xF4, 0x02, 0x05, 0x00, 0x00, 0x00]), None,  False),
        ("Over 300 kg",      bytearray([0xCA, 0xA0, 0xF4, 0x02, 0x05, 0x75, 0x30, 0x00]), None,  False),
    ]

    all_pass = True
    for desc, pkt, exp_kg, exp_stable in cases:
        kg, stable = client._parse_weight(pkt)
        if exp_kg is None:
            passed = kg is None
        else:
            passed = (kg is not None
                      and abs(kg - exp_kg) < 0.01
                      and stable == exp_stable)

        if passed:
            ok(f"{desc} → {kg} kg, stable={stable}")
        else:
            fail(f"{desc} → got ({kg}, {stable}), expected ({exp_kg}, {exp_stable})")
            all_pass = False

    return all_pass


# ────────────────────────────────────────────────
# Test 7: HTTP timeout is set on the session
# ────────────────────────────────────────────────

def test_http_timeout_configured():
    """
    Confirm HTTP_TIMEOUT is defined and is an aiohttp.ClientTimeout
    with a sensible total value (≤30 s).
    """
    section("7 · HTTP timeout configured (offline)")

    from scale_client import HTTP_TIMEOUT
    if not isinstance(HTTP_TIMEOUT, aiohttp.ClientTimeout):
        fail(f"HTTP_TIMEOUT is {type(HTTP_TIMEOUT)}, expected aiohttp.ClientTimeout")
        return False
    if HTTP_TIMEOUT.total is None or HTTP_TIMEOUT.total > 30:
        fail(f"HTTP_TIMEOUT.total = {HTTP_TIMEOUT.total} — should be ≤30 s")
        return False
    ok(f"HTTP_TIMEOUT.total = {HTTP_TIMEOUT.total} s")
    return True


# ────────────────────────────────────────────────
# Test 8: Upload task is tracked (not fire-and-forget)
# ────────────────────────────────────────────────

def test_upload_task_tracked():
    """
    Verify that _on_notification stores the task in self._upload_task
    so setup_and_monitor can await it on disconnect.
    """
    section("8 · Upload task tracked (offline)")

    from scale_client import ScaleClient

    # Build a minimal client instance without BLE init
    client = ScaleClient.__new__(ScaleClient)
    client._stable_weight_uploaded = False
    client._upload_task = None
    client.current_weight_kg = None

    uploaded_weights = []

    async def fake_upload(weight_kg):
        uploaded_weights.append(weight_kg)

    # Stable packet: CA A0 F4 02 05 1C 5A 00 → 72.58 kg
    stable_pkt = bytearray([0xCA, 0xA0, 0xF4, 0x02, 0x05, 0x1C, 0x5A, 0x00])

    async def run():
        loop = asyncio.get_event_loop()
        with patch.object(client, '_upload_weight', side_effect=fake_upload):
            client._on_notification(None, stable_pkt)
            # Task should now be set
            if client._upload_task is None:
                return False, "._upload_task is still None after notification"
            await client._upload_task
            if not uploaded_weights:
                return False, "_upload_weight was never called"
            return True, f"task set and called with {uploaded_weights[0]} kg"

    passed, msg = asyncio.run(run())
    if passed:
        ok(msg)
    else:
        fail(msg)
    return passed


# ────────────────────────────────────────────────
# Test 9: Bluetooth adapter detection
# ────────────────────────────────────────────────

def test_bt_adapter_detection():
    """
    wait_for_bt_adapter() should return True when hci0 is present,
    False when it's missing (using a short timeout).
    """
    section("9 · Bluetooth adapter detection (offline)")

    from scale_client import wait_for_bt_adapter

    # Check whether a real adapter exists first
    try:
        result = subprocess.run(["hciconfig"], capture_output=True, timeout=3)
        real_adapter = b"hci0" in result.stdout
    except Exception:
        real_adapter = False

    async def run():
        # Case A: mock hciconfig to report hci0 present
        mock_proc = AsyncMock()
        mock_proc.communicate = AsyncMock(return_value=(b"hci0:   Type: Primary", b""))
        with patch("asyncio.create_subprocess_exec", return_value=mock_proc):
            found = await wait_for_bt_adapter(timeout=5.0)
            if not found:
                return False, "Expected True when hci0 present, got False"

        # Case B: mock hciconfig to report no adapter (very short timeout)
        mock_proc2 = AsyncMock()
        mock_proc2.communicate = AsyncMock(return_value=(b"", b""))
        with patch("asyncio.create_subprocess_exec", return_value=mock_proc2):
            found = await wait_for_bt_adapter(timeout=0.1)
            if found:
                return False, "Expected False when no adapter, got True"

        return True, f"adapter present/absent detection correct (real hw: {real_adapter})"

    passed, msg = asyncio.run(run())
    if passed:
        ok(msg)
    else:
        fail(msg)
    return passed


# ════════════════════════════════════════════════
# NETWORK TESTS
# ════════════════════════════════════════════════

# ────────────────────────────────────────────────
# Test 2: Active-profile API (GET + POST)
# ────────────────────────────────────────────────

async def test_active_profile_api(dry_run: bool, force_profile: str | None):
    section("2 · Active-profile API")

    if dry_run:
        info("DRY-RUN: would GET " + ACTIVE_PROFILE_URL)
        info("Assuming active profile = 'default'")
        return DEFAULT_PROFILE

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(ACTIVE_PROFILE_URL) as resp:
                body = await resp.json()
                if resp.status == 200:
                    profile = body.get("profileId", DEFAULT_PROFILE)
                    ok(f"GET  → status {resp.status}, profileId = '{profile}'")
                else:
                    fail(f"GET  → status {resp.status}: {body}")
                    return DEFAULT_PROFILE
    except Exception as e:
        fail(f"GET  → exception: {e}")
        return DEFAULT_PROFILE

    if force_profile and force_profile != profile:
        info(f"Overriding active profile to '{force_profile}' for this test...")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    ACTIVE_PROFILE_URL,
                    json={"profileId": force_profile},
                    headers=HEADERS,
                ) as resp:
                    if resp.status == 200:
                        ok(f"POST → set active profile to '{force_profile}'")
                        profile = force_profile
                    else:
                        text = await resp.text()
                        fail(f"POST → status {resp.status}: {text}")
        except Exception as e:
            fail(f"POST → exception: {e}")

    return profile


# ────────────────────────────────────────────────
# Test 3: Weight upload
# ────────────────────────────────────────────────

async def test_weight_upload(weight_kg: float, profile_id: str, dry_run: bool):
    section(f"3 · Weight upload  ({weight_kg} kg → profile '{profile_id}')")

    payload = {
        "date":   datetime.now().strftime("%Y-%m-%d"),
        "weight": round(weight_kg, 2),
    }
    url = f"{HEALTH_API_URL}?profileId={profile_id}"
    info(f"POST {url}")
    info(f"Body: {json.dumps(payload)}")

    if dry_run:
        info("DRY-RUN: skipping actual POST")
        return True

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=HEADERS) as resp:
                text = await resp.text()
                if resp.status == 200:
                    ok(f"Upload succeeded → {resp.status}: {text}")
                    return True
                else:
                    fail(f"Upload failed   → {resp.status}: {text}")
                    return False
    except Exception as e:
        fail(f"Upload exception: {e}")
        return False


# ────────────────────────────────────────────────
# Test 4: Active-profile reset
# ────────────────────────────────────────────────

async def test_reset_active_profile(dry_run: bool):
    section("4 · Reset active-profile → 'default'")

    if dry_run:
        info(f"DRY-RUN: would POST {ACTIVE_PROFILE_URL} → {{profileId: 'default'}}")
        return True

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                ACTIVE_PROFILE_URL,
                json={"profileId": DEFAULT_PROFILE},
                headers=HEADERS,
            ) as resp:
                if resp.status == 200:
                    ok(f"Reset succeeded  → profileId now = '{DEFAULT_PROFILE}'")
                    return True
                else:
                    text = await resp.text()
                    fail(f"Reset failed     → {resp.status}: {text}")
                    return False
    except Exception as e:
        fail(f"Reset exception: {e}")
        return False


# ────────────────────────────────────────────────
# Test 5: Verify uploaded data appears in GET
# ────────────────────────────────────────────────

async def test_verify_upload(weight_kg: float, profile_id: str, dry_run: bool):
    section(f"5 · Verify data in GET /health-api?profileId={profile_id}")

    if dry_run:
        info("DRY-RUN: skipping verification")
        return True

    today = datetime.now().strftime("%Y-%m-%d")
    url = f"{HEALTH_API_URL}?profileId={profile_id}"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=HEADERS) as resp:
                if resp.status != 200:
                    fail(f"GET failed → {resp.status}: {await resp.text()}")
                    return False
                body = await resp.json()
    except Exception as e:
        fail(f"GET exception: {e}")
        return False

    # API returns { profileId, count, data: [...] }
    entries = body.get("data", body) if isinstance(body, dict) else body

    match = next((e for e in entries if e.get("date") == today), None)
    if match and abs(match.get("weight", 0) - weight_kg) < 0.1:
        ok(f"Found today's entry: date={match['date']}, weight={match['weight']} kg")
        return True
    elif match:
        fail(f"Today's entry has wrong weight: {match}")
        return False
    else:
        fail(f"No entry for {today} found. All entries: {[e.get('date') for e in entries]}")
        return False


# ────────────────────────────────────────────────
# Test 6: List all profiles
# ────────────────────────────────────────────────

async def test_list_profiles(dry_run: bool):
    section("6 · List all profiles")

    if dry_run:
        info("DRY-RUN: skipping profile list")
        return True

    url = f"{HEALTH_API_URL}?action=profiles"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=HEADERS) as resp:
                if resp.status == 200:
                    body = await resp.json()
                    profiles = body.get("profiles", body)
                    ok(f"Profiles found: {profiles}")
                    return True
                else:
                    fail(f"List profiles failed → {resp.status}: {await resp.text()}")
                    return False
    except Exception as e:
        fail(f"Exception: {e}")
        return False


# ────────────────────────────────────────────────
# Test 10: Bad API key is rejected (401)
# ────────────────────────────────────────────────

async def test_bad_api_key(dry_run: bool):
    section("10 · Bad API key rejected by health-api")

    if dry_run:
        info("DRY-RUN: skipping")
        return True

    bad_headers = {"X-API-Key": "BADKEY", "Content-Type": "application/json"}
    payload = {"date": datetime.now().strftime("%Y-%m-%d"), "weight": 1.0}
    url = f"{HEALTH_API_URL}?profileId={DEFAULT_PROFILE}"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=bad_headers) as resp:
                if resp.status == 401:
                    ok(f"Bad key correctly rejected → 401 Unauthorized")
                    return True
                else:
                    fail(f"Expected 401, got {resp.status}: {await resp.text()}")
                    return False
    except Exception as e:
        fail(f"Exception: {e}")
        return False


# ────────────────────────────────────────────────
# Test 11: Invalid profile ID is rejected (400)
# ────────────────────────────────────────────────

async def test_invalid_profile_id(dry_run: bool):
    section("11 · Invalid profile ID rejected by health-api")

    if dry_run:
        info("DRY-RUN: skipping")
        return True

    bad_profiles = ["has space", "has/slash", "has@at", "a" * 60]
    all_pass = True

    try:
        async with aiohttp.ClientSession() as session:
            for bad_id in bad_profiles:
                url = f"{HEALTH_API_URL}?profileId={bad_id}"
                async with session.get(url, headers=HEADERS) as resp:
                    if resp.status in (400, 404):
                        ok(f"'{bad_id[:20]}' → {resp.status} (correctly rejected)")
                    else:
                        fail(f"'{bad_id[:20]}' → expected 400/404, got {resp.status}")
                        all_pass = False
    except Exception as e:
        fail(f"Exception: {e}")
        return False

    return all_pass


# ════════════════════════════════════════════════
# Runner
# ════════════════════════════════════════════════

async def run_all(weight_kg: float, force_profile: str | None, dry_run: bool):
    print()
    print("╔══════════════════════════════════════════════╗")
    print("║       BLE Scale — End-to-End Mock Test       ║")
    print("╚══════════════════════════════════════════════╝")
    if dry_run:
        print("  *** DRY-RUN MODE — no requests will be sent ***")

    results = {}

    # ── Offline tests (no network) ───────────────
    results["1_packet_parsing"]      = test_packet_parsing()
    results["7_http_timeout"]        = test_http_timeout_configured()
    results["8_upload_task_tracked"] = test_upload_task_tracked()
    results["9_bt_adapter_detect"]   = test_bt_adapter_detection()

    # ── Network tests ────────────────────────────
    profile_id = await test_active_profile_api(dry_run, force_profile)

    uploaded = await test_weight_upload(weight_kg, profile_id, dry_run)
    results["3_upload"] = uploaded

    if uploaded:
        results["4_reset"] = await test_reset_active_profile(dry_run)
    else:
        info("Skipping reset (upload failed)")
        results["4_reset"] = False

    results["5_verify"]              = await test_verify_upload(weight_kg, profile_id, dry_run)
    results["6_list_profiles"]       = await test_list_profiles(dry_run)
    results["10_bad_api_key"]        = await test_bad_api_key(dry_run)
    results["11_invalid_profile_id"] = await test_invalid_profile_id(dry_run)

    # ── Summary ──────────────────────────────────
    section("Summary")
    all_pass = True
    for name, passed in sorted(results.items()):
        if passed:
            ok(name.split("_", 1)[1])
        else:
            fail(name.split("_", 1)[1])
            all_pass = False

    print()
    if all_pass:
        print("  🎉  All tests passed — pipeline is working!\n")
    else:
        print("  ⚠️   Some tests failed — check output above.\n")

    return all_pass


def main():
    parser = argparse.ArgumentParser(description="Mock test for BLE scale upload pipeline")
    parser.add_argument("--weight",  type=float, default=72.5,
                        help="Mock weight in kg to upload (default: 72.5)")
    parser.add_argument("--profile", type=str,   default=None,
                        help="Force a specific profile instead of reading active-profile API")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would be sent without making any API calls")
    args = parser.parse_args()

    success = asyncio.run(run_all(args.weight, args.profile, args.dry_run))
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
