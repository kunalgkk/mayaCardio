# 5-Minute Demonstration Walkthrough Script
## MayaCardio Bedside Data Pipeline — "Never Lose a Heartbeat"

**Candidate:** M SATHWIK (23BCE11144), VIT Bhopal  
**Target Board:** Raspberry Pi 4 (ARM64) & Free Cloud Service  
**Total Video Duration:** $\le 5$ Minutes  

---

### Walkthrough Timeline & Exact Execution Steps

#### Minute 0:00 – 1:00 | Normal Online Ingestion & Pipeline Status
1. **Launch Components:**
   - Terminal 1 (Mock): `python scripts/biosignal_mock.py --port 7100 --mode ramp`
   - Terminal 2 (Cloud): `uvicorn mayacardio.cloud_server:app --port 8000`
   - Terminal 3 (Pipeline): `python -m mayacardio.cli run`
2. **Inspect Status:**
   - In Terminal 4, execute:
     ```bash
     ./pipeline status
     ```
   - **Observed Output:**
     ```json
     {"mode": "online", "samples_saved": 800, "samples_in_cloud": 800, "backlog": 0}
     ```
   - *Verification:* Mode is `online`, samples stream live to cloud, backlog is 0.

---

#### Minute 1:00 – 2:15 | Network Drop & Automatic Switch to Offline Mode
1. **Simulate Network Failure:**
   - Terminate the cloud server process (Ctrl+C in Terminal 2).
2. **Observe Automatic Transition:**
   - Run `pipeline status` repeatedly.
   - Within $\le 3.0$ seconds (well under the 10-second limit), output switches to:
     ```json
     {"mode": "offline", "samples_saved": 2400, "samples_in_cloud": 800, "backlog": 1600}
     ```
3. **Verify Local Edge Processing:**
   - Execute:
     ```bash
     ./pipeline results --session <session_id>
     ```
   - Shows window metrics computed locally on the device with `"mode": "offline"`.

---

#### Minute 2:15 – 3:30 | Network Restoration & Automatic Catch-up Reconciliation
1. **Restore Cloud Connectivity:**
   - Re-launch the cloud server in Terminal 2.
2. **Observe Automatic Reconnection:**
   - Within 1.0 second, pipeline pings `/health` and switches to `"mode": "online"`.
3. **Catch-up Verification:**
   - Run `pipeline status`: Backlog drops rapidly to 0 as outbox batches stream to cloud.
   - Run export parity check:
     ```bash
     ./pipeline export --session <session_id> --from device > device.jsonl
     ./pipeline export --session <session_id> --from cloud > cloud.jsonl
     diff device.jsonl cloud.jsonl
     ```
   - *Result:* Zero difference. Parity is 100% identical.

---

#### Minute 3:30 – 4:30 | Process Kill (SIGKILL) & WAL Crash Recovery
1. **Simulate Abrupt Crash:**
   - In Terminal 3, send `SIGKILL` (`kill -9 <pid>`) during active acquisition.
2. **Inspect Durable State:**
   - Launch pipeline again:
     ```bash
     ./pipeline status
     ```
   - All committed samples remain intact; SQLite WAL auto-recovers without corruption.
3. **State Durability Guarantees:**
   - Process Crash / Reboot: **0 samples lost**.
   - Sudden Power Interruption: **Maximum bounded loss $\le 1.0$ second** ($\le 400$ samples/sensor in the uncommitted in-flight batch).

---

#### Minute 4:30 – 5:00 | Automated Test Evidence & Ground Truth Verification
1. **Execute Ground Truth Checker:**
   ```bash
   python scripts/check_ground_truth.py device.jsonl --mode ramp
   ```
   - Output: `{"status": "PASS", "duplicates": 0, "corrupted_values": 0, "silent_losses": 0}`.
2. **Execute Automated Test Suite:**
   ```bash
   python -m pytest tests/
   ```
   - Output: `30 passed in 9.28s`.
3. **Run Failure Suite:**
   ```bash
   python scripts/run_failure_suite.py
   ```
   - Output: `ALL 7 FAILURE SCENARIOS VERIFIED: PASS`.
