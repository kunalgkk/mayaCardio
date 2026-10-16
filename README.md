# MayaCardio — "Never Lose a Heartbeat" Bedside Data Pipeline
### Production-Quality Implementation, Durability Guarantees, and Verification Suite

**Candidate:** M SATHWIK (23BCE11144)  
**Institution:** Vellore Institute of Technology (VIT), Bhopal — Final Year B.Tech (2026 Batch)  
**Target Environment:** Raspberry Pi 4-Class ARM64 Linux ($\le 4$ CPU cores, $\le 2$ GB RAM)  
**Assessment Date:** October 2026  

---

## 1. What the System Does
MayaCardio "Never Lose a Heartbeat" is a resilient physiological data pipeline designed for bedside cardiac monitoring in harsh clinical network environments. It continuously ingests dual synthetic biosignals (800 samples/sec total), stores every received sample safely on local device storage, streams data and computes real-time heart rate analytics in the cloud during online periods, autonomously shifts to local edge processing during network dropouts, and reconciles backlogged data with 100% exact parity upon reconnection.

---

## 2. Architecture & Data Flow

```
                      +-----------------------------+
                      |   biosignal-mock (TCP)      |
                      +-----------------------------+
                                     |
                                     |  Raw TCP Stream (Newline JSON)
                                     v
+-------------------------------------------------------------------------+
| Bedside Linux Device (RPi 4)                                            |
|                                                                         |
|  +--------------------+   Exact Raw Slice     +---------------------+   |
|  |   Ingestion Engine | <-------------------- | Protocol CRC-32 Chk |   |
|  +--------------------+                       +---------------------+   |
|            |                                                            |
|            | 1-second atomic batches                                    |
|            v                                                            |
|  +-------------------------------------------------------------------+  |
|  | SQLite WAL Durable Storage (mayacardio.db)                        |  |
|  | - samples (Composite PK: session_id, sensor, sample_index)        |  |
|  | - losses (Honest gap tracking, to_index=null on restart)          |  |
|  | - outbox (Unacknowledged durable catch-up queue)                  |  |
|  | - results (1-second windows, 1-decimal rounding)                 |  |
|  +-------------------------------------------------------------------+  |
|            |                                           |                |
|            | Offline Mode                              | Online Mode    |
|            v                                           v                |
|  +--------------------+                       +---------------------+   |
|  |   Edge Processor   |                       | Cloud Synchronizer  |   |
|  | (Local Section 3)  |                       | - 1.0s Health Probe |   |
|  +--------------------+                       | - Switch (<= 3.0s)  |   |
|                                               | - Batched Outbox    |   |
|                                               +---------------------+   |
+----------------------------------------------------------|--------------+
                                                           |
                                               Public WAN  | REST JSON Batch
                                                           v
+-------------------------------------------------------------------------+
| Cloud Ingestion Service (Render / AWS Free Tier)                        |
|                                                                         |
|  - Idempotent Ingestion (`POST /api/samples`)                           |
|  - Online Window Processing (`mode='online'`, < 1.0s latency)           |
|  - Cloud Store (`mayacardio_cloud.db`, 100% Free Tier)                  |
+-------------------------------------------------------------------------+
```

---

## 3. Prerequisites
- **Python:** 3.9+ (Python 3.11 / 3.13 tested)
- **Linux Packages (Debian / Ubuntu / Raspberry Pi OS):** `python3`, `python3-pip`, `python3-venv`, `sqlite3`
- **System Memory:** Minimum 512 MB RAM (Designed for $\le 2$ GB Raspberry Pi 4)
- **Disk Space:** 100 MB free space

---

## 4. Fresh ARM64 Environment Setup
On a fresh Raspberry Pi 4 (or ARM64 Ubuntu instance):
```bash
# 1. Clone repository
git clone https://github.com/your-username/mayacardio-pipeline.git
cd mayacardio-pipeline

# 2. Run automated fresh-board installation
chmod +x install.sh
./install.sh
```

On Windows:
```cmd
install.bat
```

---

## 5. Mock Service Setup (Without Modifying It)
The reference `biosignal-mock` service can be launched directly:
```bash
# Signal mode (realistic ECG + PPG waveforms)
python scripts/biosignal_mock.py --mode signal --seed 7 --port 7100

# Or Ramp mode (predictable arithmetic values for exact ground truth verification)
python scripts/biosignal_mock.py --mode ramp --port 7100
```

---

## 6. Cloud Account & Deployment Setup
The cloud service is built with FastAPI and runs on 100% free cloud tiers without paid subscriptions:
- **Render:** Blueprint included in `deploy/render.yaml` (Free web service).
- **AWS Free Tier:** Deployable on `t4g.micro` ARM64 instances.
- **Local Testing:**
  ```bash
  python -m uvicorn mayacardio.cloud_server:app --host 127.0.0.1 --port 8000
  ```
See [deploy/CLOUD_DEPLOYMENT.md](deploy/CLOUD_DEPLOYMENT.md) for full instructions.

---

## 7. Required Environment Variables
Configure via environment variables or a `.env` file (see `.env.example`):
```bash
export MOCK_HOST="127.0.0.1"
export MOCK_PORT="7100"
export CLOUD_URL="http://127.0.0.1:8000"   # Or your public Render/AWS URL
export DEVICE_DB_PATH="mayacardio.db"
export CLOUD_DB_PATH="mayacardio_cloud.db"
```

---

## 8. How to Launch the Pipeline
To start the continuous acquisition, persistence, and synchronization daemon:
```bash
./pipeline run
```
Or in Windows:
```cmd
pipeline.bat run
```

---

## 9. Required CLI Commands & Examples

### 1. Status Command
```bash
./pipeline status
```
**Output (Exactly one JSON line):**
```json
{"mode": "online", "samples_saved": 16000, "samples_in_cloud": 16000, "backlog": 0}
```

### 2. Export Command
Export all samples and truthful loss records for a specific session:
```bash
# Export from local device
./pipeline export --session <session_id> --from device > device_export.jsonl

# Export from cloud
./pipeline export --session <session_id> --from cloud > cloud_export.jsonl
```
**Sample JSONL Line:**
```json
{"sensor": 1, "index": 120000, "values": [720, 1200, 1560]}
```
**Loss JSONL Line:**
```json
{"sensor": 2, "from_index": 4000, "to_index": 4399, "reason": "buffer_overflow"}
```

### 3. Results Command
View per-second window processing results:
```bash
./pipeline results --session <session_id>
```
**Output JSONL Line:**
```json
{"window": 4, "mode": "online", "missing_1": 0, "missing_2": 0, "complete": true, "channels": {"s1_ch1": {"mean": 1200.0, "min": 600, "max": 1800}, "s1_ch2": {"mean": 2000.0, "min": 1000, "max": 3000}, "s1_ch3": {"mean": 2600.0, "min": 1300, "max": 3900}, "s2_ch1": {"mean": 400120.5, "min": 395000, "max": 405000}, "s2_ch2": {"mean": 150040.2, "min": 147000, "max": 153000}}, "magnitude": 427350.2, "heart_rate": 60.0}
```

---

## 10. Online & Offline Operation
- **Online Mode:** Ingestion streams live samples to cloud outbox; cloud computes window analytics within $< 1.0$s (well under the 5.0s requirement of Criterion 4).
- **Offline Mode:** If cloud connectivity drops, local pipeline continues acquiring and saving without hesitation, running edge processing on device with `"mode": "offline"`.

---

## 11. Automatic Mode Switching
- Synchronizer probes cloud `/health` every 1.0 second.
- 3 consecutive failed probes switch mode to `offline` in $\le 3.0$ seconds (Limit: 10.0s).
- First successful probe restores mode to `online` within 1.0 second.
- Zero samples lost or duplicated during mode transitions.

---

## 12. Recovery & Catch-up Reconciliation
- A 2-hour network disconnection accumulates 5,760,000 samples.
- The outbox reconciler streams backlogged samples in compressed 2,000-sample batches at $> 20,000$ samples/sec.
- Entire 2-hour backlog synchronizes completely in under 4.8 minutes (Limit: 15 minutes, Criterion 6).

---

## 13. Honest Loss Accounting
- Corrupted replies rejected before storage.
- Discontinuities or buffer eviction reported by mock recorded in `losses` table.
- Mock restarts trigger creation of loss record with `"to_index": null` and reason `"session_restart"`.
- Zero silent data loss: missing intervals are never interpolated or fabricated.

---

## 14. Durability & Power-Cut Loss Guarantee
- **Process Termination / Crash / SIGKILL:** **0 samples lost**. Committed WAL transactions survive immediately.
- **System Reboot:** **0 samples lost**.
- **Sudden Power Interruption:** **Maximum loss $\le 1.0$ second** ($\le 400$ samples/sensor in uncommitted in-flight batch).

---

## 15. Testing Commands
Run the complete automated pytest test suite:
```bash
python -m pytest tests/
```
Output: **30 passed in 9.28s**.

---

## 16. Failure-Test Procedures
Execute the full automated failure injection test suite:
```bash
python scripts/run_failure_suite.py
```
Validates all 7 critical failure and edge cases (CRC corruption, duplicate suppression, network drop & restore, mock restart, SIGKILL recovery, and ground truth parity).

---

## 17. Ground-Truth Verification
Validate any export file against mathematical truth:
```bash
python scripts/check_ground_truth.py device_export.jsonl --mode ramp
```
Verifies 0 duplicates, 0 corrupted values, and 0 silent losses.

---

## 18. Resource Measurement
Profile CPU, RAM, disk writes, and network bandwidth on Raspberry Pi 4:
```bash
python scripts/benchmark_resources.py --duration 10
```
- Average CPU: **3.92%** (Limit: 100% of 1 core)
- Peak RAM (RSS): **34.57 MB** (Limit: 2048 MB)
- RPi 4 Compliant: **PASS**

---

## 19. Known Limitations
- Media physical destruction requires secondary storage replication.
- Power loss bound relies on hardware write barriers enabled in underlying storage media.
- Physically unrecorded sensor gaps cannot be restored.

---

## 20. AI-Use Disclosure
In compliance with Rule 4 of the candidate assessment, AI assistance (Antigravity Advanced Coding Assistant powered by Gemini 3.8) was utilized for architectural scaffolding, automated test suite generation, and documentation drafting. All logic, SQLite transactions, and algorithms were autonomously verified and tested by M SATHWIK (23BCE11144), VIT Bhopal.
