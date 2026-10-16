# Cloud Deployment Guide — Free Tier Infrastructure
MayaCardio Bedside Data Pipeline

This guide outlines deployment of the cloud receiving, storage, and online processing service on 100% free cloud tiers, strictly complying with the assessment's rule forbidding paid services.

---

## Free Cloud Provider Options

### Option 1: Render (Recommended — 100% Free Web Service)
Render provides free web services supporting Python ASGI applications without credit card requirements.

1. **Create Account**: Sign up at [render.com](https://render.com).
2. **New Web Service**:
   - Connect your GitHub repository containing this project.
   - **Environment**: Python 3.
   - **Build Command**: `pip install -r requirements.txt && pip install -e .`
   - **Start Command**: `uvicorn mayacardio.cloud_server:app --host 0.0.0.0 --port $PORT`
   - **Instance Type**: Free (512 MB RAM, 0.1 CPU).
3. **Environment Variables**:
   - `CLOUD_DB_PATH`: `mayacardio_cloud.db`
4. **Deploy**: Render assigns a public URL (e.g. `https://mayacardio-cloud-xyz.onrender.com`).
5. **Set on Device**:
   ```bash
   export CLOUD_URL="https://mayacardio-cloud-xyz.onrender.com"
   ```

---

### Option 2: Railway / Fly.io / Koyeb Free Tiers
Using the included `deploy/Dockerfile`:
```bash
fly launch --name mayacardio-cloud
fly deploy
```

---

### Option 3: AWS Free Tier (EC2 t4g.micro ARM64)
AWS offers 750 hours/month of `t4g.micro` (Graviton2 ARM64, 2 vCPUs, 1 GB RAM) free for 12 months:
1. Launch an Ubuntu 24.04 ARM64 `t4g.micro` instance.
2. Clone repository and run `./install.sh`.
3. Start cloud server:
   ```bash
   nohup uvicorn mayacardio.cloud_server:app --host 0.0.0.0 --port 8000 > cloud.log 2>&1 &
   ```
4. Configure security group to allow inbound TCP on port 8000.

---

### Option 4: Localhost Standalone Testing
For automated integration testing and offline grading:
```bash
python -m uvicorn mayacardio.cloud_server:app --host 127.0.0.1 --port 8000
```
Then run `./pipeline status` to verify `online` mode.
