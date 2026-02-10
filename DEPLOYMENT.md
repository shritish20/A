# 🚀 VolGuard v3.2 - Docker Deployment Guide

Complete guide to deploy VolGuard in a Docker container with all features enabled.

---

## 📋 Prerequisites

### Required:
- Docker Engine 20.10+ ([Install Docker](https://docs.docker.com/get-docker/))
- Docker Compose 2.0+ (included with Docker Desktop)
- Upstox API access token
- Minimum 2GB RAM, 2 CPU cores recommended

### Optional (for Telegram alerts):
- Telegram Bot Token (from [@BotFather](https://t.me/botfather))
- Your Telegram Chat ID (from [@userinfobot](https://t.me/userinfobot))

---

## 🗂️ Project Structure

Your deployment directory should look like this:

```
volguard-docker/
├── volguard_integrated_full.py  # Main application (rename to volguard.py)
├── Dockerfile                   # Container definition
├── docker-compose.yml           # Orchestration config
├── requirements.txt             # Python dependencies
├── .env                         # Your secrets (create from .env.example)
├── .env.example                 # Template
├── .dockerignore               # Build optimization
└── DEPLOYMENT.md               # This file
```

---

## ⚙️ Step-by-Step Deployment

### Step 1: Prepare Files

```bash
# Create deployment directory
mkdir volguard-docker
cd volguard-docker

# Copy all provided files into this directory:
# - volguard_integrated_full.py (rename to volguard.py)
# - Dockerfile
# - docker-compose.yml
# - requirements.txt
# - .env.example
# - .dockerignore

# Rename the main application file
mv volguard_integrated_full.py volguard.py
```

### Step 2: Configure Environment

```bash
# Copy environment template
cp .env.example .env

# Edit with your actual credentials
nano .env  # or use your preferred editor
```

**Critical configurations in `.env`:**

```bash
# REQUIRED - Get from Upstox
UPSTOX_ACCESS_TOKEN=your_actual_token_here

# REQUIRED - Set your capital
BASE_CAPITAL=1000000

# REQUIRED - Trading mode
ENABLE_AUTO_TRADING=false      # false for testing
ENABLE_MOCK_TRADING=true       # true for testing

# OPTIONAL - Telegram alerts
TELEGRAM_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id
```

⚠️ **IMPORTANT:** 
- Keep `ENABLE_AUTO_TRADING=false` until you've thoroughly tested
- Set `ENABLE_MOCK_TRADING=true` for safe testing
- Never commit `.env` to version control!

### Step 3: Build Container

```bash
# Build the Docker image
docker-compose build

# This will:
# - Create a Python 3.11 environment
# - Install all dependencies
# - Set up the application
# - Create a non-root user for security
```

Expected output:
```
[+] Building 45.2s (15/15) FINISHED
 => [internal] load build definition from Dockerfile
 => => transferring dockerfile: 1.23kB
 ...
 => => naming to docker.io/library/volguard-docker_volguard
```

### Step 4: Start the Container

```bash
# Start in detached mode
docker-compose up -d

# Check status
docker-compose ps
```

Expected output:
```
NAME                  STATUS              PORTS
volguard-trading      Up 10 seconds       0.0.0.0:8000->8000/tcp
```

### Step 5: Verify Deployment

```bash
# Check logs
docker-compose logs -f volguard

# You should see:
# ======================================================================
# VolGuard v3.2 - Production Refactor (Integrated)
# ======================================================================
# Base Capital:    ₹1,000,000.00
# Auto Trading:    DISABLED 🟡
# Execution Mode:  MockExecutor
# Telegram:        ACTIVE ✅  (if configured)
# P&L Attribution: ENABLED ✅
# ======================================================================
```

### Step 6: Test the System

```bash
# Test health endpoint
curl http://localhost:8000/api/health

# Expected response:
# {"status":"healthy","version":"3.2.0","timestamp":"..."}

# Test dashboard
curl http://localhost:8000/api/dashboard

# Test Telegram (if configured)
curl -X POST http://localhost:8000/api/test-alert
# Check your Telegram - you should receive a test message!

# Access API documentation
# Open in browser: http://localhost:8000/docs
```

---

## 🎛️ Container Management

### View Logs

```bash
# Follow logs in real-time
docker-compose logs -f

# View last 100 lines
docker-compose logs --tail=100

# View specific service
docker-compose logs volguard
```

### Stop Container

```bash
# Stop gracefully
docker-compose stop

# Stop and remove containers
docker-compose down
```

### Restart Container

```bash
# Restart after config changes
docker-compose restart

# Or rebuild and restart
docker-compose up -d --build
```

### Access Container Shell

```bash
# For debugging
docker-compose exec volguard bash

# Inside container:
volguard@container:/app$ ls -la
volguard@container:/app$ cat /app/data/volguard.db
volguard@container:/app$ exit
```

---

## 💾 Data Persistence

Data is persisted in Docker volumes:

```bash
# List volumes
docker volume ls | grep volguard

# Should show:
# volguard_database    # SQLite database
# volguard_logs        # Application logs

# Inspect volume
docker volume inspect volguard_database

# Backup database
docker run --rm -v volguard_database:/data -v $(pwd):/backup alpine \
  tar czf /backup/volguard_backup_$(date +%Y%m%d).tar.gz -C /data .

# Restore database
docker run --rm -v volguard_database:/data -v $(pwd):/backup alpine \
  tar xzf /backup/volguard_backup_YYYYMMDD.tar.gz -C /data
```

---

## 🔧 Configuration Management

### Update Environment Variables

```bash
# Edit .env file
nano .env

# Restart container to apply
docker-compose restart
```

### Update Application Code

```bash
# Edit volguard.py
nano volguard.py

# Rebuild and restart
docker-compose up -d --build
```

### Scale Resources

Edit `docker-compose.yml`:

```yaml
deploy:
  resources:
    limits:
      cpus: '4.0'      # Increase CPU
      memory: 4G       # Increase RAM
```

Then restart:
```bash
docker-compose up -d
```

---

## 🔍 Monitoring

### Health Checks

```bash
# Container health status
docker-compose ps

# Manual health check
curl http://localhost:8000/api/health

# Cache status
curl http://localhost:8000/api/system/cache-status
```

### Resource Usage

```bash
# Real-time stats
docker stats volguard-trading

# Shows:
# - CPU %
# - Memory usage/limit
# - Network I/O
# - Block I/O
```

### Application Metrics

```bash
# Dashboard
curl http://localhost:8000/api/dashboard | jq

# Active positions
curl http://localhost:8000/api/positions | jq

# Trade history
curl http://localhost:8000/api/trades/history | jq
```

---

## 🐛 Troubleshooting

### Container Won't Start

```bash
# Check logs
docker-compose logs

# Common issues:
# 1. Port 8000 already in use
#    Solution: Change PORT in .env or stop conflicting service

# 2. Missing UPSTOX_ACCESS_TOKEN
#    Solution: Set token in .env

# 3. Permission errors
#    Solution: Check file permissions on volumes
```

### Database Issues

```bash
# Reset database (DESTRUCTIVE!)
docker-compose down
docker volume rm volguard_database
docker-compose up -d

# Or delete from inside container
docker-compose exec volguard rm /app/data/volguard.db
docker-compose restart
```

### Memory Issues

```bash
# If container crashes due to OOM
# Increase memory limit in docker-compose.yml:
    limits:
      memory: 4G  # Increase from 2G
```

### Telegram Not Working

```bash
# Verify configuration
docker-compose exec volguard env | grep TELEGRAM

# Test endpoint
curl -X POST http://localhost:8000/api/test-alert

# Check logs for errors
docker-compose logs | grep -i telegram
```

---

## 🔒 Security Best Practices

### 1. Environment Variables

```bash
# Never commit .env to git
echo ".env" >> .gitignore

# Use strong, unique tokens
# Rotate tokens regularly
```

### 2. Network Security

```bash
# Only expose necessary ports
# Use firewall rules
sudo ufw allow 8000/tcp

# Or use nginx reverse proxy with SSL
```

### 3. Container Security

```bash
# Container runs as non-root user (volguard:1000)
# Verify:
docker-compose exec volguard whoami
# Output: volguard

# Keep images updated
docker-compose pull
docker-compose up -d --build
```

### 4. Data Security

```bash
# Regular backups
# Store backups securely
# Encrypt sensitive data at rest
```

---

## 🚀 Production Deployment

### Using Nginx Reverse Proxy

```nginx
# /etc/nginx/sites-available/volguard
server {
    listen 80;
    server_name volguard.yourdomain.com;

    location / {
        proxy_pass http://localhost:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

### SSL with Let's Encrypt

```bash
# Install certbot
sudo apt install certbot python3-certbot-nginx

# Get certificate
sudo certbot --nginx -d volguard.yourdomain.com
```

### Systemd Service (Alternative to docker-compose)

```bash
# /etc/systemd/system/volguard.service
[Unit]
Description=VolGuard Trading System
After=docker.service
Requires=docker.service

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=/opt/volguard
ExecStart=/usr/local/bin/docker-compose up -d
ExecStop=/usr/local/bin/docker-compose down
TimeoutStartSec=0

[Install]
WantedBy=multi-user.target
```

Enable:
```bash
sudo systemctl enable volguard
sudo systemctl start volguard
```

---

## 📊 Monitoring & Logging

### Log Aggregation

```bash
# Send logs to external service (e.g., Datadog, CloudWatch)
# Edit docker-compose.yml:
    logging:
      driver: "syslog"
      options:
        syslog-address: "tcp://logs.example.com:514"
```

### Prometheus Metrics (Advanced)

Add to docker-compose.yml:
```yaml
  prometheus:
    image: prom/prometheus
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
    ports:
      - "9090:9090"
```

---

## 🔄 Updates & Maintenance

### Update Application

```bash
# Pull latest code
git pull  # if using git

# Or copy new volguard.py

# Rebuild and deploy
docker-compose up -d --build
```

### Update Dependencies

```bash
# Edit requirements.txt
nano requirements.txt

# Rebuild
docker-compose build --no-cache
docker-compose up -d
```

### Cleanup

```bash
# Remove unused images
docker image prune -a

# Remove unused volumes
docker volume prune

# Full cleanup (CAREFUL!)
docker system prune -a
```

---

## 📞 Support & Resources

### Quick Commands Reference

```bash
# Start
docker-compose up -d

# Stop
docker-compose down

# Logs
docker-compose logs -f

# Restart
docker-compose restart

# Status
docker-compose ps

# Shell
docker-compose exec volguard bash

# Backup
docker run --rm -v volguard_database:/data -v $(pwd):/backup alpine tar czf /backup/backup.tar.gz -C /data .
```

### Environment Variables Quick Reference

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `UPSTOX_ACCESS_TOKEN` | ✅ Yes | - | Upstox API token |
| `TELEGRAM_TOKEN` | ❌ No | - | Telegram bot token |
| `TELEGRAM_CHAT_ID` | ❌ No | - | Telegram chat ID |
| `BASE_CAPITAL` | ❌ No | 1000000 | Trading capital (₹) |
| `ENABLE_AUTO_TRADING` | ❌ No | false | Enable real trading |
| `ENABLE_MOCK_TRADING` | ❌ No | true | Enable mock trading |
| `PORT` | ❌ No | 8000 | Server port |

---

## ✅ Deployment Checklist

Before going live:

- [ ] Upstox access token configured and tested
- [ ] Telegram alerts working (if enabled)
- [ ] Base capital set correctly
- [ ] Mock trading tested successfully
- [ ] Database backups configured
- [ ] Resource limits appropriate
- [ ] Monitoring in place
- [ ] Logs accessible
- [ ] Circuit breaker tested
- [ ] Emergency shutdown procedure documented

---

## 🎉 You're All Set!

Your VolGuard system is now running in a production-grade Docker container with:

✅ Isolated environment  
✅ Automatic restarts  
✅ Persistent data  
✅ Health monitoring  
✅ Resource limits  
✅ Non-root security  
✅ Easy updates  

**Happy Trading! 📈**

---

*For issues or questions, check the logs first: `docker-compose logs -f`*
