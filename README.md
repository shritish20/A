# 🚀 VolGuard v3.2 - Docker Deployment Package

**Professional Options Trading System with P&L Attribution & Telegram Alerts**

This package contains everything you need to deploy VolGuard in a production-ready Docker container.

---

## 📦 Package Contents

```
volguard-docker/
├── volguard_integrated_full.py  ⭐ Main application
├── Dockerfile                   🐳 Container definition
├── docker-compose.yml           🎼 Orchestration
├── requirements.txt             📚 Python dependencies
├── .env.example                 🔐 Configuration template
├── .dockerignore               🚫 Build optimization
├── quickstart.sh               ⚡ Quick deployment script
├── DEPLOYMENT.md               📖 Complete guide
└── README.md                   📝 This file
```

---

## ⚡ Quick Start (30 seconds)

### Option 1: Interactive Setup (Recommended)

```bash
# Make script executable
chmod +x quickstart.sh

# Run quick start
./quickstart.sh

# Choose option 1 (Full Setup)
# Follow the prompts
```

### Option 2: Manual Setup

```bash
# 1. Rename main file
mv volguard_integrated_full.py volguard.py

# 2. Configure environment
cp .env.example .env
nano .env  # Add your UPSTOX_ACCESS_TOKEN

# 3. Build and start
docker-compose up -d

# 4. Verify
curl http://localhost:8000/api/health
```

---

## 🎯 What You Get

### ✅ Production-Ready Container
- **Isolated Environment:** Clean Python 3.11 runtime
- **Security:** Non-root user, minimal attack surface
- **Persistence:** Database and logs survive restarts
- **Auto-Restart:** Container restarts if it crashes
- **Health Checks:** Automatic monitoring
- **Resource Limits:** CPU and memory controls

### ✅ Complete Feature Set
- **Real-time Trading:** Upstox API integration
- **P&L Attribution:** Theta/Vega/Delta breakdown
- **Telegram Alerts:** Position updates & exit notifications
- **Risk Management:** Circuit breaker, stop losses
- **Analytics:** 15-minute intervals with smart caching
- **GTT Orders:** Server-side stop losses

### ✅ Easy Management
- One-command deployment
- Simple configuration via `.env`
- Comprehensive logging
- Database backups
- Zero-downtime updates

---

## 🔧 Prerequisites

- **Docker:** Version 20.10+ ([Install](https://docs.docker.com/get-docker/))
- **Docker Compose:** Version 2.0+ (included with Docker Desktop)
- **Upstox API Token:** Get from [Upstox API Console](https://api.upstox.com/)
- **System Resources:** 
  - 2GB RAM minimum
  - 2 CPU cores recommended
  - 5GB disk space

---

## 📋 Configuration

### Required Settings

Edit `.env` file:

```bash
# CRITICAL: Your Upstox API token
UPSTOX_ACCESS_TOKEN=your_token_here

# Your trading capital
BASE_CAPITAL=1000000

# Trading mode (KEEP FALSE UNTIL TESTED!)
ENABLE_AUTO_TRADING=false
ENABLE_MOCK_TRADING=true
```

### Optional Settings

```bash
# Telegram Alerts (highly recommended)
TELEGRAM_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id

# Server Configuration
PORT=8000
HOST=0.0.0.0

# Database
DATABASE_URL=sqlite:///./data/volguard.db
```

---

## 🚦 Deployment Steps

### Step 1: Get Your Files Ready

```bash
# Create directory
mkdir volguard-docker
cd volguard-docker

# Copy all package files here
# Rename main application
mv volguard_integrated_full.py volguard.py
```

### Step 2: Configure

```bash
# Create environment file
cp .env.example .env

# Edit with your credentials
nano .env
```

**Critical:** Set `UPSTOX_ACCESS_TOKEN` before proceeding!

### Step 3: Deploy

```bash
# Build container
docker-compose build

# Start services
docker-compose up -d

# Verify deployment
docker-compose ps
docker-compose logs --tail=50
```

### Step 4: Verify

```bash
# Health check
curl http://localhost:8000/api/health

# Expected: {"status":"healthy","version":"3.2.0",...}

# Test Telegram (if configured)
curl -X POST http://localhost:8000/api/test-alert

# Access API docs
# Open browser: http://localhost:8000/docs
```

---

## 📊 Accessing Your System

### Web Interface
- **API Documentation:** http://localhost:8000/docs
- **Interactive API:** http://localhost:8000/redoc

### API Endpoints
```bash
# Dashboard
curl http://localhost:8000/api/dashboard

# Active positions
curl http://localhost:8000/api/positions

# Trade history
curl http://localhost:8000/api/trades/history

# System status
curl http://localhost:8000/api/system/cache-status
```

---

## 🎛️ Daily Operations

### View Logs
```bash
# Real-time logs
docker-compose logs -f

# Last 100 lines
docker-compose logs --tail=100

# Specific time range
docker-compose logs --since 1h
```

### Stop/Start
```bash
# Stop system
docker-compose stop

# Start system
docker-compose start

# Restart (after config changes)
docker-compose restart
```

### Update Code
```bash
# Edit volguard.py
nano volguard.py

# Rebuild and deploy
docker-compose up -d --build
```

---

## 💾 Data Management

### Backups

```bash
# Backup database
docker run --rm \
  -v volguard_database:/data \
  -v $(pwd):/backup \
  alpine tar czf /backup/volguard_backup_$(date +%Y%m%d).tar.gz -C /data .

# Restore database
docker run --rm \
  -v volguard_database:/data \
  -v $(pwd):/backup \
  alpine tar xzf /backup/volguard_backup_YYYYMMDD.tar.gz -C /data
```

### Reset Database

```bash
# ⚠️ WARNING: This deletes all trade history!
docker-compose down
docker volume rm volguard_database
docker-compose up -d
```

---

## 🔍 Monitoring

### Health Checks
```bash
# Container status
docker-compose ps

# Manual health check
curl http://localhost:8000/api/health

# Detailed status
curl http://localhost:8000/api/dashboard | jq
```

### Resource Usage
```bash
# Real-time stats
docker stats volguard-trading

# Shows CPU, memory, network, disk I/O
```

### Active Monitoring
```bash
# Watch positions
watch -n 5 'curl -s http://localhost:8000/api/positions | jq'

# Watch P&L
watch -n 5 'curl -s http://localhost:8000/api/dashboard | jq .daily_pnl'
```

---

## 🐛 Troubleshooting

### Container Won't Start

```bash
# Check logs
docker-compose logs

# Common fixes:
# 1. Port conflict: Change PORT in .env
# 2. Missing token: Set UPSTOX_ACCESS_TOKEN
# 3. File permissions: sudo chown -R $USER:$USER .
```

### Database Issues

```bash
# View database
docker-compose exec volguard ls -la /app/data/

# Reset if corrupted
docker-compose exec volguard rm /app/data/volguard.db
docker-compose restart
```

### Telegram Not Working

```bash
# Verify config
docker-compose exec volguard env | grep TELEGRAM

# Test endpoint
curl -X POST http://localhost:8000/api/test-alert

# Check logs
docker-compose logs | grep -i telegram
```

### Memory Issues

```bash
# If OOM (Out of Memory) errors:
# Edit docker-compose.yml:
    limits:
      memory: 4G  # Increase from 2G

# Apply changes
docker-compose up -d
```

---

## 🔒 Security Checklist

- [ ] `.env` file is **NOT** in version control
- [ ] Strong, unique Upstox token
- [ ] `ENABLE_AUTO_TRADING=false` until tested
- [ ] Firewall configured (if exposed to internet)
- [ ] Regular backups enabled
- [ ] Container runs as non-root user
- [ ] Resource limits set
- [ ] Logs monitored

---

## 📈 Going to Production

### Before Enabling Real Trading

1. ✅ Test thoroughly with `ENABLE_MOCK_TRADING=true`
2. ✅ Verify all risk parameters in code
3. ✅ Test circuit breaker functionality
4. ✅ Confirm Telegram alerts working
5. ✅ Set up monitoring and backups
6. ✅ Have emergency shutdown procedure
7. ✅ Understand capital at risk

### Enable Auto-Trading

```bash
# Edit .env
ENABLE_AUTO_TRADING=true
ENABLE_MOCK_TRADING=false

# Restart
docker-compose restart

# Monitor closely!
docker-compose logs -f
```

### Production Enhancements

- **Reverse Proxy:** Nginx with SSL
- **Monitoring:** Prometheus + Grafana
- **Log Aggregation:** ELK Stack or Datadog
- **Alerts:** PagerDuty integration
- **Backups:** Automated daily backups
- **Redundancy:** Multi-region deployment

---

## 🆘 Emergency Procedures

### Immediate Shutdown

```bash
# Stop all trading immediately
docker-compose stop

# Or force kill
docker-compose kill
```

### Circuit Breaker

```bash
# Trigger manually via API
curl -X POST http://localhost:8000/api/system/reset-circuit-breaker

# Or access logs to see if auto-triggered
docker-compose logs | grep -i "circuit breaker"
```

### Data Recovery

```bash
# If container crashed, data is safe in volumes
docker volume ls | grep volguard

# Restore from backup
# (see Data Management section)
```

---

## 📚 Documentation

- **DEPLOYMENT.md** - Complete deployment guide
- **API Docs** - http://localhost:8000/docs (when running)
- **This README** - Quick reference

---

## 🎓 Learning Path

1. **Day 1:** Deploy with mock trading
2. **Day 2-7:** Monitor mock trades, verify logic
3. **Week 2:** Test all features (alerts, circuit breaker, exits)
4. **Week 3:** Paper trade with small capital
5. **Week 4+:** Gradually increase capital if profitable

---

## 💡 Tips & Best Practices

### Development
```bash
# Use .env for secrets, never hardcode
# Test changes with mock trading first
# Review logs daily: docker-compose logs --since 24h
```

### Operations
```bash
# Automate backups (cron job)
# Monitor resource usage
# Keep containers updated
# Document any custom changes
```

### Trading
```bash
# Start small and scale gradually
# Never risk more than you can afford to lose
# Monitor positions actively
# Have stop-loss discipline
# Keep detailed trade journal
```

---

## 🆘 Support

### Self-Help
1. Check logs: `docker-compose logs -f`
2. Review DEPLOYMENT.md
3. Check health: `curl http://localhost:8000/api/health`
4. Verify config: `docker-compose exec volguard env`

### Common Issues
- **Port conflict:** Change PORT in .env
- **Token invalid:** Regenerate Upstox token
- **Database locked:** Restart container
- **Telegram not sending:** Check token/chat ID

---

## 🎉 You're Ready!

You now have a professional-grade algorithmic trading system running in Docker with:

- ✅ Isolated, secure environment
- ✅ Real-time market data
- ✅ Advanced analytics
- ✅ Risk management
- ✅ Telegram notifications
- ✅ P&L attribution
- ✅ Easy deployment & updates

**Start with mock trading and scale gradually. Happy trading! 📈**

---

## ⚖️ Disclaimer

This software is for educational and research purposes. Trading involves substantial risk of loss. 
Past performance is not indicative of future results. Use at your own risk.

---

**VolGuard v3.2** | Dockerized | Production Ready | Built with ❤️
