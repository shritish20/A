# 🛡️ VolGuard v3.3 - Professional Options Trading System

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Upstox SDK](https://img.shields.io/badge/upstox--sdk-2.19.0-green.svg)](https://pypi.org/project/upstox-python-sdk/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.104.1-009688.svg)](https://fastapi.tiangolo.com)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**VolGuard** is a production-grade, fully-automated options trading system built on the Upstox API. It specializes in **overnight option selling** with institutional-grade risk management.

## ✨ Features

- ✅ **100% Upstox SDK v2.19.0 Aligned** - Every API call matches specification
- ✅ **WebSocket Streaming** - Real-time market data with zero REST polling
- ✅ **Overnight Option Selling** - Product="D" for carryforward positions
- ✅ **Multi-Leg GTT Orders** - Trailing stop loss and profit targets
- ✅ **Expiry Management** - Auto square-off 1 day before expiry @ 2:00 PM
- ✅ **Event Risk Management** - FOMC, RBI, Budget - square off 1 day before
- ✅ **Correlation Manager** - No duplicate strategies across expiries
- ✅ **Dynamic Configuration** - Runtime updates via API, no restart
- ✅ **Telegram Alerts** - Real-time notifications for all critical events
- ✅ **Docker Ready** - One-command deployment

## 🚀 Quick Start (30 Seconds)

```bash
# 1. Clone repository
git clone https://github.com/yourusername/volguard.git
cd volguard

# 2. Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure credentials
cp .env.example .env
# Edit .env with your Upstox tokens

# 5. Run the system
python volguard_final.py
```

## 🐳 Docker Deployment

```bash
# Build and run with Docker Compose
docker-compose up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down
```

## 📋 Prerequisites

- **Python 3.11+**
- **Upstox Trading Account** with API access
- **Upstox App Credentials** from [developer portal](https://upstox.com/developer/)

## 🔑 Getting Upstox Credentials

1. **Create an app** at [Upstox Developer Console](https://upstox.com/developer/apps/)
2. **Generate access token** via OAuth flow
3. **Copy credentials** to `.env` file

## 📊 API Documentation

Once running, access interactive API docs:
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

### Core Endpoints

| Endpoint | Description |
|---------|-------------|
| `GET /api/dashboard/analytics` | Market regime scores & mandates |
| `GET /api/dashboard/professional` | Hedge fund terminal (GEX, VRP, Greeks) |
| `GET /api/live/positions` | Real-time P&L and Greeks |
| `GET /api/journal/history` | Trade history with attribution |
| `POST /api/system/config` | Update configuration at runtime |
| `POST /api/emergency/exit-all` | PANIC BUTTON - exit all positions |

## ⚙️ Configuration

All settings are **runtime-updatable** via API - no restart required:

```bash
# Update max loss to 2%
curl -X POST http://localhost:8000/api/system/config \
  -H "X-Upstox-Token: your_token" \
  -H "Content-Type: application/json" \
  -d '{"max_loss": 2.0}'
```

## 🧪 Testing Mode

Start with mock trading to verify everything works:

```env
INITIAL_AUTO_TRADING=false
INITIAL_MOCK_TRADING=true
```

## 📁 Project Structure

```
volguard/
├── volguard_final.py      # Main application (single file)
├── requirements.txt       # Python dependencies
├── Dockerfile            # Container configuration
├── docker-compose.yml    # Local deployment
├── .env.example          # Environment template
├── .gitignore           # Git ignore rules
└── README.md            # This file
```

## 🚨 Emergency Procedures

```bash
# Immediate exit of ALL positions
curl -X POST http://localhost:8000/api/emergency/exit-all \
  -H "X-Upstox-Token: your_token"

# Reset circuit breaker
curl -X POST http://localhost:8000/api/system/reset-circuit-breaker \
  -H "X-Upstox-Token: your_token"
```

## 📈 Performance Optimization

| Setting | Recommended | Description |
|---------|------------|-------------|
| `MARKET_DATA_MODE=ltpc` | ✅ | LTP only - minimum bandwidth |
| `WEBSOCKET_RECONNECT_INTERVAL=5` | ✅ | 5 second reconnect |
| `MONITOR_INTERVAL_SECONDS=5` | ✅ | P&L check frequency |

## 🔒 Security Best Practices

1. **Never commit `.env` file** - it's in `.gitignore`
2. **Rotate access tokens** weekly
3. **Use sandbox mode** for testing
4. **Enable 2FA** on your Upstox account

## 🆘 Troubleshooting

**Q: WebSocket won't connect?**  
A: Verify access token is valid and not expired. Check `UPSTOX_SANDBOX` setting.

**Q: Orders not executing?**  
A: Check margin availability via `/api/health` endpoint.

**Q: Token refresh failing?**  
A: Ensure `UPSTOX_REFRESH_CODE` is a valid, unused authorization code.

## 📄 License

MIT License - See [LICENSE](LICENSE) file

## 🤝 Support

- **Issues**: [GitHub Issues](https://github.com/yourusername/volguard/issues)
- **API Docs**: [Upstox Developer Portal](https://upstox.com/developer/)

---

**Made with ❤️ for professional traders** | **100% Upstox SDK Aligned** | **v3.3.0**
