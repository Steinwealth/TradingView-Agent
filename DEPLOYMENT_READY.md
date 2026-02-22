# Coinbase Deployment - Ready to Deploy

**Date:** November 6, 2025  
**Version:** 3.6.0  
**Account:** Coinbase Main (50/50 @ 1x/3x)  
**Mode:** DEMO (until Agent complete)

---

## ✅ **Setup Complete**

### **Secrets Stored in Google Secret Manager:**
- ✅ `coinbase-api-key-1` - API Key Name
- ✅ `coinbase-private-key-1` - EC Private Key
- ✅ `telegram-bot-token` - Telegram bot token
- ✅ `telegram-chat-id` - Destination chat/channel ID

### **Configuration Updated:**
- ✅ `config.yaml` - Version 3.6 with virtual partitions
- ✅ Account: `coinbase-main`
- ✅ Mode: `DEMO`
- ✅ Partitions: 50% @ 1x, 50% @ 3x
- ✅ Strategy: `ichimoku-coinbase-usa-5m`

---

## 🚀 **Deploy to Google Cloud Run**

### **Command:**

```bash
cd /path/to/tradingview-agent

gcloud run deploy tradingview-agent \
  --source . \
  --region us-central1 \
  --allow-unauthenticated \
  --min-instances 1 \
  --max-instances 3 \
  --memory 512Mi \
  --cpu 1 \
  --timeout 60s \
  --update-secrets \
      COINBASE_API_KEY_1=coinbase-api-key-1:latest,\
      COINBASE_PRIVATE_KEY_1=coinbase-private-key-1:latest,\
      TELEGRAM_BOT_TOKEN=telegram-bot-token:latest,\
      TELEGRAM_CHAT_ID=telegram-chat-id:latest
```

**This will:**
- Build and deploy the Agent
- Mount Coinbase secrets from Secret Manager
- Start with min 1 instance (always ready)
- Return webhook URL

---

## 🧪 **Post-Deployment Testing**

### **1. Get Webhook URL:**

```bash
gcloud run services describe tradingview-agent \
  --region us-central1 \
  --format='value(status.url)'
```

**Save this URL!** You'll need it for TradingView alerts.

---

### **2. Test Health Endpoint:**

```bash
curl https://YOUR-AGENT-URL.run.app/health
```

**Expected Response:**
```json
{
  "status": "healthy",
  "version": "3.6.0",
  "config_type": "YAML",
  "strategies": {
    "total": 1,
    "enabled": 1
  },
  "accounts": {
    "total": 1,
    "enabled": 1,
    "demo": 1,
    "live": 0
  },
  "partition_mode": "enabled"
}
```

---

### **3. Test Partition Status:**

```bash
curl https://YOUR-AGENT-URL.run.app/partitions/coinbase-main
```

**Expected Response:**
```json
{
  "account_id": "coinbase-main",
  "partition_mode": "enabled",
  "partition_split": "50/50",
  "real_account": {
    "total_balance": 1000.0,
    "available_cash": 1000.0,
    "cash_in_positions": 0.0
  },
  "partitions": {
    "50%-btc-1x": {
      "virtual_balance": 500.0,
      "leverage": 1,
      "enabled": true
    },
    "50%-btc-3x": {
      "virtual_balance": 500.0,
      "leverage": 3,
      "enabled": true
    }
  }
}
```

---

### **4. Check Logs:**

```bash
gcloud run logs read tradingview-agent --region us-central1 --limit 50
```

**Look For:**
- ✅ "Loaded YAML configuration"
- ✅ "Partition mode enabled for coinbase-main"
- ✅ "Initialized 50%-btc-1x: $500.00 @ 1x"
- ✅ "Initialized 50%-btc-3x: $500.00 @ 3x"
- ⚠️ Check for authentication errors

---

## ⚠️ **Important: API Format Verification**

**Your Coinbase credentials are in Cloud API (CDP) format:**
- API Key Name (full path)
- EC Private Key (PEM format)

**Broker code currently expects legacy format:**
- api_key (string)
- api_secret (HMAC secret)

**What This Means:**
- Authentication may fail on first deployment
- If it does, broker code needs update for JWT/Cloud API authentication
- See `COINBASE_API_FORMAT_NOTE.md` for details

**How to Check:**
1. Deploy and test health endpoint
2. Check logs for auth errors
3. If auth fails: "Invalid signature" or "Authentication failed"
4. Then we'll update broker code

---

## 📺 **Next: TradingView Alerts**

Once deployment succeeds and authentication works:

### **Create 3 Alerts on TradingView:**

**Chart:** COINBASE:BIPZ2030, 5-minute timeframe  
**Strategy:** €£$¥ Ichimoku (easy_ichimoku_v8.pine)

**Alert 1 - LONG:**
```json
{
  "strategy_id": "ichimoku-coinbase-usa-5m",
  "side": "buy",
  "price": "{{close}}"
}
```

**Alert 2 - SHORT:**
```json
{
  "strategy_id": "ichimoku-coinbase-usa-5m",
  "side": "sell",
  "price": "{{close}}"
}
```

**Alert 3 - EXIT:**
```json
{
  "strategy_id": "ichimoku-coinbase-usa-5m",
  "side": "exit",
  "price": "{{close}}"
}
```

**Webhook URL:** `https://YOUR-AGENT-URL.run.app/webhook`

---

## 📊 **Expected DEMO Results**

**Starting Balance:** $1,000
- Partition 1x: $500 @ 1x leverage
- Partition 3x: $500 @ 3x leverage

**Month 1 Expected:**
- Total: $1,000 → $1,821 (+82.1%)
- Partition 1x: $500 → $620 (+24%)
- Partition 3x: $500 → $1,201 (+140%)

**To Monitor:**
```bash
# Check partition balances
curl https://YOUR-AGENT-URL.run.app/partitions/coinbase-main

# Check DEMO balance
curl https://YOUR-AGENT-URL.run.app/demo/balance/coinbase-main
```

---

## ✅ **Ready to Deploy!**

Run the deployment command above and let me know the result!

If authentication works → Proceed to TradingView alerts  
If authentication fails → Update broker code for Cloud API format

🚀

