#!/bin/bash
# Cloud Scheduler Setup Script for TradingView Agent Scale-to-Zero
# Run this after deploying the Agent to Cloud Run

set -e

# Get the Cloud Run service URL
echo "Getting Cloud Run service URL..."
SERVICE_URL=$(gcloud run services describe tradingview-agent \
  --region=us-central1 \
  --format="value(status.url)" 2>/dev/null || echo "")

if [ -z "$SERVICE_URL" ]; then
  echo "❌ Error: Could not find tradingview-agent service in us-central1"
  echo "   Make sure the service is deployed first."
  exit 1
fi

echo "✅ Service URL: $SERVICE_URL"
echo ""

# Project ID
PROJECT_ID=$(gcloud config get-value project)
echo "Project: $PROJECT_ID"
echo ""

# Create Cloud Scheduler jobs
echo "Creating Cloud Scheduler jobs..."
echo ""

# Job 1: Friday 7:00 AM ET (4am PT) - First check after entry blocking activates
echo "1. Creating Friday 7:00 AM ET (4am PT) check..."
gcloud scheduler jobs create http friday-early-check \
  --location=us-central1 \
  --schedule="0 7 * * 5" \
  --uri="$SERVICE_URL/scale-check" \
  --http-method=GET \
  --time-zone="America/New_York" \
  --description="Early Friday check - scale to zero if all positions closed" \
  --project="$PROJECT_ID" 2>/dev/null && echo "   ✅ Created" || echo "   ⚠️  Already exists or error"

# Job 2: Friday 4:30 PM ET - Primary scale check after market close
echo "2. Creating Friday 4:30 PM ET scale check..."
gcloud scheduler jobs create http friday-scale-check \
  --location=us-central1 \
  --schedule="30 16 * * 5" \
  --uri="$SERVICE_URL/scale-check" \
  --http-method=GET \
  --time-zone="America/New_York" \
  --description="Check if service can scale to zero on Friday after positions close" \
  --project="$PROJECT_ID" 2>/dev/null && echo "   ✅ Created" || echo "   ⚠️  Already exists or error"

# Job 3: Friday 5:00 PM ET - Final check
echo "3. Creating Friday 5:00 PM ET final check..."
gcloud scheduler jobs create http friday-final-check \
  --location=us-central1 \
  --schedule="0 17 * * 5" \
  --uri="$SERVICE_URL/scale-check" \
  --http-method=GET \
  --time-zone="America/New_York" \
  --description="Final Friday check before weekend - scale to zero if flat" \
  --project="$PROJECT_ID" 2>/dev/null && echo "   ✅ Created" || echo "   ⚠️  Already exists or error"

# Job 4: Weekend monitoring (every 30 minutes Sat-Sun)
echo "4. Creating weekend monitoring (every 30 min Sat-Sun)..."
gcloud scheduler jobs create http weekend-monitor \
  --location=us-central1 \
  --schedule="*/30 * * * 0,6" \
  --uri="$SERVICE_URL/scale-check" \
  --http-method=GET \
  --time-zone="America/New_York" \
  --description="Monitor for position closures on weekends - scale to zero when flat" \
  --project="$PROJECT_ID" 2>/dev/null && echo "   ✅ Created" || echo "   ⚠️  Already exists or error"

# Job 5: Monday 4:00 AM ET (1am PT) - Wake service for new trading week
echo "5. Creating Monday 4:00 AM ET (1am PT) wake job..."
gcloud scheduler jobs create http monday-wake \
  --location=us-central1 \
  --schedule="0 4 * * 1" \
  --uri="$SERVICE_URL/health" \
  --http-method=GET \
  --time-zone="America/New_York" \
  --description="Wake service Monday 1am PT (4am ET) for new trading week" \
  --project="$PROJECT_ID" 2>/dev/null && echo "   ✅ Created" || echo "   ⚠️  Already exists or error"

# Job 6: Daily EOD at 4:00 PM PT (7:00 PM ET) - Trigger end-of-day summary
echo "6. Creating daily EOD trigger (4:00 PM PT / 7:00 PM ET)..."
# Get admin secret from Secret Manager or use placeholder (user must update)
ADMIN_SECRET=$(gcloud secrets versions access latest --secret="admin-secret" --project="$PROJECT_ID" 2>/dev/null || echo "REPLACE_WITH_ADMIN_SECRET")
gcloud scheduler jobs create http daily-eod \
  --location=us-central1 \
  --schedule="0 19 * * 1-5" \
  --uri="$SERVICE_URL/eod/trigger?admin_secret=$ADMIN_SECRET" \
  --http-method=GET \
  --time-zone="America/New_York" \
  --description="Trigger daily EOD summary at 4:00 PM PT (7:00 PM ET) on weekdays" \
  --project="$PROJECT_ID" 2>/dev/null && echo "   ✅ Created" || echo "   ⚠️  Already exists or error"

echo ""
echo "=" | head -c 70
echo ""
echo "✅ Cloud Scheduler Setup Complete!"
echo ""
echo "Jobs created:"
echo "  • friday-early-check (Fri 7:00 AM ET / 4am PT)"
echo "  • friday-scale-check (Fri 4:30 PM ET)"
echo "  • friday-final-check (Fri 5:00 PM ET)"
echo "  • weekend-monitor (Every 30 min Sat-Sun)"
echo "  • monday-wake (Mon 4:00 AM ET / 1am PT)"
echo "  • daily-eod (Mon-Fri 7:00 PM ET / 4:00 PM PT) ⭐ NEW"
echo ""
echo "⚠️  IMPORTANT: Update daily-eod job with correct admin_secret:"
echo "   gcloud scheduler jobs update daily-eod --location=us-central1 \\"
echo "     --uri=\"$SERVICE_URL/eod/trigger?admin_secret=YOUR_ADMIN_SECRET\""
echo ""
echo "To list jobs:"
echo "  gcloud scheduler jobs list --location=us-central1"
echo ""
echo "To test a job:"
echo "  gcloud scheduler jobs run daily-eod --location=us-central1"
echo ""
