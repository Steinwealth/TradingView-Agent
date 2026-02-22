"""
TradingView Agent - Main Application (V3.5)
Professional webhook relay for TradingView → Broker automation

V3.5 Features:
- YAML configuration (config.yaml)
- Strategy ID routing (strategy_id)
- Multi-account support
- Cleaner alert format
"""

from fastapi import FastAPI, Request, HTTPException, BackgroundTasks, Header
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import os
import logging
from datetime import datetime, timedelta
import hmac
import hashlib
import json
from pathlib import Path
from typing import Optional, Dict

# Local imports
from config import settings
from config.risk_limits import RISK_LIMITS
from config.yaml_config import get_config, get_strategy_by_id
from services.webhook_handler import WebhookHandler
from services.order_executor import OrderExecutor
from services.risk_manager import RiskManager
from services.pnl_tracker import PnLTracker
from services.contract_monitor import contract_monitor, start_contract_monitoring
from services.daily_summary_scheduler import DailySummaryScheduler
from services.afterhours_position_checker import AfterhoursPositionChecker
from models.webhook_payload import WebhookPayload, OrderResponse
from services.broker_reconciler import BrokerReconciler
from services.position_monitor import position_monitor
from services.exit_monitor import exit_monitor

# Initialize FastAPI app
app = FastAPI(
    title="TradingView Agent",
    description="24/7 webhook relay for TradingView strategies to broker APIs (V3.5 YAML Config)",
    version="3.5.0"
)

# CORS middleware (for dashboard access)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize services
# Load YAML config
config = get_config()

webhook_handler = WebhookHandler(settings)
pnl_tracker = PnLTracker(use_firestore=settings.USE_FIRESTORE)
risk_manager = RiskManager(RISK_LIMITS, settings=settings, pnl_tracker=pnl_tracker)
order_executor = OrderExecutor(settings, risk_manager=risk_manager)
daily_summary_scheduler = DailySummaryScheduler(order_executor, pnl_tracker)
afterhours_position_checker = AfterhoursPositionChecker(order_executor)
broker_reconciler = BrokerReconciler(order_executor)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Repo root: when main.py is at repo root, parent is repo; else use env or /tmp (e.g. Cloud Run /app)
try:
    _main_dir = Path(__file__).resolve().parent
    repo_root = _main_dir if (_main_dir / "config.yaml").exists() else _main_dir.parent
except Exception:
    repo_root = Path(os.getenv("HISTORICAL_ENHANCER_FALLBACK_ROOT", "/tmp"))
DEFAULT_HISTORICAL_DIR = repo_root / "historical_enhancer" / "daily"
HISTORICAL_ENHANCER_DIR = Path(os.getenv("HISTORICAL_ENHANCER_DIR", str(DEFAULT_HISTORICAL_DIR)))
HISTORICAL_RETENTION_DAYS = int(os.getenv("HISTORICAL_ENHANCER_RETENTION_DAYS", "45"))
HISTORICAL_STORAGE_MODE = os.getenv("HISTORICAL_ENHANCER_STORAGE", "file").lower()
HISTORICAL_FIRESTORE_COLLECTION = os.getenv("HISTORICAL_ENHANCER_FIRESTORE_COLLECTION", "historical_enhancer")

_historical_firestore_collection: Optional["firestore.CollectionReference"] = None  # type: ignore

if HISTORICAL_STORAGE_MODE == "firestore":
    try:
        from google.cloud import firestore  # type: ignore

        firestore_client = firestore.Client()
        _historical_firestore_collection = firestore_client.collection(HISTORICAL_FIRESTORE_COLLECTION)
        logger.info(f"Historical Enhancer logging to Firestore collection '{HISTORICAL_FIRESTORE_COLLECTION}'")
    except Exception as firestore_error:
        logger.warning(f"Historical Enhancer Firestore initialization failed ({firestore_error}); falling back to file storage.")
        HISTORICAL_STORAGE_MODE = "file"
        _historical_firestore_collection = None


def _prune_historical_enhancer():
    if HISTORICAL_STORAGE_MODE != "file":
        return
    if HISTORICAL_RETENTION_DAYS <= 0 or not HISTORICAL_ENHANCER_DIR.exists():
        return
    cutoff_date = datetime.utcnow().date() - timedelta(days=HISTORICAL_RETENTION_DAYS)
    for path in HISTORICAL_ENHANCER_DIR.glob("*.jsonl"):
        try:
            date_part = path.stem.split("_")[0]
            file_date = datetime.strptime(date_part, "%Y-%m-%d").date()
        except Exception:
            continue
        if file_date < cutoff_date:
            try:
                path.unlink()
            except Exception as prune_err:
                logger.debug(f"Historical Enhancer cleanup skipped for {path}: {prune_err}")


def _append_historical_enhancer_event(payload: WebhookPayload, strategy_name: str, execution_result: Optional[OrderResponse] = None, pnl_data: Optional[Dict] = None):
    """
    Capture Historical Enhancer data for trade analysis.
    
    Enhanced to include execution context:
    - Entry signals (when historical_enhancer field is present)
    - Exit signals (when historical_enhancer field is present)
    - Execution data (slippage, actual prices, order details)
    - P&L data (for exit events)
    
    This data is used to analyze losing trades and improve strategy performance.
    """
    try:
        event_type = "entry" if payload.is_entry() else ("exit" if payload.is_exit() else "other")
        
        # Capture both entry and exit events if historical_enhancer data is present
        # For entries, require historical_enhancer field
        # For exits, also capture if historical_enhancer is present (for exit analysis)
        if event_type == "entry" and not payload.historical_enhancer:
            return
        # Note: Exit events are captured if historical_enhancer is present (optional for exits)

        bar_time = payload.bar_time or datetime.utcnow().isoformat()
        date_part = bar_time.split("T")[0]
        
        # Calculate slippage if execution data available
        slippage = None
        slippage_pct = None
        if execution_result and execution_result.fill_price and payload.price:
            slippage = execution_result.fill_price - payload.price
            slippage_pct = (slippage / payload.price) * 100 if payload.price > 0 else None
        
        # Build historical_enhancer dict with exit_reason if available
        historical_enhancer_data = payload.historical_enhancer.copy() if payload.historical_enhancer and isinstance(payload.historical_enhancer, dict) else (payload.historical_enhancer or {})
        
        # For exit events, ensure exit_reason is set (from payload, historical_enhancer, or default)
        if event_type == "exit":
            exit_reason = getattr(payload, 'exit_reason', None)
            if not exit_reason and isinstance(historical_enhancer_data, dict):
                exit_reason = historical_enhancer_data.get('exit_reason')
            if not exit_reason:
                # Default exit reason based on source
                if payload.source and "Agent" in payload.source:
                    exit_reason = "Agent Exit"
                else:
                    exit_reason = "TradingView Exit"
            
            # Ensure exit_reason is in historical_enhancer data
            if isinstance(historical_enhancer_data, dict):
                historical_enhancer_data['exit_reason'] = exit_reason
            elif not historical_enhancer_data:
                historical_enhancer_data = {'exit_reason': exit_reason}
        
        record = {
            "event": event_type,
            "received_at": datetime.utcnow().isoformat(),
            "strategy_id": payload.strategy_id,
            "strategy_name": strategy_name,
            "side": payload.side,
            "price": payload.price,  # Signal price from TradingView
            "bar_time": payload.bar_time,
            "cid": payload.cid,
            "symbol": payload.symbol,
            "timeframe": payload.timeframe,
            "historical_enhancer": historical_enhancer_data,  # Includes peak_profit_pct, peak_profit_time, and exit_reason for exits
            
            # Execution context (added by Agent)
            "execution": {
                "status": execution_result.status if execution_result else None,
                "order_id": execution_result.order_id if execution_result else None,
                "client_order_id": execution_result.client_order_id if execution_result else None,
                "signal_price": payload.price,  # TradingView signal price
                "execution_price": execution_result.fill_price if execution_result else None,  # Actual fill price
                "slippage": slippage,  # slippage in price units
                "slippage_pct": slippage_pct,  # slippage as percentage
                "quantity": execution_result.quantity if execution_result else None,
                "commission": execution_result.commission if execution_result else None,
                "funding_fee": execution_result.funding_fee if execution_result else None,
                "broker": execution_result.broker if execution_result else None,
                "account_id": execution_result.account_id if execution_result else None,
                "leverage": execution_result.leverage if execution_result else None,
                "execution_timestamp": execution_result.timestamp if execution_result else None,
            } if execution_result else None,
            
            # P&L data (for exit events - from OrderResponse)
            "pnl": {
                "pnl_usd": execution_result.pnl_usd if execution_result and hasattr(execution_result, 'pnl_usd') and execution_result.pnl_usd is not None else None,
                "pnl_pct": execution_result.pnl_pct if execution_result and hasattr(execution_result, 'pnl_pct') and execution_result.pnl_pct is not None else None,
                "entry_price": execution_result.entry_price if execution_result and hasattr(execution_result, 'entry_price') and execution_result.entry_price is not None else None,
                "exit_price": execution_result.fill_price if execution_result and execution_result.fill_price else None,
                "holding_time_seconds": execution_result.holding_time_seconds if execution_result and hasattr(execution_result, 'holding_time_seconds') and execution_result.holding_time_seconds is not None else None,
                "commission": execution_result.commission if execution_result and hasattr(execution_result, 'commission') and execution_result.commission is not None else None,
                "funding_fee": execution_result.funding_fee if execution_result and hasattr(execution_result, 'funding_fee') and execution_result.funding_fee is not None else None,
            } if (execution_result and payload.is_exit() and 
                  hasattr(execution_result, 'pnl_usd') and execution_result.pnl_usd is not None) else None,
        }

        if HISTORICAL_STORAGE_MODE == "firestore" and _historical_firestore_collection is not None:
            _historical_firestore_collection.add(record)
        else:
            HISTORICAL_ENHANCER_DIR.mkdir(parents=True, exist_ok=True)
            file_base = f"{date_part}_{payload.strategy_id}"
            jsonl_path = HISTORICAL_ENHANCER_DIR / f"{file_base}.jsonl"
            with jsonl_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(record) + "\n")
            _prune_historical_enhancer()
    except Exception as log_error:
        logger.warning(f"Historical Enhancer logging failed: {log_error}")


@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "TradingView Agent",
        "version": "3.5.0",
        "config_type": "YAML",
        "timestamp": datetime.utcnow().isoformat()
    }


@app.get("/health")
async def health_check():
    """
    Detailed health check for monitoring (V3.5)
    """
    try:
        # Get config stats
        from config.yaml_config import get_enabled_strategies, get_enabled_accounts
        
        enabled_strategies = get_enabled_strategies(config)
        enabled_accounts = get_enabled_accounts(config)
        
        # Check broker connections
        broker_status = await webhook_handler.check_broker_connections()
        
        # Check last heartbeat
        last_heartbeat = await webhook_handler.get_last_heartbeat()
        heartbeat_ok = (datetime.utcnow() - last_heartbeat).seconds < 300  # 5 min
        
        partition_accounts_summary = order_executor.get_partition_overview()
        
        return {
            "status": "healthy" if broker_status and heartbeat_ok else "degraded",
            "version": "3.5.0",
            "config_type": "YAML",
            "timestamp": datetime.utcnow().isoformat(),
            "strategies": {
                "total": len(config.strategies),
                "enabled": len(enabled_strategies),
                "ids": [s.id for s in enabled_strategies]
            },
            "accounts": {
                "total": len(config.accounts),
                "enabled": len(enabled_accounts),
                "demo": len([a for a in enabled_accounts if a.mode == "DEMO"]),
                "live": len([a for a in enabled_accounts if a.mode == "LIVE"])
            },
            "partition_accounts_summary": partition_accounts_summary,
            "brokers": broker_status,
            "last_heartbeat": last_heartbeat.isoformat() if last_heartbeat else None,
            "heartbeat_ok": heartbeat_ok
        }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "error": str(e)}
        )


@app.post("/webhook")
async def webhook(request: Request, background_tasks: BackgroundTasks):
    """
    Main webhook endpoint for TradingView alerts (V3.5)
    
    Expected payload:
    {
      "strategy_id": "ichimoku-coinbase-usa-5m",
      "side": "buy",
      "price": 98500.00
    }
    
    Receives TradingView alert → Validates → Routes to broker → Executes order
    
    CRITICAL: This endpoint must respond quickly (< 10 seconds) to prevent TradingView timeouts.
    Cloud Run has a 60-second timeout, but TradingView may timeout earlier.
    """
    # CRITICAL: Log webhook receipt IMMEDIATELY before any processing
    # This ensures we capture ALL webhooks, even if they fail validation
    webhook_timestamp = datetime.utcnow().isoformat()
    client_ip = request.client.host if request.client else "unknown"
    user_agent = request.headers.get("user-agent", "unknown")
    
    # Log request metadata for debugging delivery issues
    logger.info(
        f"🌐 WEBHOOK REQUEST RECEIVED | "
        f"Time: {webhook_timestamp} | "
        f"Method: {request.method} | "
        f"Path: {request.url.path} | "
        f"IP: {client_ip} | "
        f"User-Agent: {user_agent[:100]} | "
        f"Content-Type: {request.headers.get('content-type', 'unknown')}"
    )
    
    try:
        # Get raw body - log BEFORE parsing to catch all requests
        raw_body = await request.body()
        body_str = raw_body.decode('utf-8') if raw_body else ""
        
        # Log raw webhook receipt immediately (before validation)
        logger.info(
            f"🌐 RAW WEBHOOK RECEIVED | "
            f"Time: {webhook_timestamp} | "
            f"IP: {client_ip} | "
            f"Body Length: {len(body_str)} bytes | "
            f"Body Preview: {body_str[:200]}"
        )
        
        # Parse JSON
        try:
            body = await request.json()
        except Exception as json_error:
            logger.error(
                f"❌ WEBHOOK JSON PARSE FAILED | "
                f"Time: {webhook_timestamp} | "
                f"Error: {str(json_error)} | "
                f"Body: {body_str[:500]}"
            )
            raise HTTPException(status_code=400, detail=f"Invalid JSON: {str(json_error)}")
        
        # Log parsed webhook details
        strategy_id = body.get('strategy_id', 'unknown')
        side = body.get('side', 'unknown')
        price = body.get('price')
        
        logger.info(
            f"📥 WEBHOOK PARSED | "
            f"Strategy: {strategy_id} | "
            f"Side: {side} | "
            f"Price: {price} | "
            f"Full Payload: {body}"
        )
        
        # For exit signals, add extra logging
        if side.lower() in ['exit', 'close']:
            logger.warning(
                f"🚨 EXIT SIGNAL RECEIVED | "
                f"Strategy: {strategy_id} | "
                f"Side: {side} | "
                f"Price: {price} | "
                f"Timestamp: {webhook_timestamp} | "
                f"Full Payload: {body}"
            )
        
        # 1. VALIDATE PAYLOAD
        try:
            payload = WebhookPayload(**body)
        except Exception as e:
            logger.error(
                f"❌ WEBHOOK VALIDATION FAILED | "
                f"Strategy: {strategy_id} | "
                f"Side: {side} | "
                f"Error: {str(e)} | "
                f"Payload: {body}"
            )
            # For exit signals, send critical alert even if validation fails
            if side.lower() in ['exit', 'close']:
                try:
                    from services.telegram_notifier import telegram_notifier
                    await telegram_notifier.send_critical_failure(
                        f"🚨 EXIT SIGNAL VALIDATION FAILED\n\n"
                        f"Strategy: {strategy_id}\n"
                        f"Side: {side}\n"
                        f"Price: {price}\n"
                        f"Error: {str(e)}\n\n"
                        f"⚠️ Exit signal was received but failed validation.\n"
                        f"Check webhook payload format.\n\n"
                        f"Raw Payload: {body}"
                    )
                except Exception as alert_error:
                    logger.error(f"Failed to send validation failure alert: {alert_error}")
            raise HTTPException(status_code=400, detail=f"Invalid payload: {str(e)}")
        
        # 2. GET STRATEGY FROM CONFIG
        strategy_config = get_strategy_by_id(config, payload.strategy_id)
        if not strategy_config:
            logger.error(
                f"❌ UNKNOWN STRATEGY | "
                f"Strategy ID: {payload.strategy_id} | "
                f"Side: {payload.side} | "
                f"Payload: {body}"
            )
            # For exit signals, send critical alert
            if payload.is_exit():
                try:
                    from services.telegram_notifier import telegram_notifier
                    await telegram_notifier.send_critical_failure(
                        f"🚨 EXIT SIGNAL - UNKNOWN STRATEGY\n\n"
                        f"Strategy ID: {payload.strategy_id}\n"
                        f"Side: {payload.side}\n"
                        f"Price: {payload.price}\n\n"
                        f"⚠️ Exit signal received for unknown strategy.\n"
                        f"Check strategy configuration.\n\n"
                        f"Payload: {body}"
                    )
                except Exception as alert_error:
                    logger.error(f"Failed to send unknown strategy alert: {alert_error}")
            raise HTTPException(status_code=404, detail=f"Unknown strategy: {payload.strategy_id}")
        
        strategy_name = strategy_config.name
        
        # 3. CHECK STRATEGY ENABLED
        # CRITICAL: Exit signals should ALWAYS be processed, even if strategy is disabled
        # This ensures open positions can be closed even if strategy is temporarily disabled
        if not strategy_config.enabled and not payload.is_exit():
            logger.warning(
                f"⚠️ STRATEGY DISABLED | "
                f"Strategy: {payload.strategy_id} | "
                f"Side: {payload.side} | "
                f"Payload: {body}"
            )
            raise HTTPException(status_code=403, detail=f"Strategy '{payload.strategy_id}' is disabled")
        elif not strategy_config.enabled and payload.is_exit():
            # Exit signal for disabled strategy - allow it but log warning
            logger.warning(
                f"⚠️ EXIT SIGNAL FOR DISABLED STRATEGY (ALLOWING) | "
                f"Strategy: {payload.strategy_id} | "
                f"Symbol: {strategy_config.execution.product_id} | "
                f"Exit signals are always allowed to close open positions"
            )
        
        # 4. HANDLE HEARTBEAT (if applicable)
        if payload.side == 'heartbeat':
            await webhook_handler.update_heartbeat(strategy_name)
            return {
                "status": "success",
                "action": "heartbeat",
                "strategy": strategy_name,
                "timestamp": datetime.utcnow().isoformat()
            }
        
        # 4.5. WEEKEND ENTRY BLOCKING (if enabled)
        # Exits are always allowed, only new entries are blocked Fri 4am PT - Mon 1am PT
        if config.general.disable_entries_fri_sun:
            from zoneinfo import ZoneInfo
            et_tz = ZoneInfo('America/New_York')
            now_et = datetime.now(et_tz)
            weekday = now_et.weekday()  # Monday=0, Friday=4, Saturday=5, Sunday=6
            hour = now_et.hour
            
            # Block entries: Friday 4am PT (7am ET) - Monday 1am PT (4am ET)
            # Allow exits at all times
            is_entry = payload.side in ['buy', 'sell']
            is_friday_after_4am_pt = (weekday == 4 and hour >= 7)  # Friday 4am PT = 7am ET
            is_saturday = weekday == 5  # Saturday
            is_sunday = weekday == 6  # Sunday
            is_monday_before_1am_pt = (weekday == 0 and hour < 4)  # Monday before 1am PT = 4am ET
            should_block = is_entry and (is_friday_after_4am_pt or is_saturday or is_sunday or is_monday_before_1am_pt)
            
            if should_block:
                logger.info(f"📅 Weekend entry blocked: {payload.side} signal for {payload.strategy_id} on {now_et.strftime('%A %I:%M %p %Z')}")
                return {
                    "status": "rejected",
                    "reason": f"Weekend entry blocked (Fri 4am PT - Mon 1am PT). Exits always allowed.",
                    "strategy": strategy_name,
                    "strategy_id": payload.strategy_id,
                    "day": now_et.strftime('%A'),
                    "time_et": now_et.strftime('%I:%M %p %Z'),
                    "timestamp": datetime.utcnow().isoformat()
                }
        
        # 5. RISK CHECKS
        # Note: Exit signals now bypass all risk checks in risk_manager
        risk_check = await risk_manager.validate_trade(payload)
        if not risk_check['allowed']:
            logger.warning(
                f"⚠️ RISK CHECK FAILED | "
                f"Strategy: {strategy_name} | "
                f"Side: {payload.side} | "
                f"Reason: {risk_check['reason']} | "
                f"Payload: {body}"
            )
            # Exit signals should never reach here (they bypass risk checks)
            # But if they do, send critical alert
            if payload.is_exit():
                logger.error(
                    f"❌ CRITICAL: Exit signal rejected by risk manager! "
                    f"This should never happen - exits bypass risk checks. "
                    f"Reason: {risk_check['reason']}"
                )
                try:
                    from services.telegram_notifier import telegram_notifier
                    await telegram_notifier.send_critical_failure(
                        f"🚨 CRITICAL: EXIT SIGNAL REJECTED BY RISK MANAGER\n\n"
                        f"Strategy: {strategy_name}\n"
                        f"Symbol: {strategy_config.execution.product_id}\n"
                        f"Side: {payload.side}\n"
                        f"Price: {payload.price}\n"
                        f"Reason: {risk_check['reason']}\n\n"
                        f"⚠️ CRITICAL BUG: Exit signals should bypass risk checks!\n"
                        f"This indicates a code issue.\n\n"
                        f"⚠️ Position may still be open!"
                    )
                except Exception as alert_error:
                    logger.error(f"Failed to send risk check failure alert: {alert_error}")
            return {
                "status": "rejected",
                "reason": risk_check['reason'],
                "strategy": strategy_name,
                "strategy_id": payload.strategy_id,
                "payload": payload.dict()
            }
        
        # Note: Historical Enhancer will be called after execution to include execution data
        
        # 6. EXECUTE ORDER (in background for fast response)
        # CRITICAL: Return response quickly (< 10 seconds) to prevent TradingView timeout
        # Cloud Run can scale to zero, causing cold starts that may delay webhook processing
        # TradingView may timeout if response takes too long
        
        # Log that we're queuing the background task
        logger.info(
            f"✅ WEBHOOK ACCEPTED | "
            f"Strategy: {strategy_name} | "
            f"Side: {payload.side} | "
            f"Symbol: {strategy_config.execution.product_id} | "
            f"Queuing background task..."
        )
        
        # For exit signals, add extra logging and ensure fast response
        if payload.is_exit():
            logger.warning(
                f"🚨 EXIT SIGNAL QUEUED FOR EXECUTION | "
                f"Strategy: {strategy_name} | "
                f"Symbol: {strategy_config.execution.product_id} | "
                f"Price: {payload.price} | "
                f"Timestamp: {webhook_timestamp} | "
                f"⚠️ CRITICAL: Exit signal must be processed"
            )
        
        # Queue background task with error handling wrapper
        async def execute_with_error_handling(payload, strategy_name):
            """Wrapper to ensure errors are logged and alerted"""
            try:
                await execute_trade_async(payload, strategy_name)
            except Exception as bg_error:
                logger.error(
                    f"❌ BACKGROUND TASK FAILED | "
                    f"Strategy: {strategy_name} | "
                    f"Side: {payload.side} | "
                    f"Error: {str(bg_error)}",
                    exc_info=True
                )
                # Send critical alert for background task failures
                if payload.is_exit():
                    try:
                        from services.telegram_notifier import telegram_notifier
                        await telegram_notifier.send_critical_failure(
                            f"🚨 CRITICAL: EXIT SIGNAL BACKGROUND TASK FAILED\n\n"
                            f"Strategy: {strategy_name}\n"
                            f"Symbol: {strategy_config.execution.product_id}\n"
                            f"Side: {payload.side}\n"
                            f"Price: {payload.price}\n"
                            f"Error: {str(bg_error)}\n\n"
                            f"⚠️ Exit signal was received but execution failed!\n"
                            f"⚠️ Position may still be open!\n\n"
                            f"Check Cloud Run logs for full stack trace."
                        )
                    except Exception as alert_error:
                        logger.error(f"Failed to send background task failure alert: {alert_error}")
        
        background_tasks.add_task(
            execute_with_error_handling,
            payload,
            strategy_name
        )
        
        # 7. RETURN SUCCESS IMMEDIATELY (fast response prevents TradingView timeout)
        # Response time should be < 10 seconds to avoid TradingView webhook timeout
        response_time = (datetime.utcnow() - datetime.fromisoformat(webhook_timestamp.replace('Z', '+00:00'))).total_seconds()
        logger.info(f"⚡ Webhook response time: {response_time:.3f}s")
        
        response = {
            "status": "accepted",
            "strategy": strategy_name,
            "strategy_id": payload.strategy_id,
            "action": payload.side,
            "symbol": strategy_config.execution.product_id,
            "timestamp": datetime.utcnow().isoformat(),
            "message": "Order queued for execution",
            "response_time_seconds": round(response_time, 3)
        }
        
        if payload.price:
            response['price'] = payload.price
        
        return response
        
    except HTTPException as http_ex:
        # Log HTTP exceptions (validation errors, etc.)
        logger.error(
            f"❌ WEBHOOK HTTP EXCEPTION | "
            f"Status: {http_ex.status_code} | "
            f"Detail: {http_ex.detail} | "
            f"Time: {webhook_timestamp}"
        )
        raise
    except Exception as e:
        # Log all other exceptions
        logger.error(
            f"❌ WEBHOOK UNHANDLED EXCEPTION | "
            f"Error: {str(e)} | "
            f"Time: {webhook_timestamp} | "
            f"IP: {client_ip}",
            exc_info=True
        )
        # Try to send alert for unhandled exceptions
        try:
            from services.telegram_notifier import telegram_notifier
            await telegram_notifier.send_critical_failure(
                f"🚨 WEBHOOK UNHANDLED EXCEPTION\n\n"
                f"Time: {webhook_timestamp}\n"
                f"IP: {client_ip}\n"
                f"Error: {str(e)}\n\n"
                f"⚠️ Webhook failed with unhandled exception.\n"
                f"Check Cloud Run logs for full stack trace."
            )
        except Exception as alert_error:
            logger.error(f"Failed to send webhook exception alert: {alert_error}")
        raise HTTPException(status_code=500, detail=str(e))


async def execute_trade_async(payload: WebhookPayload, strategy_name: str):
    """
    Execute trade asynchronously (background task - V3.5)
    
    This allows webhook to return fast while order executes
    """
    try:
        # Get strategy config for symbol
        strategy_config = get_strategy_by_id(config, payload.strategy_id)
        symbol = strategy_config.execution.product_id if strategy_config else "unknown"
        
        logger.info(f"Executing trade: {strategy_name} ({payload.strategy_id}) {payload.side} {symbol}")
        
        # For exit signals, log more details to help diagnose issues
        if payload.is_exit():
            logger.info(f"🔍 EXIT SIGNAL PROCESSING: strategy={payload.strategy_id}, symbol={symbol}, source={payload.source or 'TradingView webhook'}")
        
        # Execute via order executor
        result = await order_executor.execute(payload)
        
        logger.info(f"Trade executed: {result.status} - {result.symbol}")
        
        # For exit signals, log the result status to help diagnose issues
        if payload.is_exit():
            logger.info(f"🔍 EXIT SIGNAL RESULT: status={result.status}, symbol={result.symbol}, error={result.error or 'None'}")
        
        # Capture Historical Enhancer data with execution context (after execution)
        # This ensures we have both TradingView technical data AND execution details
        # For exits, also include peak profit from exit_monitor if available
        if payload.is_exit():
            # For exit events, try to get peak profit from exit_monitor if not in payload
            if payload.historical_enhancer is None:
                payload.historical_enhancer = {}
            
            if not isinstance(payload.historical_enhancer, dict):
                payload.historical_enhancer = {}
            
            # If peak profit not already in payload, try to get it from exit_monitor
            if 'peak_profit_pct' not in payload.historical_enhancer or payload.historical_enhancer.get('peak_profit_pct') is None:
                try:
                    from services.exit_monitor import exit_monitor
                    # Find the position that was just closed
                    symbol = result.symbol if result else payload.symbol
                    if symbol:
                        # Search for position in exit_monitor (may be in any partition)
                        for position_key, position in exit_monitor.monitored_positions.items():
                            if position.symbol == symbol and position.peak_profit_pct > 0:
                                payload.historical_enhancer['peak_profit_pct'] = position.peak_profit_pct
                                if position.peak_profit_time:
                                    payload.historical_enhancer['peak_profit_time'] = position.peak_profit_time.isoformat()
                                logger.debug(f"📈 Added peak profit {position.peak_profit_pct:.2f}% to exit Historical Enhancer for {symbol}")
                                break
                except Exception as e:
                    logger.debug(f"Could not retrieve peak profit from exit_monitor for exit: {e}")
        
        # Log Historical Enhancer data (for both entries and exits)
        if payload.historical_enhancer or (payload.is_exit() and payload.historical_enhancer):
            _append_historical_enhancer_event(payload, strategy_name, execution_result=result)
        
        # Log to P&L tracker
        try:
            if result.status == "success":
                # Get account mode from order executor
                account_mode = None
                if result.account_id:
                    account_config = order_executor._get_account_config(result.account_id)
                    if account_config:
                        account_mode = getattr(account_config, 'mode', None)
                
                if payload.is_entry():
                    # Opening position - log it
                    # Use capital_deployed from OrderResponse if available, otherwise calculate from notional/leverage
                    deployed_capital = result.capital_deployed
                    if deployed_capital is None and result.quantity and result.fill_price:
                        # Fallback: calculate from notional and leverage
                        notional_value = result.quantity * (result.fill_price or result.price or 0)
                        leverage = result.leverage or payload.leverage or 1
                        deployed_capital = notional_value / leverage if leverage > 0 else notional_value
                    
                    await pnl_tracker.log_trade_open(
                        strategy=strategy_name,
                        symbol=result.symbol,
                        side="LONG" if payload.is_long() else "SHORT",
                        quantity=result.quantity,
                        entry_price=result.fill_price or result.price or 0,
                        leverage=result.leverage or payload.leverage or 1,
                        broker=result.broker,
                        account=result.account_id,
                        account_mode=account_mode,
                        deployed_capital=deployed_capital,
                        tp_price=result.fill_price * (1 + payload.tp_pct / 100) if payload.tp_pct and result.fill_price else None,
                        sl_price=result.fill_price * (1 - abs(payload.sl_pct) / 100) if payload.sl_pct and result.fill_price else None
                    )
                    logger.info(f"P&L: Logged position open for {strategy_name} (account: {result.account_id}, mode: {account_mode})")
                
                elif payload.is_exit():
                    # Closing position - log it with commission and funding fees
                    # Get commission from OrderResponse (if available)
                    commission = result.commission if hasattr(result, 'commission') and result.commission is not None else None
                    
                    # Funding fee will be calculated in log_trade_close based on holding time
                    # In LIVE mode, could fetch from broker API in the future
                    funding_fee = None
                    
                    await pnl_tracker.log_trade_close(
                        strategy=strategy_name,
                        symbol=result.symbol,
                        exit_price=result.fill_price or result.price or 0,
                        account=result.account_id,
                        account_mode=account_mode,
                        commission=commission,
                        funding_fee=funding_fee
                    )
                    logger.info(f"P&L: Logged position close for {strategy_name} (account: {result.account_id}, mode: {account_mode})")
        
        except Exception as e:
            logger.error(f"Failed to log to P&L tracker: {str(e)}")
            # Don't fail the trade, just log the error
        
        # Telegram notifications are handled in order_executor
        # 
        # Note: We do NOT send duplicate alerts here for:
        # - "skipped" status: Already sent SIGNAL SKIPPED alert in order_executor
        # - "rejected" status: For exits, check if position exists - if so, alert user
        # - "failed" status with known errors: Already sent appropriate alerts in order_executor
        #
        # For exit signals that are rejected, check if there's actually an open position
        # This helps catch cases where exit signal doesn't match position state
        if payload.is_exit() and result.status == "rejected":
            # Check if there's actually an open position for this symbol
            # This could indicate a state mismatch or strategy_id mismatch
            try:
                from services.partition_manager import partition_manager
                all_positions = []
                for account_id, account_state in partition_manager.partition_state.items():
                    partitions = account_state.get('partitions', {})
                    for partition_id, partition_data in partitions.items():
                        open_positions = partition_data.get('open_positions', {})
                        for pos_symbol, position in open_positions.items():
                            # Normalize symbols for comparison
                            pos_symbol_norm = pos_symbol.upper().replace('-PERP', '').replace('-CBSE', '').replace('ETP', 'ETH').replace('BIP', 'BTC').replace('SLP', 'SOL').replace('XPP', 'XRP')
                            result_symbol_norm = (result.symbol or symbol or '').upper().replace('-PERP', '').replace('-CBSE', '').replace('ETP', 'ETH').replace('BIP', 'BTC').replace('SLP', 'SOL').replace('XPP', 'XRP')
                            if pos_symbol_norm == result_symbol_norm:
                                all_positions.append({
                                    'account_id': account_id,
                                    'partition_id': partition_id,
                                    'symbol': pos_symbol,
                                    'side': position.get('side'),
                                    'quantity': position.get('quantity')
                                })
                
                if all_positions:
                    # Exit was rejected but position exists - this is a problem!
                    from services.telegram_notifier import telegram_notifier
                    position_details = "\n".join([
                        f"  • {p['account_id']}/{p['partition_id']}: {p['side']} {p['symbol']} ({p['quantity']})"
                        for p in all_positions
                    ])
                    await telegram_notifier.send_critical_failure(
                        f"⚠️ EXIT SIGNAL REJECTED BUT POSITION EXISTS\n\n"
                        f"Strategy: {strategy_name}\n"
                        f"Symbol: {result.symbol or symbol}\n"
                        f"Exit Signal Status: {result.status.upper()}\n"
                        f"Error: {result.error or 'No error message'}\n\n"
                        f"Open Positions Found:\n{position_details}\n\n"
                        f"⚠️ This may indicate:\n"
                        f"• Strategy ID mismatch\n"
                        f"• Position state synchronization issue\n"
                        f"• Exit signal routing problem\n\n"
                        f"Check logs for details."
                    )
                    logger.error(
                        f"❌ EXIT SIGNAL REJECTED BUT POSITION EXISTS: "
                        f"strategy={payload.strategy_id}, symbol={result.symbol or symbol}, "
                        f"error={result.error}, positions={all_positions}"
                    )
            except Exception as check_error:
                logger.warning(f"Could not check for open positions after rejected exit: {check_error}")
        
        # Only send TRADE EXECUTION FAILED for actual unexpected broker failures
        # that weren't already alerted by order_executor
        if result.status == "failed" and result.error:
            # Check if this is an unexpected failure that needs alerting
            # Skip alerting for known/expected error patterns that are already handled
            known_errors = [
                "No partition positions to close",
                "insufficient available capital",
                "ADV cap prevented execution",
                "holiday_block_window",
                "cooperative-busy",
            ]
            is_known_error = any(known in (result.error or "") for known in known_errors)
            
            if not is_known_error:
                from services.telegram_notifier import telegram_notifier
                await telegram_notifier.send_critical_failure(
                    f"🚨 TRADE EXECUTION FAILED\n\n"
                    f"Strategy: {strategy_name}\n"
                    f"Symbol: {result.symbol}\n"
                    f"Action: {payload.side.upper()}\n"
                    f"Status: {result.status.upper()}\n"
                    f"Error: {result.error}\n\n"
                    f"⚠️ Check logs for details."
                )
        
        return result
        
    except Exception as e:
        logger.error(f"Trade execution failed: {str(e)}", exc_info=True)
        
        # Send Telegram alert for unhandled exceptions
        try:
            from services.telegram_notifier import telegram_notifier
            strategy_config = get_strategy_by_id(config, payload.strategy_id)
            symbol = strategy_config.execution.product_id if strategy_config else "unknown"
            
            await telegram_notifier.send_critical_failure(
                f"🚨 CRITICAL: TRADE EXECUTION EXCEPTION\n\n"
                f"Strategy: {strategy_name}\n"
                f"Symbol: {symbol}\n"
                f"Action: {payload.side.upper()}\n"
                f"Error: {str(e)}\n\n"
                f"⚠️ Execution failed with unhandled exception.\n"
                f"Check Cloud Run logs for full stack trace."
            )
        except Exception as telegram_error:
            logger.error(f"Failed to send Telegram alert: {str(telegram_error)}")
        
        # Re-raise to ensure it's logged
        raise


@app.get("/positions")
async def get_positions():
    """
    Get all open positions across all brokers
    """
    try:
        positions = await order_executor.get_all_positions()
        return {
            "status": "success",
            "positions": positions,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting positions: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stats")
async def get_stats():
    """
    Get trading statistics (today/week/month)
    """
    try:
        today_stats = await pnl_tracker.get_statistics('today')
        week_stats = await pnl_tracker.get_statistics('week')
        month_stats = await pnl_tracker.get_statistics('month')
        
        return {
            "status": "success",
            "today": today_stats,
            "week": week_stats,
            "month": month_stats,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting stats: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/pnl/open")
async def get_open_pnl():
    """
    Get current open P&L for all positions
    """
    try:
        open_positions = await pnl_tracker.get_open_positions_all()
        
        total_unrealized_pnl = sum(p.get('unrealized_pnl', 0) for p in open_positions)
        total_unrealized_pnl_pct = sum(p.get('unrealized_pnl_pct', 0) for p in open_positions)
        
        return {
            "status": "success",
            "total_positions": len(open_positions),
            "total_unrealized_pnl": total_unrealized_pnl,
            "total_unrealized_pnl_pct": total_unrealized_pnl_pct,
            "positions": open_positions,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting open P&L: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/scale-check")
async def scale_check():
    """
    Check if service can scale to zero (for Cloud Scheduler)
    
    Returns:
    - can_scale_to_zero: True if all positions are flat and no open orders
    - reason: Explanation of why scaling is/isn't allowed
    - open_positions_count: Number of open positions
    - is_weekend: True if it's Friday-Sunday
    """
    try:
        from zoneinfo import ZoneInfo
        
        # Check current time in ET
        et_tz = ZoneInfo('America/New_York')
        now_et = datetime.now(et_tz)
        weekday = now_et.weekday()  # Monday=0, Friday=4, Saturday=5, Sunday=6
        hour = now_et.hour
        
        # Weekend period for scale-to-zero: Friday 4am PT (7am ET) - Monday 1am PT (4am ET)
        # This matches the entry blocking schedule
        is_friday_after_4am_pt = (weekday == 4 and hour >= 7)  # Friday 4am PT = 7am ET
        is_saturday = weekday == 5  # Saturday
        is_sunday = weekday == 6  # Sunday
        is_monday_before_1am_pt = (weekday == 0 and hour < 4)  # Monday before 1am PT = 4am ET
        is_weekend_period = is_friday_after_4am_pt or is_saturday or is_sunday or is_monday_before_1am_pt
        
        # Get all open positions from partition manager (source of truth)
        # This is more accurate than pnl_tracker which may have stale Firestore data
        from services.partition_manager import partition_state
        open_positions_count = 0
        has_deployed_cash = False
        total_deployed = 0.0
        
        for account_id, account_data in partition_state.items():
            partitions = account_data.get('partitions', {})
            for partition_id, partition_data in partitions.items():
                # Count open positions from partition manager
                open_positions = partition_data.get('open_positions', {})
                open_positions_count += len(open_positions)
                
                # Check deployed cash (proxy for pending orders)
                deployed = partition_data.get('deployed_cash', 0.0) or 0.0
                if deployed > 0.01:  # More than 1 cent deployed
                    has_deployed_cash = True
                    total_deployed += deployed
        
        # Check if positions are flat
        positions_flat = open_positions_count == 0
        
        # Check if weekend entry blocking is enabled
        weekend_blocking_enabled = config.general.disable_entries_fri_sun
        
        # Determine if we can scale to zero
        can_scale = False
        reason = ""
        
        if not weekend_blocking_enabled:
            can_scale = False
            reason = "Weekend entry blocking disabled - scale-to-zero requires disable_entries_fri_sun: true"
        elif not is_weekend_period:
            can_scale = False
            reason = "Not weekend period (Mon-Thu) - service should remain active"
        elif not positions_flat:
            can_scale = False
            reason = f"Open positions detected ({open_positions_count} positions) - service stays active until all exits complete"
        elif has_deployed_cash:
            can_scale = False
            reason = f"Deployed capital detected (${total_deployed:,.2f}) - may have pending orders"
        else:
            can_scale = True
            reason = "All positions flat, no deployed capital, weekend period, entry blocking enabled - safe to scale to zero"
        
        return {
            "can_scale_to_zero": can_scale,
            "reason": reason,
            "open_positions_count": open_positions_count,
            "has_deployed_cash": has_deployed_cash,
            "total_deployed_cash": total_deployed,
            "is_weekend_period": is_weekend_period,
            "is_friday_after_4am_pt": is_friday_after_4am_pt,
            "is_saturday": is_saturday,
            "is_sunday": is_sunday,
            "is_monday_before_1am_pt": is_monday_before_1am_pt,
            "weekend_blocking_enabled": weekend_blocking_enabled,
            "day_of_week": now_et.strftime('%A'),
            "timestamp": datetime.utcnow().isoformat(),
            "timestamp_et": now_et.isoformat()
        }
    except Exception as e:
        logger.error(f"Error in scale-check: {str(e)}")
        # On error, be conservative - don't scale to zero
        return {
            "can_scale_to_zero": False,
            "reason": f"Error checking scale status: {str(e)}",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }


@app.get("/pnl/accounts")
async def get_account_stats():
    """
    Get P&L stats per account (all configured accounts)
    """
    try:
        account_stats = await pnl_tracker.get_account_stats()
        
        # Calculate totals
        total_pnl = sum(a['total_pnl'] for a in account_stats)
        total_trades = sum(a['total_trades'] for a in account_stats)
        partition_accounts_summary = order_executor.get_partition_overview()
        
        # Separate by mode
        demo_stats = [a for a in account_stats if a.get('account_mode', '').upper() == 'DEMO']
        live_stats = [a for a in account_stats if a.get('account_mode', '').upper() == 'LIVE']
        
        demo_total_pnl = sum(a['total_pnl'] for a in demo_stats)
        live_total_pnl = sum(a['total_pnl'] for a in live_stats)
        
        return {
            "status": "success",
            "total_pnl_all_accounts": total_pnl,
            "total_trades_all_accounts": total_trades,
            "demo": {
                "total_pnl": demo_total_pnl,
                "total_trades": sum(a['total_trades'] for a in demo_stats),
                "accounts": demo_stats
            },
            "live": {
                "total_pnl": live_total_pnl,
                "total_trades": sum(a['total_trades'] for a in live_stats),
                "accounts": live_stats
            },
            "accounts": account_stats,
            "partition_accounts": partition_accounts_summary,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting account stats: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/pnl/accounts/demo")
async def get_demo_account_stats():
    """
    Get P&L stats for DEMO accounts only
    """
    try:
        demo_stats = await pnl_tracker.get_demo_account_stats()
        total_pnl = sum(a['total_pnl'] for a in demo_stats)
        total_trades = sum(a['total_trades'] for a in demo_stats)
        
        return {
            "status": "success",
            "mode": "DEMO",
            "total_pnl": total_pnl,
            "total_trades": total_trades,
            "accounts": demo_stats,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting DEMO account stats: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/pnl/accounts/live")
async def get_live_account_stats():
    """
    Get P&L stats for LIVE accounts only
    """
    try:
        live_stats = await pnl_tracker.get_live_account_stats()
        total_pnl = sum(a['total_pnl'] for a in live_stats)
        total_trades = sum(a['total_trades'] for a in live_stats)
        
        return {
            "status": "success",
            "mode": "LIVE",
            "total_pnl": total_pnl,
            "total_trades": total_trades,
            "accounts": live_stats,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting LIVE account stats: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/accounts/{account_id}/balance")
async def get_account_balance_snapshot(account_id: str):
    """
    Return live broker balance information plus partition allocation snapshot.
    """
    try:
        snapshot = await order_executor.get_account_balance_snapshot(account_id)
        return {
            "status": "success",
            **snapshot,
            "timestamp": datetime.utcnow().isoformat()
        }
    except ValueError as ve:
        raise HTTPException(status_code=404, detail=str(ve))
    except Exception as e:
        logger.error(f"Error fetching account balance snapshot: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/pnl/best-worst")
async def get_best_worst_trades(days: int = 30):
    """
    Get best and worst trades from last N days
    
    Query params:
        days: Number of days to look back (default: 30)
    """
    try:
        result = await pnl_tracker.get_best_and_worst_trades(days=days)
        
        return {
            "status": "success",
            **result,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting best/worst trades: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/risk/liquidation")
async def get_liquidation_risk():
    """
    Get liquidation risk assessment for all open positions
    """
    try:
        # Get current prices for all open positions
        open_positions = await pnl_tracker.get_open_positions_all()
        
        current_prices = {}
        for pos in open_positions:
            # Get current price from broker
            symbol = pos['symbol']
            if symbol not in current_prices:
                # TODO: Fetch from broker
                current_prices[symbol] = pos.get('current_price', pos['entry_price'])
        
        risks = await pnl_tracker.get_liquidation_risk_all(current_prices)
        
        # Check if trading should be halted
        should_halt, halt_reason = await pnl_tracker.should_halt_trading(current_prices)
        
        return {
            "status": "success",
            "should_halt_trading": should_halt,
            "halt_reason": halt_reason,
            "positions_at_risk": len([r for r in risks if r['risk_level'] in ['CRITICAL', 'HIGH', 'MODERATE']]),
            "risks": risks,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting liquidation risk: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/kill-switch")
async def emergency_stop(request: Request):
    """
    EMERGENCY: Close all positions and stop trading
    
    Requires special admin secret
    """
    try:
        body = await request.json()
        
        # Verify admin secret (separate from webhook secret)
        if body.get('admin_secret') != settings.ADMIN_SECRET:
            raise HTTPException(status_code=401, detail="Unauthorized")
        
        logger.critical("KILL SWITCH ACTIVATED!")
        
        # Close all positions
        results = await order_executor.close_all_positions()
        
        # Disable trading (hard stop)
        await risk_manager.disable_trading(reason="TRADING DISABLED - Kill switch activated", critical=True)
        
        # TODO: Send emergency alert to all channels
        
        return {
            "status": "success",
            "message": "All positions closed, trading disabled",
            "results": results,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Kill switch error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


async def _handle_trading_toggle(request: Request, *, enable: bool):
    body = await request.json()
    if body.get('admin_secret') != settings.ADMIN_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    if enable:
        await risk_manager.enable_trading()
        order_executor.clear_commission_cap_violation()
        message = "Trading enabled"
        logger.info("Trading resumed via admin API")
    else:
        await risk_manager.disable_trading(reason="Trading paused via admin API", critical=False)
        message = "Trading disabled"
        logger.info("Trading paused via admin API")

    return {
        "status": "success",
        "message": message,
        "timestamp": datetime.utcnow().isoformat()
    }


@app.post("/trading/disable")
async def pause_trading(request: Request):
    """
    Pause trading without closing positions (admin only).
    """
    try:
        return await _handle_trading_toggle(request, enable=False)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Pause trading error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/trading/enable")
async def enable_trading(request: Request):
    """
    Enable trading after a manual pause (admin only).
    """
    try:
        return await _handle_trading_toggle(request, enable=True)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Enable trading error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/resume-trading")
async def resume_trading(request: Request):
    """
    Backwards-compatible alias for /trading/enable.
    """
    return await enable_trading(request)


# ============================================
# MANUAL BROKER RECONCILIATION (ADMIN)
# ============================================

@app.post("/reconcile/run")
async def reconcile_run(request: Request):
    """
    Manually trigger broker ↔ ledger reconciliation for all LIVE accounts.
    Requires admin_secret in the JSON body.
    """
    try:
        body = await request.json()
        if body.get('admin_secret') != settings.ADMIN_SECRET:
            raise HTTPException(status_code=401, detail="Unauthorized")
        await broker_reconciler.reconcile_all_accounts(scope="manual-api")
        return {
            "status": "ok",
            "message": "Reconciliation triggered",
            "timestamp": datetime.utcnow().isoformat()
        }
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Manual reconcile error: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))

# ============================================
# CONTRACT EXPIRY MONITORING ENDPOINTS
# ============================================

@app.get("/contract/status")
async def get_contract_status():
    """
    Get current status of all monitored contracts
    
    Returns expiry status, days remaining, and trading permissions
    """
    try:
        status = contract_monitor.get_status()
        return JSONResponse(content=status)
    
    except Exception as e:
        logger.error(f"Contract status error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/contract/check")
async def manual_contract_check():
    """
    Manually trigger contract expiry check
    
    Useful for testing or forcing immediate check
    """
    try:
        result = await contract_monitor.check_contracts()
        return JSONResponse(content=result)
    
    except Exception as e:
        logger.error(f"Manual contract check error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================
# DEMO ACCOUNT ENDPOINTS (V3.5)
# ============================================

@app.get("/demo/accounts")
async def get_all_demo_accounts():
    """
    Get all DEMO accounts with balances and stats
    
    Returns summary of all DEMO accounts
    """
    try:
        accounts = order_executor.get_all_demo_accounts()
        
        return {
            "status": "success",
            "total_accounts": len(accounts),
            "accounts": accounts,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting DEMO accounts: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/demo/balance/{account_id}")
async def get_demo_balance(account_id: str):
    """
    Get current DEMO account balance and detailed stats
    
    Args:
        account_id: Account ID (e.g., 'coinbase-usa-1x')
    
    Returns:
        {
            'account_id': str,
            'current_balance': float,
            'total_pnl': float,
            'total_pnl_pct': float,
            'total_trades': int,
            'win_rate': float,
            ...
        }
    """
    try:
        balance_info = order_executor.get_demo_account_balance(account_id)
        
        if 'error' in balance_info:
            raise HTTPException(status_code=404, detail=balance_info['error'])
        
        return {
            "status": "success",
            **balance_info,
            "timestamp": datetime.utcnow().isoformat()
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting DEMO balance: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/demo/positions/{account_id}")
async def get_demo_positions(account_id: str):
    """
    Get open DEMO positions for an account
    
    Args:
        account_id: Account ID
    
    Returns:
        {
            'account_id': str,
            'open_positions': [...],
            'total_positions': int
        }
    """
    try:
        positions = order_executor.get_demo_positions(account_id)
        
        return {
            "status": "success",
            **positions,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting DEMO positions: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/demo/history/{account_id}")
async def get_demo_history(account_id: str, limit: int = 50):
    """
    Get DEMO trade history for an account
    
    Args:
        account_id: Account ID
        limit: Max trades to return (default 50)
    
    Returns:
        {
            'account_id': str,
            'total_trades': int,
            'win_rate': float,
            'trades': [...]
        }
    """
    try:
        history = order_executor.get_demo_trade_history(account_id, limit)
        
        return {
            "status": "success",
            **history,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting DEMO history: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/eod/preview")
async def eod_preview(admin_secret: Optional[str] = Header(None)):
    """
    Send an immediate EOD preview to Telegram.
    Header: Admin-Secret: <settings.ADMIN_SECRET>
    """
    try:
        expected = settings.ADMIN_SECRET
        if not expected or admin_secret != expected:
            raise HTTPException(status_code=401, detail="Unauthorized")
        await daily_summary_scheduler.trigger_now()
        return {"status": "success", "message": "EOD preview sent"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error sending EOD preview: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/eod/status")
async def eod_status(request: Request, admin_secret: Optional[str] = Header(None)):
    """
    Check EOD report status for today.
    Query param: ?admin_secret=<settings.ADMIN_SECRET> (or header)
    """
    try:
        # Get admin_secret from query param or header
        admin_secret_param = request.query_params.get("admin_secret")
        admin_secret = admin_secret or admin_secret_param
        
        expected = settings.ADMIN_SECRET
        if not expected or admin_secret != expected:
            raise HTTPException(status_code=401, detail="Unauthorized")
        
        from zoneinfo import ZoneInfo
        et_tz = ZoneInfo('America/New_York')
        pt_tz = ZoneInfo('America/Los_Angeles')
        now_et = datetime.now(et_tz)
        now_pt = datetime.now(pt_tz)
        today = now_et.date()
        weekday = now_et.weekday()  # Monday=0, Friday=4, Saturday=5, Sunday=6
        
        # Check if EOD was sent today
        last_eod_sent = getattr(daily_summary_scheduler, '_last_eod_sent', None)
        
        # Check holiday status
        is_weekend = weekday >= 5
        is_holiday_block = False
        holiday_reason = ""
        
        if getattr(settings, "CALENDAR_ENABLED", False):
            try:
                from services.holiday_guard import is_prepost_holiday_block_now
                is_holiday_block = is_prepost_holiday_block_now()
                if is_holiday_block:
                    holiday_reason = "Pre/Post-holiday block window active"
            except Exception:
                pass
        
        status = {
            "date": today.isoformat(),
            "day_of_week": today.strftime('%A'),
            "is_weekend": is_weekend,
            "is_holiday_block": is_holiday_block,
            "holiday_reason": holiday_reason,
            "eod_sent_today": False,
            "last_eod_sent": None,
            "time_et": now_et.strftime('%Y-%m-%d %I:%M %p %Z'),
            "time_pt": now_pt.strftime('%Y-%m-%d %I:%M %p %Z'),
            "should_send": not is_weekend and not is_holiday_block
        }
        
        if last_eod_sent:
            last_eod_date = last_eod_sent.date()
            status["eod_sent_today"] = (last_eod_date == today)
            status["last_eod_sent"] = last_eod_sent.isoformat()
            status["last_eod_sent_date"] = last_eod_date.isoformat()
        
        return status
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting EOD status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/eod/trigger")
async def eod_trigger(request: Request):
    """
    Trigger EOD summary (for Cloud Scheduler).
    Query param: ?admin_secret=<settings.ADMIN_SECRET>
    
    Sends EOD if:
    - It's a weekday (Monday-Friday)
    - NOT in a holiday block window (if calendar.enabled)
    
    Skips EOD if:
    - It's a weekend (Saturday/Sunday)
    - Holiday guard is enabled and we're in a pre/post-holiday block window
    
    Note: Weekend entry blocking (Fri 4am PT - Mon 1am PT) does NOT block EOD.
    EOD will send on Friday 4pm PT even though new trade entries are blocked.
    """
    try:
        # Get admin_secret from query param (Cloud Scheduler can't send headers easily)
        admin_secret = request.query_params.get("admin_secret")
        
        expected = settings.ADMIN_SECRET
        if not expected or admin_secret != expected:
            raise HTTPException(status_code=401, detail="Unauthorized")
        
        # Check if we should skip EOD
        from zoneinfo import ZoneInfo
        
        et_tz = ZoneInfo('America/New_York')
        pt_tz = ZoneInfo('America/Los_Angeles')
        now_et = datetime.now(et_tz)
        now_pt = datetime.now(pt_tz)
        weekday = now_et.weekday()  # Monday=0, Friday=4, Saturday=5, Sunday=6
        
        # 1. Check if it's a weekend (Saturday or Sunday)
        # EOD should NOT send on weekends
        if weekday >= 5:  # Saturday or Sunday
            logger.info(f"📅 EOD skipped: Weekend ({now_pt.strftime('%A')})")
            return {
                "status": "skipped",
                "reason": f"Weekend ({now_pt.strftime('%A')})",
                "day": now_pt.strftime('%A'),
                "time_pt": now_pt.strftime('%I:%M %p %Z'),
                "timestamp": datetime.utcnow().isoformat()
            }
        
        # 2. Check holiday guard (uses existing holiday_guard service)
        # EOD should NOT send during pre/post-holiday block windows
        if getattr(settings, "CALENDAR_ENABLED", False):
            try:
                from services.holiday_guard import is_prepost_holiday_block_now
                if is_prepost_holiday_block_now():
                    logger.info(
                        f"📅 EOD skipped: Holiday block window active "
                        f"({settings.CALENDAR_TZ if hasattr(settings, 'CALENDAR_TZ') else 'ET'})"
                    )
                    return {
                        "status": "skipped",
                        "reason": f"Pre/Post-holiday block window active ({settings.CALENDAR_TZ if hasattr(settings, 'CALENDAR_TZ') else 'ET'})",
                        "timestamp": datetime.utcnow().isoformat()
                    }
            except Exception as hg_err:
                logger.debug(f"Holiday guard check failed (continuing anyway): {hg_err}")
        
        # Note: We do NOT check weekend entry blocking period (Fri 4am PT - Mon 1am PT)
        # because EOD should send on Friday 4pm PT even though new entries are blocked.
        # Weekend entry blocking is for trade entries, not for EOD reports.
        
        # All checks passed - send EOD
        logger.info("📊 EOD triggered by Cloud Scheduler")
        await daily_summary_scheduler.trigger_now()
        return {"status": "success", "message": "EOD summary sent"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error triggering EOD: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/demo/reset/{account_id}")
async def reset_demo_account(account_id: str):
    """
    Reset DEMO account to starting balance (from config.yaml)
    
    ⚠️ This will delete all trade history and positions!
    
    Args:
        account_id: Account ID to reset
    
    Returns:
        {
            'account_id': str,
            'balance': float,
            'status': str
        }
    """
    try:
        result = order_executor.reset_demo_account(account_id)
        
        if result.get('status') == 'failed':
            raise HTTPException(status_code=404, detail=result.get('error'))
        
        return {
            "status": "success",
            **result
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error resetting DEMO account: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# UNIFIED POSITION ENDPOINTS (V3.6.1)
# ============================================================================

@app.get("/positions/unified/{account_id}")
async def get_unified_positions(account_id: str):
    """
    Get unified positions for an account across all systems
    
    This endpoint provides a single source of truth for position data by
    reconciling partition positions, broker positions, and demo positions.
    
    Args:
        account_id: Account ID to get positions for
    
    Returns:
        {
            'account_id': str,
            'positions': List[UnifiedPosition],
            'total_positions': int,
            'total_deployed_capital': float,
            'last_updated': str,
            'last_reconciled': str
        }
    """
    try:
        from services.position_reconciler import position_reconciler
        
        # Get unified positions
        positions = await position_reconciler.get_unified_positions(account_id)
        
        # Get summary data
        summary = await position_reconciler.get_position_summary(account_id)
        
        # Get last reconcile time
        last_reconciled = position_reconciler.get_last_reconcile_time(account_id)
        
        return {
            "status": "success",
            "account_id": account_id,
            "positions": [
                {
                    "symbol": pos.symbol,
                    "side": pos.side,
                    "quantity": pos.quantity,
                    "entry_price": pos.entry_price,
                    "current_price": pos.current_price,
                    "unrealized_pnl_usd": pos.unrealized_pnl_usd,
                    "unrealized_pnl_pct": pos.unrealized_pnl_pct,
                    "deployed_capital": pos.deployed_capital,
                    "leverage": pos.leverage,
                    "partition_id": pos.partition_id,
                    "broker": pos.broker,
                    "entry_time": pos.entry_time.isoformat() if pos.entry_time else None,
                    "age_hours": pos.age_hours,
                    "last_updated": pos.last_updated.isoformat() if pos.last_updated else None
                }
                for pos in positions
            ],
            "summary": summary,
            "last_reconciled": last_reconciled.isoformat() if last_reconciled else None,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting unified positions for {account_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/positions/reconcile/{account_id}")
async def reconcile_account_positions(account_id: str, admin_secret: Optional[str] = Header(None)):
    """
    Force reconciliation of positions for a specific account
    
    Requires admin_secret header for security.
    
    Args:
        account_id: Account ID to reconcile
    
    Returns:
        ReconciliationResult with details of the reconciliation
    """
    try:
        expected = settings.ADMIN_SECRET
        if not expected or admin_secret != expected:
            raise HTTPException(status_code=401, detail="Unauthorized")
        
        from services.position_reconciler import position_reconciler
        
        # Force reconciliation
        result = await position_reconciler.reconcile_account_positions(account_id)
        
        return {
            "status": "success",
            "account_id": result.account_id,
            "total_positions": result.total_positions,
            "mismatches_found": result.mismatches_found,
            "mismatches_resolved": result.mismatches_resolved,
            "errors": result.errors,
            "last_reconciled": result.last_reconciled.isoformat(),
            "positions": [
                {
                    "symbol": pos.symbol,
                    "side": pos.side,
                    "quantity": pos.quantity,
                    "deployed_capital": pos.deployed_capital,
                    "partition_id": pos.partition_id,
                    "age_hours": pos.age_hours
                }
                for pos in result.positions
            ]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error reconciling positions for {account_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/positions/reconcile-all")
async def reconcile_all_positions(admin_secret: Optional[str] = Header(None)):
    """
    Force reconciliation of positions for all accounts
    
    Requires admin_secret header for security.
    
    Returns:
        Dict of reconciliation results by account_id
    """
    try:
        expected = settings.ADMIN_SECRET
        if not expected or admin_secret != expected:
            raise HTTPException(status_code=401, detail="Unauthorized")
        
        from services.position_reconciler import position_reconciler
        
        # Reconcile all accounts
        results = await position_reconciler.reconcile_all_accounts()
        
        return {
            "status": "success",
            "accounts_reconciled": len(results),
            "results": {
                account_id: {
                    "total_positions": result.total_positions,
                    "mismatches_found": result.mismatches_found,
                    "mismatches_resolved": result.mismatches_resolved,
                    "errors": result.errors,
                    "last_reconciled": result.last_reconciled.isoformat()
                }
                for account_id, result in results.items()
            },
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error reconciling all positions: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/positions/summary/{account_id}")
async def get_position_summary(account_id: str):
    """
    Get position summary with aggregated data
    
    Args:
        account_id: Account ID to get summary for
    
    Returns:
        Position summary with totals and breakdowns
    """
    try:
        from services.position_reconciler import position_reconciler
        
        summary = await position_reconciler.get_position_summary(account_id)
        
        return {
            "status": "success",
            **summary
        }
        
    except Exception as e:
        logger.error(f"Error getting position summary for {account_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/pnl/realtime/{account_id}")
async def get_realtime_pnl(account_id: str, force_refresh: bool = False):
    """
    Get real-time P&L for all open positions in an account
    
    Args:
        account_id: Account ID to get P&L for
        force_refresh: Skip cache and fetch fresh data
    
    Returns:
        Real-time P&L data with current market prices
    """
    try:
        from services.realtime_pnl import realtime_pnl
        
        # Calculate real-time P&L
        account_pnl = await realtime_pnl.calculate_account_pnl(account_id, force_refresh)
        
        return {
            "status": "success",
            "account_id": account_pnl.account_id,
            "summary": {
                "total_unrealized_pnl_usd": account_pnl.total_unrealized_pnl_usd,
                "total_deployed_capital": account_pnl.total_deployed_capital,
                "portfolio_pnl_pct": account_pnl.portfolio_pnl_pct,
                "total_positions": account_pnl.total_positions,
                "profitable_positions": account_pnl.profitable_positions,
                "losing_positions": account_pnl.losing_positions,
                "win_rate": account_pnl.win_rate,
                "largest_winner_usd": account_pnl.largest_winner_usd,
                "largest_loser_usd": account_pnl.largest_loser_usd
            },
            "positions": [
                {
                    "symbol": pos.symbol,
                    "side": pos.side,
                    "quantity": pos.quantity,
                    "entry_price": pos.entry_price,
                    "current_price": pos.current_price,
                    "unrealized_pnl_usd": pos.unrealized_pnl_usd,
                    "unrealized_pnl_pct": pos.unrealized_pnl_pct,
                    "deployed_capital": pos.deployed_capital,
                    "leverage": pos.leverage,
                    "partition_id": pos.partition_id,
                    "age_hours": pos.age_hours,
                    "is_profitable": pos.is_profitable,
                    "entry_time": pos.entry_time.isoformat() if pos.entry_time else None
                }
                for pos in account_pnl.positions
            ],
            "last_updated": account_pnl.last_updated.isoformat(),
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting real-time P&L for {account_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/pnl/alerts/{account_id}")
async def get_pnl_alerts(account_id: str, loss_threshold: float = 10.0):
    """
    Get P&L alerts for positions that need attention
    
    Args:
        account_id: Account ID to check
        loss_threshold: Alert threshold for losses (percentage)
    
    Returns:
        List of alerts for positions needing attention
    """
    try:
        from services.realtime_pnl import realtime_pnl
        
        alerts = await realtime_pnl.get_position_alerts(account_id, loss_threshold)
        
        return {
            "status": "success",
            "account_id": account_id,
            "alert_count": len(alerts),
            "alerts": alerts,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting P&L alerts for {account_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/monitoring/status")
async def get_monitoring_status():
    """
    Get position monitoring status
    
    Returns:
        Current status of the position monitoring service
    """
    try:
        status = position_monitor.get_monitoring_status()
        
        return {
            "status": "success",
            **status,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting monitoring status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/balance/sync/{account_id}")
async def sync_account_balance(account_id: str, admin_secret: Optional[str] = Header(None)):
    """
    Manually sync account balance between partition system and demo accounts
    
    This fixes balance display issues by recalculating real_account from partition totals.
    
    Args:
        account_id: Account ID to sync
    
    Returns:
        Updated balance information
    """
    try:
        expected = settings.ADMIN_SECRET
        if not expected or admin_secret != expected:
            raise HTTPException(status_code=401, detail="Unauthorized")
        
        from services.partition_manager import recalculate_real_account_from_partitions
        
        # Recalculate real_account from partition totals
        await recalculate_real_account_from_partitions(account_id)
        
        # Get updated balance info
        balance_info = order_executor.get_demo_account_balance(account_id)
        
        return {
            "status": "success",
            "message": f"Balance synced for {account_id}",
            "balance_info": balance_info,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error syncing balance for {account_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/partitions/{account_id}/worst-trades")
async def get_worst_trades(
    account_id: str,
    partition_id: Optional[str] = None,
    limit: int = 10,
    admin_secret: Optional[str] = Header(None)
):
    """
    Get worst trades for a partition or all partitions in an account.
    
    Args:
        account_id: Account ID
        partition_id: Optional partition ID (if not provided, returns worst trades across all partitions)
        limit: Max number of worst trades to return (default 10)
        admin_secret: Admin secret for authentication
    
    Returns:
        {
            'account_id': str,
            'partition_id': str or None,
            'worst_trades': [...],
            'max_drawdown_trade': {...} or None
        }
    """
    try:
        # Verify admin secret if provided
        if admin_secret and admin_secret != settings.ADMIN_SECRET:
            raise HTTPException(status_code=401, detail="Unauthorized")
        
        from services.partition_manager import partition_state
        
        if account_id not in partition_state:
            raise HTTPException(status_code=404, detail=f"Account {account_id} not found")
        
        state = partition_state[account_id]
        partitions = state.get('partitions', {})
        
        if partition_id:
            if partition_id not in partitions:
                raise HTTPException(status_code=404, detail=f"Partition {partition_id} not found")
            partitions_to_check = {partition_id: partitions[partition_id]}
        else:
            partitions_to_check = partitions
        
        all_worst_trades = []
        max_drawdown_trade_info = None
        
        for pid, partition in partitions_to_check.items():
            trade_history = partition.get('trade_history', [])
            
            # Get worst trades for this partition
            losing_trades = [t for t in trade_history if t.get('pnl_usd_net', 0) < 0]
            losing_trades.sort(key=lambda x: x.get('pnl_usd_net', 0))
            
            for trade in losing_trades[:limit]:
                trade['partition_id'] = pid
                all_worst_trades.append(trade)
            
            # Check if this partition has max drawdown trade info
            if partition.get('max_drawdown_trade_id') is not None:
                trade_id = partition['max_drawdown_trade_id']
                # Find the trade in history
                for trade in trade_history:
                    if trade.get('trade_id') == trade_id:
                        max_drawdown_trade_info = {
                            'partition_id': pid,
                            'trade_id': trade_id,
                            'symbol': partition.get('max_drawdown_trade_symbol'),
                            'pnl_usd': partition.get('max_drawdown_trade_pnl'),
                            'max_drawdown_usd': partition.get('max_drawdown_usd', 0),
                            'max_drawdown_pct': partition.get('max_drawdown_pct', 0),
                            'trade_details': trade
                        }
                        break
        
        # Sort all worst trades across partitions
        all_worst_trades.sort(key=lambda x: x.get('pnl_usd_net', 0))
        
        return {
            "status": "success",
            "account_id": account_id,
            "partition_id": partition_id,
            "worst_trades": all_worst_trades[:limit],
            "max_drawdown_trade": max_drawdown_trade_info,
            "timestamp": datetime.utcnow().isoformat()
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting worst trades: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/debug/firestore/{account_id}")
async def debug_firestore_data(account_id: str, admin_secret: Optional[str] = Header(None)):
    """Debug raw Firestore partition data to identify data inconsistencies"""
    try:
        expected = settings.ADMIN_SECRET
        if not expected or admin_secret != expected:
            raise HTTPException(status_code=401, detail="Unauthorized")
        
        from services.partition_manager import partition_state
        from services.state_store import state_store
        
        # Get raw partition state
        raw_state = partition_state.get(account_id, {})
        
        # Also get from Firestore directly
        firestore_state = await state_store.load_partition_state(account_id)
        
        debug_info = {
            "account_id": account_id,
            "in_memory_state": raw_state,
            "firestore_state": firestore_state,
            "partitions_count": len(raw_state.get('partitions', {})),
            "real_account": raw_state.get('real_account', {}),
            "positions_summary": {}
        }
        
        # Analyze positions in each partition
        partitions = raw_state.get('partitions', {})
        for partition_id, partition_data in partitions.items():
            open_positions = partition_data.get('open_positions', {})
            debug_info["positions_summary"][partition_id] = {
                "virtual_balance": partition_data.get('virtual_balance', 0.0),
                "deployed_cash": partition_data.get('deployed_cash', 0.0),
                "available_cash": partition_data.get('available_cash', 0.0),
                "open_positions_count": len(open_positions),
                "positions": {}
            }
            
            for symbol, position in open_positions.items():
                debug_info["positions_summary"][partition_id]["positions"][symbol] = {
                    "side": position.get('side'),
                    "quantity": position.get('quantity'),
                    "entry_price": position.get('entry_price'),
                    "cash_used": position.get('cash_used'),
                    "leverage": position.get('leverage'),
                    "legs_count": len(position.get('legs', [])),
                    "legs": position.get('legs', [])
                }
        
        return debug_info
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Debug firestore failed for {account_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/monitoring/start")
async def start_monitoring(admin_secret: Optional[str] = Header(None)):
    """
    Start position monitoring
    
    Requires admin_secret header for security.
    """
    try:
        expected = settings.ADMIN_SECRET
        if not expected or admin_secret != expected:
            raise HTTPException(status_code=401, detail="Unauthorized")
        
        if position_monitor.is_running:
            return {
                "status": "already_running",
                "message": "Position monitor is already running"
            }
        
        await position_monitor.start_monitoring()
        
        return {
            "status": "success",
            "message": "Position monitor started",
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error starting monitoring: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/monitoring/stop")
async def stop_monitoring(admin_secret: Optional[str] = Header(None)):
    """
    Stop position monitoring
    
    Requires admin_secret header for security.
    """
    try:
        expected = settings.ADMIN_SECRET
        if not expected or admin_secret != expected:
            raise HTTPException(status_code=401, detail="Unauthorized")
        
        if not position_monitor.is_running:
            return {
                "status": "already_stopped",
                "message": "Position monitor is not running"
            }
        
        await position_monitor.stop_monitoring()
        
        return {
            "status": "success",
            "message": "Position monitor stopped",
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error stopping monitoring: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/test/telegram-alerts")
async def test_telegram_alerts(admin_secret: Optional[str] = Header(None)):
    """
    Send test order execution and exit alerts to Telegram.
    Requires admin_secret header.
    """
    try:
        expected = settings.ADMIN_SECRET
        if not expected or admin_secret != expected:
            raise HTTPException(status_code=401, detail="Unauthorized")
        
        from services.telegram_notifier import telegram_notifier
        
        logger.info("Sending test ORDER EXECUTION alert (entry)...")
        
        # Test ORDER EXECUTION (Entry)
        await telegram_notifier.send_execution_alert(
            account_label="coinbase-main",
            action="BUY",
            symbol="ETH-PERP",
            quantity=1.0,
            price=3031.57,
            leverage=3,
            mode="DEMO",
            strategy="Easy Ichimoku ETH 5m",
            position_side="LONG",
            partition_id="partition_1",
            partition_allocation=33.33,
            capital_deployed=252.00,
            capital_capacity=333.30,
            adv_cap_note="ADV cap: 80% of 7-day average"
        )
        
        logger.info("Sending test POSITION CLOSED alert (exit)...")
        
        # Test POSITION CLOSED (Exit)
        await telegram_notifier.send_execution_alert(
            account_label="coinbase-main",
            action="EXIT",
            symbol="ETH-PERP",
            quantity=1.0,
            price=3050.00,
            leverage=3,
            mode="DEMO",
            strategy="Easy Ichimoku ETH 5m",
            position_side="LONG",
            partition_id="partition_1",
            partition_allocation=33.33,
            entry_price=3031.57,
            exit_price=3050.00,
            pnl_usd=18.43,
            pnl_pct=0.61,
            capital_deployed=252.00,
            capital_capacity=333.30
        )
        
        return {
            "status": "success",
            "message": "Test alerts sent to Telegram",
            "timestamp": datetime.utcnow().isoformat()
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error sending test alerts: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/test/holiday-status")
async def test_holiday_status():
    """
    Test holiday detection and get current holiday status.
    """
    try:
        from services.holiday_guard import get_current_holiday_info, is_prepost_holiday_block_now
        from config import settings
        
        # Get current holiday info
        is_holiday_period, holiday_name, holiday_type = get_current_holiday_info()
        is_blocked = is_prepost_holiday_block_now()
        
        return {
            "status": "success",
            "holiday_system": {
                "enabled": getattr(settings, "CALENDAR_ENABLED", False),
                "timezone": getattr(settings, "CALENDAR_TZ", "America/New_York"),
                "pre_hours": getattr(settings, "CALENDAR_PRE_HOURS", 24),
                "post_hours": getattr(settings, "CALENDAR_POST_HOURS", 24),
                "include_low_volume": getattr(settings, "CALENDAR_INCLUDE_LOW_VOLUME", True)
            },
            "current_status": {
                "is_holiday_period": is_holiday_period,
                "is_trading_blocked": is_blocked,
                "holiday_name": holiday_name,
                "holiday_type": holiday_type
            },
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Error checking holiday status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/test/holiday-alert")
async def test_holiday_alert(admin_secret: Optional[str] = Header(None)):
    """
    Send a test holiday alert to Telegram.
    Requires admin_secret header.
    """
    try:
        expected = settings.ADMIN_SECRET
        if not expected or admin_secret != expected:
            raise HTTPException(status_code=401, detail="Unauthorized")
        
        from services.telegram_notifier import telegram_notifier
        
        logger.info("Sending test holiday alert...")
        
        # Send test holiday alert
        success = await telegram_notifier.send_holiday_alert(
            holiday_name="Test Holiday",
            holiday_type="MARKET_CLOSED",
            account_label="TradingView Agent (Test)"
        )
        
        return {
            "status": "success" if success else "failed",
            "message": "Test holiday alert sent to Telegram" if success else "Failed to send holiday alert",
            "timestamp": datetime.utcnow().isoformat()
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error sending test holiday alert: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/margin-rates")
async def get_margin_rates(symbol: Optional[str] = None, account_id: Optional[str] = None):
    """
    Get cached margin rates for symbols
    
    Returns current cached margin rates (intraday and overnight) for all symbols or a specific symbol.
    Rates are automatically fetched and cached when needed (empty cache or expired).
    
    Args:
        symbol: Optional symbol filter (e.g., "BTC-PERP", "ETH-PERP", "SOL-PERP", "XRP-PERP")
        account_id: Optional account ID to filter by broker
    
    Returns:
        {
            "symbols": {
                "BTC-PERP": {
                    "LONG": {
                        "intraday": {
                            "rate_pct": float,
                            "margin_per_1_usd": float,
                            "source": str,
                            "cached_date": str,
                            "timestamp": str
                        },
                        "overnight": {
                            "rate_pct": float,
                            "margin_per_1_usd": float,
                            "source": str,
                            "cached_date": str,
                            "timestamp": str
                        }
                    },
                    "SHORT": {...}
                },
                ...
            },
            "current_period": str,  # "intraday" or "overnight"
            "current_time_et": str
        }
    """
    from zoneinfo import ZoneInfo
    from datetime import datetime
    
    try:
        et_tz = ZoneInfo('America/New_York')
        now_et = datetime.now(et_tz)
        hour = now_et.hour
        is_overnight = 16 <= hour < 18  # 4PM-6PM ET
        current_period = "overnight" if is_overnight else "intraday"
        
        # Get all Coinbase accounts or specific account
        accounts_to_check = []
        if account_id:
            account = order_executor.account_configs.get(account_id)
            if account:
                accounts_to_check.append((account_id, account))
        else:
            # Get all Coinbase accounts
            for acc_id, acc in order_executor.account_configs.items():
                if hasattr(acc, 'broker') and acc.broker == 'coinbase_futures':
                    accounts_to_check.append((acc_id, acc))
        
        if not accounts_to_check:
            return {
                "symbols": {},
                "current_period": current_period,
                "current_time_et": now_et.isoformat(),
                "message": "No Coinbase accounts found"
            }
        
        # Get broker instance (use first account's broker)
        account_id, account = accounts_to_check[0]
        broker_wrapper = order_executor.account_brokers.get(account_id)
        if not broker_wrapper or not hasattr(broker_wrapper, 'broker'):
            return {
                "symbols": {},
                "current_period": current_period,
                "current_time_et": now_et.isoformat(),
                "message": "Broker not initialized"
            }
        
        broker = broker_wrapper.broker
        if not hasattr(broker, 'get_margin_requirements'):
            return {
                "symbols": {},
                "current_period": current_period,
                "current_time_et": now_et.isoformat(),
                "message": "Broker does not support margin requirements"
            }
        
        # Symbol mapping
        symbol_map = {
            'BTC-PERP': ('ichimoku-coinbase-btc-5m', 0.01),
            'ETH-PERP': ('ichimoku-coinbase-eth-5m', 0.10),
            'SOL-PERP': ('ichimoku-coinbase-sol-5m', 5.0),
            'XRP-PERP': ('ichimoku-coinbase-xrp-5m', 500.0)
        }
        
        symbols_to_check = [symbol] if symbol else list(symbol_map.keys())
        result = {}
        
        for sym in symbols_to_check:
            if sym not in symbol_map:
                continue
            
            strategy_id, contract_size = symbol_map[sym]
            strategy_config = get_strategy_by_id(order_executor.config, strategy_id)
            leverage = 3  # Default leverage (could get from account config)
            
            # Get current price - try multiple methods
            current_price = 0.0
            price_source = "unknown"
            try:
                # Method 1: Try broker wrapper's get_current_price
                if hasattr(broker_wrapper, 'get_current_price'):
                    try:
                        current_price = await asyncio.to_thread(broker_wrapper.get_current_price, sym)
                        if current_price > 0:
                            price_source = "broker_wrapper"
                            logger.debug(f"✅ Got price for {sym} from broker_wrapper: ${current_price:,.2f}")
                    except Exception as e:
                        logger.debug(f"broker_wrapper.get_current_price failed for {sym}: {e}")
                
                # Method 2: If that failed, try broker's get_current_price directly
                if current_price == 0.0 and hasattr(broker, 'get_current_price'):
                    try:
                        current_price = await asyncio.to_thread(broker.get_current_price, sym)
                        if current_price > 0:
                            price_source = "broker"
                            logger.debug(f"✅ Got price for {sym} from broker: ${current_price:,.2f}")
                    except Exception as e:
                        logger.debug(f"broker.get_current_price failed for {sym}: {e}")
                
                # Method 3: Try fetching product directly from cache
                if current_price == 0.0 and hasattr(broker, '_get_product_for_symbol'):
                    try:
                        product = await asyncio.to_thread(broker._get_product_for_symbol, sym)
                        if product:
                            if isinstance(product, dict):
                                current_price = float(product.get('price') or product.get('mid_market_price') or 0)
                            else:
                                current_price = float(getattr(product, 'price', None) or getattr(product, 'mid_market_price', None) or 0)
                            if current_price > 0:
                                price_source = "product_cache"
                                logger.debug(f"✅ Got price for {sym} from product cache: ${current_price:,.2f}")
                    except Exception as e:
                        logger.debug(f"_get_product_for_symbol failed for {sym}: {e}")
                
                # Method 4: Try fetching directly from REST API
                if current_price == 0.0 and hasattr(broker, 'rest_client') and broker.rest_client:
                    try:
                        # Map symbol to product_id
                        product_id_map = {
                            'BTC-PERP': 'BIP-CBSE',
                            'ETH-PERP': 'ETP-CBSE',
                            'SOL-PERP': 'SLP-CBSE',
                            'XRP-PERP': 'XPP-CBSE'
                        }
                        product_id = product_id_map.get(sym)
                        if product_id:
                            response = await asyncio.to_thread(broker.rest_client.get_product, product_id)
                            p = getattr(response, 'product', None)
                            if p:
                                price = getattr(p, 'price', None) or getattr(p, 'mid_market_price', None)
                                if price:
                                    current_price = float(price)
                                    price_source = "rest_api"
                                    logger.debug(f"✅ Got price for {sym} from REST API: ${current_price:,.2f}")
                    except Exception as e:
                        logger.debug(f"REST API get_product failed for {sym}: {e}")
                
                # Method 5: Initialize Coinbase REST client and fetch price (works in DEMO mode)
                # PRIORITY: Use Coinbase REST API for accurate prices
                if current_price == 0.0:
                    try:
                        from coinbase.rest import RESTClient
                        import os
                        
                        # Get API keys from account config (Secret Manager in Cloud Run)
                        api_key = getattr(account, 'api_key', None)
                        api_secret = getattr(account, 'api_secret', None)
                        
                        # If not in account config, try environment variables
                        if not api_key:
                            api_key = os.getenv('COINBASE_API_KEY_1')
                        if not api_secret:
                            api_secret = os.getenv('COINBASE_PRIVATE_KEY_1')
                        
                        if api_key and api_secret:
                            # Map symbol to product_id
                            product_id_map = {
                                'BTC-PERP': 'BIP-CBSE',
                                'ETH-PERP': 'ETP-CBSE',
                                'SOL-PERP': 'SLP-CBSE',
                                'XRP-PERP': 'XPP-CBSE'
                            }
                            product_id = product_id_map.get(sym)
                            if product_id:
                                # Initialize REST client (works even in DEMO mode for price fetching)
                                # API secret may have escaped newlines
                                secret_clean = api_secret.replace("\\n", "\n") if isinstance(api_secret, str) else api_secret
                                price_client = RESTClient(api_key=api_key, api_secret=secret_clean)
                                response = await asyncio.to_thread(price_client.get_product, product_id)
                                p = getattr(response, 'product', None)
                                if p:
                                    # Try multiple price fields
                                    price = (
                                        getattr(p, 'price', None) or 
                                        getattr(p, 'mid_market_price', None) or
                                        getattr(p, 'last_price', None) or
                                        getattr(p, 'mark_price', None)
                                    )
                                    if price:
                                        current_price = float(price)
                                        price_source = "coinbase_rest_api"
                                        logger.info(f"✅ Got price for {sym} from Coinbase REST API: ${current_price:,.2f}")
                    except Exception as e:
                        logger.debug(f"Coinbase REST API failed for {sym}: {e}")
                
                # Method 6: Fallback to yfinance (reliable backup for price data)
                if current_price == 0.0:
                    try:
                        import yfinance as yf
                        # Map to yfinance tickers
                        yf_ticker_map = {
                            'BTC-PERP': 'BTC-USD',
                            'ETH-PERP': 'ETH-USD',
                            'SOL-PERP': 'SOL-USD',
                            'XRP-PERP': 'XRP-USD'
                        }
                        yf_ticker = yf_ticker_map.get(sym)
                        if yf_ticker:
                            ticker = yf.Ticker(yf_ticker)
                            # Try fast_info first (real-time), then history as fallback
                            try:
                                fast_info = await asyncio.to_thread(getattr, ticker, 'fast_info', None)
                                if fast_info and hasattr(fast_info, 'lastPrice'):
                                    current_price = float(fast_info.lastPrice)
                                    price_source = "yfinance_fast"
                                    logger.info(f"✅ Got price for {sym} from yfinance (fast): ${current_price:,.2f}")
                                else:
                                    # Fallback to history
                                    info = await asyncio.to_thread(ticker.history, period='1d', interval='1m')
                                    if not info.empty:
                                        current_price = float(info['Close'].iloc[-1])
                                        price_source = "yfinance"
                                        logger.info(f"✅ Got price for {sym} from yfinance: ${current_price:,.2f}")
                            except Exception:
                                # Final fallback to history
                                info = await asyncio.to_thread(ticker.history, period='1d', interval='1m')
                                if not info.empty:
                                    current_price = float(info['Close'].iloc[-1])
                                    price_source = "yfinance"
                                    logger.info(f"✅ Got price for {sym} from yfinance: ${current_price:,.2f}")
                    except Exception as e:
                        logger.debug(f"yfinance failed for {sym}: {e}")
                
                # Method 7: Final fallback to Coinbase Public API (no auth required)
                if current_price == 0.0:
                    try:
                        import requests
                        # Map to spot symbols for public API
                        spot_symbol_map = {
                            'BTC-PERP': 'BTC',
                            'ETH-PERP': 'ETH',
                            'SOL-PERP': 'SOL',
                            'XRP-PERP': 'XRP'
                        }
                        currency = spot_symbol_map.get(sym)
                        if currency:
                            # Use Coinbase public API
                            url = f"https://api.coinbase.com/v2/exchange-rates?currency={currency}"
                            response = requests.get(url, timeout=5)
                            if response.status_code == 200:
                                data = response.json()
                                rates = data.get('data', {}).get('rates', {})
                                usd_rate = rates.get('USD')
                                if usd_rate:
                                    current_price = float(usd_rate)
                                    price_source = "coinbase_public_api"
                                    logger.info(f"✅ Got price for {sym} from Coinbase public API: ${current_price:,.2f}")
                    except Exception as e:
                        logger.debug(f"Coinbase public API failed for {sym}: {e}")
                
                if current_price == 0.0:
                    logger.warning(f"⚠️ Could not fetch price for {sym} - margin_per_1_usd will be 0.0")
            except Exception as e:
                logger.warning(f"⚠️ Error fetching price for {sym}: {e}")
                current_price = 0.0
            
            symbol_data = {}
            
            # Get rates for both LONG and SHORT
            for side in ['LONG', 'SHORT']:
                side_data = {}
                
                # Get intraday rate
                try:
                    intraday_req = broker.get_margin_requirements(sym, side, force_period='intraday')
                    intraday_rate = intraday_req.get('daytime_rate', intraday_req.get('current_rate', 0))
                    margin_per_1_intraday = (current_price * contract_size * intraday_rate) / leverage if current_price > 0 else 0.0
                    
                    side_data['intraday'] = {
                        "rate_pct": intraday_rate * 100,
                        "margin_per_1_usd": margin_per_1_intraday,
                        "source": intraday_req.get('source', 'unknown'),
                        "cached_date": broker._margin_cache_date.get(f"{broker._contract_code_from_symbol(sym)}_{side}_intraday", ''),
                        "timestamp": intraday_req.get('timestamp', '')
                    }
                except Exception as e:
                    side_data['intraday'] = {
                        "rate_pct": 0.0,
                        "margin_per_1_usd": 0.0,
                        "source": "error",
                        "error": str(e),
                        "cached_date": "",
                        "timestamp": ""
                    }
                
                # Get overnight rate
                try:
                    overnight_req = broker.get_margin_requirements(sym, side, force_period='overnight')
                    overnight_rate = overnight_req.get('overnight_rate', overnight_req.get('current_rate', 0))
                    margin_per_1_overnight = (current_price * contract_size * overnight_rate) / leverage if current_price > 0 else 0.0
                    
                    side_data['overnight'] = {
                        "rate_pct": overnight_rate * 100,
                        "margin_per_1_usd": margin_per_1_overnight,
                        "source": overnight_req.get('source', 'unknown'),
                        "cached_date": broker._margin_cache_date.get(f"{broker._contract_code_from_symbol(sym)}_{side}_overnight", ''),
                        "timestamp": overnight_req.get('timestamp', '')
                    }
                except Exception as e:
                    side_data['overnight'] = {
                        "rate_pct": 0.0,
                        "margin_per_1_usd": 0.0,
                        "source": "error",
                        "error": str(e),
                        "cached_date": "",
                        "timestamp": ""
                    }
                
                symbol_data[side] = side_data
            
            result[sym] = symbol_data
        
        return {
            "symbols": result,
            "current_period": current_period,
            "current_time_et": now_et.isoformat(),
            "current_time_et_formatted": now_et.strftime('%Y-%m-%d %H:%M:%S %Z')
        }
    
    except Exception as e:
        logger.error(f"Error getting margin rates: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================
# APPLICATION STARTUP
# ============================================

@app.on_event("startup")
async def startup_event():
    """
    Run on application startup
    Start background tasks
    """
    import asyncio
    
    logger.info("🚀 TradingView Agent starting up...")
    
    # Start contract monitoring in background
    asyncio.create_task(start_contract_monitoring())
    logger.info("✅ Contract monitoring task started")
    
    # Run initial contract check
    try:
        initial_check = await contract_monitor.check_contracts()
        logger.info(f"Initial contract check: {initial_check['status_breakdown']}")
    except Exception as e:
        logger.warning(f"Initial contract check failed: {str(e)}")
    
    # Start daily summary scheduler only if not using Cloud Scheduler
    # Set DISABLE_INTERNAL_EOD_SCHEDULER=true to prevent duplicate EOD reports
    disable_internal_eod = os.getenv('DISABLE_INTERNAL_EOD_SCHEDULER', 'false').lower() == 'true'
    if not disable_internal_eod:
        await daily_summary_scheduler.start()
        logger.info("✅ Internal EOD scheduler started")
    else:
        logger.info("⏭️ Internal EOD scheduler disabled (using Cloud Scheduler)")
    
    # Start afterhours position checker (if enabled)
    coinbase_config = getattr(config, 'coinbase_futures', None)
    if coinbase_config and coinbase_config.auto_close_trades_oversized_for_afterhours_at_end_of_intraday:
        await afterhours_position_checker.start()
        logger.info("✅ Afterhours position checker started (runs at 3:55PM ET daily)")
    else:
        logger.info("ℹ️ Afterhours position checker disabled in config")

    try:
        await order_executor.warm_partitions()
        logger.info("✅ Partition state restored from persistence")
        
        # Reconcile P&L tracker with partition state to remove stale positions
        try:
            await pnl_tracker.reconcile_with_partition_state()
            logger.info("✅ P&L tracker reconciled with partition state")
        except Exception as recon_exc:
            logger.warning(f"⚠️ P&L tracker reconciliation failed: {recon_exc}")
        
        await broker_reconciler.start_watchdog()
        logger.info("✅ Broker reconciliation watchdog started")
    except Exception as exc:
        logger.warning(f"⚠️ Partition warm-up or reconciliation failed: {exc}")

    try:
        await broker_reconciler.reconcile_all_accounts(scope="startup")
    except Exception as exc:
        logger.warning(f"⚠️ Startup reconciliation failed: {exc}")
    
    # Start periodic margin telemetry updates
    try:
        await broker_reconciler.start_telemetry_watchdog()
        logger.info("✅ Margin telemetry watchdog started")
    except Exception as exc:
        logger.warning(f"⚠️ Telemetry watchdog startup failed: {exc}")

    await broker_reconciler.start_watchdog()
    
    # Pre-populate margin cache for all Coinbase symbols
    try:
        await _pre_populate_margin_cache()
        logger.info("✅ Margin rates cache pre-populated")
    except Exception as exc:
        logger.warning(f"⚠️ Margin cache pre-population failed: {exc}")
    
    # Start position monitoring
    try:
        await position_monitor.start_monitoring()
        logger.info("✅ Position monitor started")
    except Exception as exc:
        logger.warning(f"⚠️ Position monitor startup failed: {exc}")
    
    # Start Agent-level exit monitoring (30-second position checks)
    try:
        await exit_monitor.start_monitoring()
        logger.info("✅ Agent-level exit monitor started (30-second checks)")
    except Exception as exc:
        logger.warning(f"⚠️ Exit monitor startup failed: {exc}")
    
    logger.info("✅ TradingView Agent ready!")


async def _pre_populate_margin_cache():
    """
    Pre-populate margin cache for all Coinbase symbols on startup.
    Ensures rates are available for EOD reporting even if no trades executed.
    """
    import asyncio
    from zoneinfo import ZoneInfo
    from datetime import datetime
    
    logger.info("📊 Pre-populating margin rates cache for all symbols...")
    
    # Get all Coinbase accounts
    coinbase_accounts = []
    for acc_id, acc in order_executor.account_configs.items():
        if hasattr(acc, 'broker') and acc.broker == 'coinbase_futures':
            coinbase_accounts.append((acc_id, acc))
    
    if not coinbase_accounts:
        logger.debug("No Coinbase accounts found for margin cache pre-population")
        return
    
    # Use first Coinbase account's broker
    account_id, account = coinbase_accounts[0]
    broker_wrapper = order_executor.account_brokers.get(account_id)
    if not broker_wrapper or not hasattr(broker_wrapper, 'broker'):
        logger.debug(f"Broker not initialized for {account_id}")
        return
    
    broker = broker_wrapper.broker
    if not hasattr(broker, 'ensure_daily_margin_rates'):
        logger.debug("Broker does not support margin rates")
        return
    
    # Symbols to pre-populate
    symbols = ['BTC-PERP', 'ETH-PERP', 'SOL-PERP', 'XRP-PERP']
    sides = ['LONG', 'SHORT']
    
    # Check current period
    et_tz = ZoneInfo('America/New_York')
    now_et = datetime.now(et_tz)
    hour = now_et.hour
    is_overnight = 16 <= hour < 18  # 4PM-6PM ET
    
    # Pre-populate rates for all symbols and sides
    # This ensures rates are cached even if no trades execute
    # CRITICAL: Fetch from API on initialization to ensure we have real rates, not just indicative
    for symbol in symbols:
        for side in sides:
            try:
                # Fetch fresh rates from API on initialization (force_refresh=True)
                # This ensures we get real API rates, not just indicative rates
                # The rates will be cached for the day and used for EOD reporting
                broker.ensure_daily_margin_rates(symbol, side, force_refresh=True)
                logger.info(f"✅ Margin rates fetched from API and cached for {symbol} {side}")
            except Exception as e:
                logger.warning(f"Failed to pre-populate margin rates for {symbol} {side}: {e}")
    
    logger.info(f"✅ Margin cache pre-population complete for {len(symbols)} symbols")


@app.on_event("shutdown")
async def shutdown_event():
    await daily_summary_scheduler.stop()
    await broker_reconciler.stop_watchdog()
    await broker_reconciler.stop_telemetry_watchdog()
    await position_monitor.stop_monitoring()
    await exit_monitor.stop_monitoring()


# Run application
if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))  # Cloud Run uses PORT env var
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        log_level="info",
        access_log=True
    )

