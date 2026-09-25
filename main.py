import asyncio
from datetime import datetime, timedelta
import hashlib
import http.server
import os
import sqlite3
import threading
import urllib.parse
import flet as ft
from lightweight_charts import Chart
import pandas as pd
import requests
import ta
import yfinance as yf

# ============================================================
# DATABASE INITIALIZATION & PERSISTENCE (SQLite)
# ============================================================
DB_NAME = "everybody_can_trade.db"

def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS trade_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            time TEXT,
            symbol TEXT,
            option_type TEXT,
            entry_price REAL,
            exit_price REAL,
            qty INTEGER,
            realized_pnl REAL,
            reason TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS open_positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            option_type TEXT,
            spot_at_entry REAL,
            entry_price REAL,
            current_price REAL,
            stop_loss REAL,
            target_price REAL,
            qty INTEGER,
            gtt_id TEXT
        )
    """)
    try:
        cursor.execute("ALTER TABLE open_positions ADD COLUMN gtt_id TEXT;")
    except sqlite3.OperationalError:
        pass
        
    conn.commit()
    conn.close()

init_db()

def db_save_open_position(pos):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO open_positions (symbol, option_type, spot_at_entry, entry_price, current_price, stop_loss, target_price, qty, gtt_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (pos["symbol"], pos["option_type"], pos["spot_at_entry"], pos["entry_price"], pos["current_price"], pos["stop_loss"], pos["target_price"], pos["qty"], pos.get("gtt_id")))
    conn.commit()
    conn.close()

def db_update_position_sl(symbol, option_type, new_sl):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE open_positions SET stop_loss = ? WHERE symbol = ? AND option_type = ?
    """, (new_sl, symbol, option_type))
    conn.commit()
    conn.close()

def db_remove_open_position(symbol, option_type):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM open_positions WHERE symbol = ? AND option_type = ?", (symbol, option_type))
    conn.commit()
    conn.close()

def db_load_open_positions():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT symbol, option_type, spot_at_entry, entry_price, current_price, stop_loss, target_price, qty, gtt_id FROM open_positions")
    rows = cursor.fetchall()
    conn.close()
    positions = []
    for r in rows:
        positions.append({
            "symbol": r[0], "option_type": r[1], "spot_at_entry": r[2],
            "entry_price": r[3], "current_price": r[4], "stop_loss": r[5],
            "target_price": r[6], "qty": r[7], "gtt_id": r[8]
        })
    return positions

def db_log_closed_trade(trade):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO trade_history (time, symbol, option_type, entry_price, exit_price, qty, realized_pnl, reason)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (trade["time"], trade["symbol"], trade["option_type"], trade["entry_price"], trade["exit_price"], trade["qty"], trade["realized_pnl"], trade.get("reason", "Manual")))
    conn.commit()
    conn.close()

def db_load_trade_history():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT time, symbol, option_type, entry_price, exit_price, qty, realized_pnl, reason FROM trade_history")
    rows = cursor.fetchall()
    conn.close()
    history = []
    for r in rows:
        history.append({
            "time": r[0], "symbol": r[1], "option_type": r[2],
            "entry_price": r[3], "exit_price": r[4], "qty": r[5],
            "realized_pnl": r[6], "reason": r[7]
        })
    return history

# ============================================================
# GLOBAL OAUTH SERVER FOR REDIRECTS
# ============================================================
oauth_callback_store = {"request_token": None}

class OAuthRedirectHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        parsed_url = urllib.parse.urlparse(self.path)
        query_params = urllib.parse.parse_qs(parsed_url.query)
        
        token_key = "request_token" if "request_token" in query_params else "code"
        if token_key in query_params:
            oauth_callback_store["request_token"] = query_params[token_key][0]
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(b"<h3>Authentication Successful! You can close this tab and return to market workspace.</h3>")
        else:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"<h3>Authorization failed or missing token.</h3>")

    def log_message(self, format, *args):
        pass

def start_local_server():
    try:
        server = http.server.HTTPServer(("127.0.0.1", 8000), OAuthRedirectHandler)
        server.timeout = 1.0
        while oauth_callback_store["request_token"] is None:
            server.handle_request()
        server.server_close()
    except Exception:
        pass

# ============================================================
# NSE LOT SIZES & TECHNICAL INDICATORS
# ============================================================
NSE_LOT_SIZES = {
    "RELIANCE.NS": 250, "TCS.NS": 175, "INFY.NS": 400,
    "HDFCBANK.NS": 550, "SBIN.NS": 750, "ICICIBANK.NS": 700,
    "AXISBANK.NS": 625, "KOTAKBANK.NS": 400, "LT.NS": 150, "ITC.NS": 1600,
    "BHARTIARTL.NS": 475, "HINDUNILVR.NS": 300, "BAJFINANCE.NS": 125,
    "TATAMOTORS.NS": 550, "TATASTEEL.NS": 2750, "SUNPHARMA.NS": 350,
    "WIPRO.NS": 1500, "MARUTI.NS": 50, "TITAN.NS": 175, "NTPC.NS": 1500,
    "POWERGRID.NS": 1800, "ASIANPAINT.NS": 200, "ULTRACEMCO.NS": 100,
    "BAJAJFINSV.NS": 500, "M&M.NS": 350, "HCLTECH.NS": 350, "ONGC.NS": 2500,
    "COALINDIA.NS": 1400, "BOSCHLTD.NS": 100, "BSOFT.NS": 1400,
    "DLF.NS": 750, "LAURUSLABS.NS": 800, "LICHSGFIN.NS": 1000, "IDFCFIRSTB.NS": 5000,
    "AARTIIND.NS": 1000, "DIVISLAB.NS": 200, "LALPATHLAB.NS": 300,
    "GRASIM.NS": 100, "ZYDUSLIFE.NS": 100
}

def get_lot_size(symbol: str) -> int:
    clean = str(symbol or "").upper()
    if not clean.endswith(".NS"):
        clean += ".NS"
    return NSE_LOT_SIZES.get(clean, 500)

def round_to_tick_size(price: float, tick_size: float = 0.10) -> float:
    return round(round(price / tick_size) * tick_size, 2)

def calculate_supertrend(df: pd.DataFrame, period: int = 10, multiplier: float = 3.0):
    high, low, close = df["High"].squeeze(), df["Low"].squeeze(), df["Close"].squeeze()
    atr = ta.volatility.AverageTrueRange(high=high, low=low, close=close, window=period).average_true_range()
    hl2 = (high + low) / 2
    upper_band, lower_band = (hl2 + (multiplier * atr)).to_numpy(), (hl2 - (multiplier * atr)).to_numpy()
    close_vals = close.to_numpy()

    first_valid = atr.first_valid_index()
    if first_valid is None:
        return pd.Series(index=df.index, dtype="float64"), pd.Series(index=df.index, dtype="int64")

    first_position = df.index.get_loc(first_valid)
    final_upper, final_lower = upper_band.copy(), lower_band.copy()
    direction, supertrend = [0] * len(df), [0.0] * len(df)

    direction[first_position] = 1
    supertrend[first_position] = lower_band[first_position]

    for i in range(first_position + 1, len(df)):
        final_upper[i] = upper_band[i] if upper_band[i] < final_upper[i - 1] or close_vals[i - 1] > final_upper[i - 1] else final_upper[i - 1]
        final_lower[i] = lower_band[i] if lower_band[i] > final_lower[i - 1] or close_vals[i - 1] < final_lower[i - 1] else final_lower[i - 1]
        direction[i] = (1 if close_vals[i] > final_upper[i] else -1) if direction[i - 1] == -1 else (-1 if close_vals[i] < final_lower[i] else 1)
        supertrend[i] = final_lower[i] if direction[i] == 1 else final_upper[i]

    return pd.Series(supertrend, index=df.index), pd.Series(direction, index=df.index)

def process_indicators(df: pd.DataFrame, params: dict):
    df = df[["Open", "High", "Low", "Close", "Volume"]].dropna()
    close_s, high_s, low_s = df["Close"].squeeze(), df["High"].squeeze(), df["Low"].squeeze()

    df["MA_FAST"] = close_s.rolling(window=params.get("ma_fast", 9)).mean()
    df["MA_SLOW"] = close_s.rolling(window=params.get("ma_slow", 21)).mean()
    df["EMA_FAST"] = close_s.ewm(span=params.get("ema_fast", 9), adjust=False).mean()
    df["EMA_SLOW"] = close_s.ewm(span=params.get("ema_slow", 21), adjust=False).mean()
    df["RSI"] = ta.momentum.RSIIndicator(close=close_s, window=params.get("rsi_period", 14)).rsi()
    df["ATR"] = ta.volatility.AverageTrueRange(high=high_s, low=low_s, close=close_s, window=params.get("atr_period", 14)).average_true_range()
    
    macd_ind = ta.trend.MACD(close=close_s, window_fast=params.get("macd_fast", 12), window_slow=params.get("macd_slow", 26))
    df["MACD"], df["MACD_SIGNAL"] = macd_ind.macd(), macd_ind.macd_signal()

    bb_ind = ta.volatility.BollingerBands(close=close_s, window=params.get("bb_window", 20), window_dev=params.get("bb_std", 2.0))
    df["BB_HIGH"], df["BB_LOW"] = bb_ind.bollinger_hband(), bb_ind.bollinger_lband()
    df["SUPERTREND"], df["ST_DIRECTION"] = calculate_supertrend(df, period=params.get("st_period", 10), multiplier=float(params.get("st_mult", 3.0)))
    return df.dropna()

# ============================================================
# DIRECT BROKER CANDLE FETCHING ENGINE (Zerodha Native + Fallback)
# ============================================================
broker_instrument_cache = {"provider": None, "data": {}}

def load_broker_instruments(broker_name, api_key, session_token):
    if broker_instrument_cache["provider"] == broker_name and broker_instrument_cache["data"]:
        return broker_instrument_cache["data"]
    
    mapping = {}
    try:
        if broker_name == "Zerodha Kite":
            res = requests.get("https://api.kite.trade/instruments", timeout=10)
            if res.status_code == 200:
                lines = res.text.splitlines()
                for line in lines[1:]:
                    parts = line.split(",")
                    if len(parts) > 2:
                        tradingsymbol = parts[2].strip('"')
                        instrument_token = parts[0].strip('"')
                        exchange = parts[1].strip('"')
                        if exchange in ["NSE", "NFO"]:
                            mapping[f"{tradingsymbol}.NS" if exchange == "NSE" else tradingsymbol] = instrument_token
    except Exception as e:
        print(f"Error loading instrument master: {e}")
        
    broker_instrument_cache["provider"] = broker_name
    broker_instrument_cache["data"] = mapping
    return mapping

def fetch_broker_candles(broker_session, symbol, period="30d", interval="5minute"):
    broker_name = broker_session.get("broker")
    api_key = broker_session.get("api_key")
    session_token = broker_session.get("session_token")
    clean_symbol = symbol.replace(".NS", "")
    
    if broker_name == "Zerodha Kite" and session_token:
        try:
            instruments = load_broker_instruments(broker_name, api_key, session_token)
            instrument_token = instruments.get(symbol) or instruments.get(clean_symbol)
            
            if instrument_token:
                to_date = datetime.now()
                from_date = to_date - timedelta(days=30 if "mo" in period or "d" in period else 365)
                
                headers = {
                    "X-Kite-Version": "3",
                    "Authorization": f"token {api_key}:{session_token}"
                }
                interval_map = {"5m": "5minute", "15m": "15minute", "60m": "60minute", "1d": "day"}
                kite_interval = interval_map.get(interval, "5minute")
                
                url = f"https://api.kite.trade/historical/{instrument_token}/{kite_interval}?from={from_date.strftime('%Y-%m-%d')}&to={to_date.strftime('%Y-%m-%d')}"
                res = requests.get(url, headers=headers, timeout=5)
                res_json = res.json()
                
                if res.status_code == 200 and res_json.get("status") == "success":
                    candles = res_json.get("data", {}).get("candles", [])
                    if candles:
                        df = pd.DataFrame(candles, columns=["Datetime", "Open", "High", "Low", "Close", "Volume", "OI"])
                        df["Datetime"] = pd.to_datetime(df["Datetime"])
                        df.set_index("Datetime", inplace=True)
                        return df[["Open", "High", "Low", "Close", "Volume"]]
        except Exception as e:
            print(f"Zerodha Native Candle Error: {e}")

    # Fallback to yfinance if broker session is paper trading or native fetch fails
    yf_sym = symbol if str(symbol or "").endswith(".NS") else f"{symbol}.NS"
    data = yf.download(tickers=yf_sym, period=period, interval=interval, auto_adjust=False, progress=False)
    if data.empty:
        return pd.DataFrame()
    return data.xs(yf_sym, level=1, axis=1) if isinstance(data.columns, MultiIndex := getattr(pd, 'MultiIndex', None)) and isinstance(data.columns, pd.MultiIndex) else data.copy()

# ============================================================
# MULTI-BROKER OAUTH TOKEN EXCHANGE & VALIDATION
# ============================================================
def exchange_token_and_verify(broker_name, api_key, api_secret, auth_token):
    try:
        api_key = str(api_key or "")
        api_secret = str(api_secret or "")
        auth_token = str(auth_token or "")

        if not api_key or not api_secret:
            return False, "", "API Key and Secret cannot be empty 🔴"

        if broker_name == "Zerodha Kite":
            hasher = hashlib.sha256()
            hasher.update((api_key + auth_token + api_secret).encode("utf-8"))
            payload = {"api_key": api_key, "request_token": auth_token, "checksum": hasher.hexdigest()}
            response = requests.post("https://api.kite.trade/session/token", data=payload, timeout=5)
            res = response.json()
            if response.status_code == 200 and res.get("status") == "success":
                return True, res.get("data", {}).get("access_token"), "Connected to Zerodha Kite 🟢"
            return False, "", f"Zerodha Error: {res.get('message', 'Invalid Token')}"

        elif broker_name == "Upstox":
            headers = {"accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"}
            payload = {"code": auth_token, "client_id": api_key, "client_secret": api_secret, "redirect_uri": "https://127.0.0.1", "grant_type": "authorization_code"}
            response = requests.post("https://api-v2.upstox.com/login/authorization/token", headers=headers, data=payload, timeout=5)
            res = response.json()
            if response.status_code == 200 and res.get("status") == "success":
                return True, res.get("data", {}).get("access_token"), "Connected to Upstox 🟢"
            return False, "", f"Upstox Error: {res.get('message', 'Invalid Auth Code')}"

        elif broker_name == "Angel One":
            headers = {"Content-Type": "application/json", "Accept": "application/json", "X-PrivateKey": api_key}
            payload = {"clientcode": auth_token, "password": api_secret}
            response = requests.post("https://apiconnect.angelone.in/rest/auth/angelbroking/user/v1/loginByPassword", headers=headers, json=payload, timeout=5)
            res = response.json()
            if response.status_code == 200 and res.get("status") == True:
                return True, res.get("data", {}).get("jwtToken"), "Connected to Angel One SmartAPI 🟢"
            return False, "", f"Angel One Error: {res.get('message', 'Invalid Login')}"

        return False, "", "Unsupported Broker Selected 🔴"
    except Exception as e:
        return False, "", f"Connection Exception: {str(e)} 🔴"

# ============================================================
# LIVE BROKER ORDER EXECUTION & GTT OCO MODIFICATION ENGINE
# ============================================================
def place_live_broker_order(broker_name, api_key, session_token, symbol, action, qty, price, stop_loss, target):
    try:
        clean_symbol = symbol.replace(".NS", "")
        rounded_price = round_to_tick_size(price, 0.10)
        rounded_sl = round_to_tick_size(stop_loss, 0.10)
        rounded_tp = round_to_tick_size(target, 0.10)
        
        if not session_token:
            return False, "Broker Session Token missing. Please login first 🔴", None

        if broker_name == "Zerodha Kite":
            headers = {
                "X-Kite-Version": "3",
                "Authorization": f"token {api_key}:{session_token}",
                "Content-Type": "application/x-www-form-urlencoded"
            }
            payload = {
                "tradingsymbol": clean_symbol,
                "exchange": "NFO" if "CE" in clean_symbol or "PE" in clean_symbol else "NSE",
                "transaction_type": action.upper(),
                "order_type": "LIMIT",
                "price": rounded_price,
                "quantity": int(qty),
                "product": "MIS",
                "validity": "DAY"
            }
            response = requests.post("https://api.kite.trade/orders/regular", headers=headers, data=payload, timeout=5)
            res = response.json()
            gtt_id = None
            if response.status_code == 200 and res.get("status") == "success":
                order_id = res.get("data", {}).get("order_id")
                
                gtt_payload = {
                    "type": "two-leg",
                    "condition": {
                        "exchange": "NFO" if "CE" in clean_symbol or "PE" in clean_symbol else "NSE",
                        "tradingsymbol": clean_symbol,
                        "trigger_values": [rounded_sl, rounded_tp]
                    },
                    "orders": [
                        {
                            "transaction_type": "SELL",
                            "quantity": int(qty),
                            "product": "MIS",
                            "order_type": "LIMIT",
                            "price": rounded_sl
                        },
                        {
                            "transaction_type": "SELL",
                            "quantity": int(qty),
                            "product": "MIS",
                            "order_type": "LIMIT",
                            "price": rounded_tp
                        }
                    ]
                }
                gtt_res = requests.post("https://api.kite.trade/gtts", headers=headers, json=gtt_payload, timeout=5).json()
                if gtt_res.get("status") == "success":
                    gtt_id = gtt_res.get("data", {}).get("trigger_id")

                return True, f"Kite Order Placed! ID: {order_id} (GTT SL: {rounded_sl}, TP: {rounded_tp}) 🟢", gtt_id
            
            error_msg = res.get('message', 'Rejected')
            return False, f"Kite API Error: {error_msg}", None

        elif broker_name == "Upstox":
            headers = {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": f"Bearer {session_token}"
            }
            payload = {
                "quantity": int(qty), "product": "I", "validity": "DAY",
                "price": rounded_price, "tag": "everybody_can_trade",
                "instrument_token": clean_symbol, "order_type": "LIMIT",
                "transaction_type": action.upper(), "disclosed_quantity": 0,
                "trigger_price": 0, "is_amo": False
            }
            response = requests.post("https://api-v2.upstox.com/order/place", headers=headers, json=payload, timeout=5)
            res = response.json()
            if response.status_code == 200 and res.get("status") == "success":
                return True, "Upstox Order Placed Successfully 🟢", None
            return False, f"Upstox API Error: {res.get('message', 'Rejected')}", None

        elif broker_name == "Angel One":
            headers = {
                "Content-Type": "application/json", "Accept": "application/json",
                "Authorization": f"Bearer {session_token}", "X-PrivateKey": api_key
            }
            payload = {
                "variety": "NORMAL", "tradingsymbol": clean_symbol + "-EQ", "symboltoken": clean_symbol,
                "transactiontype": action.upper(), "exchange": "NSE", "ordertype": "LIMIT",
                "producttype": "INTRADAY", "duration": "DAY", "price": str(rounded_price),
                "squareoff": "0", "stoploss": str(rounded_sl), "quantity": str(qty)
            }
            response = requests.post("https://apiconnect.angelone.in/rest/secure/angelbroking/order/v1/placeOrder", headers=headers, json=payload, timeout=5)
            res = response.json()
            if response.status_code == 200 and res.get("status") == True:
                return True, f"Angel One Order Placed! ID: {res.get('data', {}).get('orderid')} 🟢", None
            return False, f"Angel One Error: {res.get('message', 'Rejected')}", None

        return False, "Unsupported Broker Selected 🔴", None
    except Exception as e:
        return False, f"Broker Connection Exception: {str(e)} 🔴", None

def modify_broker_gtt_sl(broker_name, api_key, session_token, symbol, gtt_id, new_sl, target):
    try:
        if broker_name != "Zerodha Kite" or not gtt_id or not session_token:
            return False

        clean_symbol = symbol.replace(".NS", "")
        rounded_sl = round_to_tick_size(new_sl, 0.10)
        rounded_tp = round_to_tick_size(target, 0.10)
        exchange = "NFO" if "CE" in clean_symbol or "PE" in clean_symbol else "NSE"

        headers = {
            "X-Kite-Version": "3",
            "Authorization": f"token {api_key}:{session_token}",
            "Content-Type": "application/json"
        }
        payload = {
            "type": "two-leg",
            "condition": {
                "exchange": exchange,
                "tradingsymbol": clean_symbol,
                "trigger_values": [rounded_sl, rounded_tp]
            },
            "orders": [
                {
                    "transaction_type": "SELL",
                    "quantity": 100, 
                    "product": "MIS",
                    "order_type": "LIMIT",
                    "price": rounded_sl
                },
                {
                    "transaction_type": "SELL",
                    "quantity": 100,
                    "product": "MIS",
                    "order_type": "LIMIT",
                    "price": rounded_tp
                }
            ]
        }
        res = requests.put(f"https://api.kite.trade/gtts/{gtt_id}", headers=headers, json=payload, timeout=5)
        return res.status_code == 200 and res.json().get("status") == "success"
    except Exception:
        return False

# ============================================================
# MAIN APPLICATION SETUP (FLET UI)
# ============================================================
def main(page: ft.Page):
    page.title = "Everybody Can Trade - Multi-Broker Engine"
    page.theme_mode = ft.ThemeMode.DARK
    page.padding = 20
    page.scroll = ft.ScrollMode.AUTO

    portfolio_state = {
        "cash": 100000.0, "initial_capital": 100000.0,
        "positions": db_load_open_positions(), "history": db_load_trade_history()
    }
    
    scan_data_cache = {}

    header = ft.Text("🇮🇳 Everybody Can Trade (Multi-Broker Algorithmic Engine)", size=24, weight=ft.FontWeight.BOLD)
    subtitle = ft.Text("Quantitative breakout rules built right into your trading workspace.", size=13)

    broker_dropdown = ft.Dropdown(
        label="Select Broker API", width=170, value="Zerodha Kite",
        options=[
            ft.dropdown.Option("Paper Trading"),
            ft.dropdown.Option("Zerodha Kite"),
            ft.dropdown.Option("Upstox"),
            ft.dropdown.Option("Angel One"),
        ]
    )
    api_key_input = ft.TextField(label="API Key / Client ID", value="", width=170)
    api_secret_input = ft.TextField(label="API Secret / Password", value="", width=170, password=True, can_reveal_password=True)
    request_token_input = ft.TextField(label="Request Token / Client Code", value="", width=240, hint_text="Auto-captured or paste code")
    
    open_browser_button = ft.Button(content=ft.Text("Login & Connect Broker"))
    broker_connection_status = ft.Text("Status: Not Connected 🔴", color="red", size=13)

    login_card = ft.Column([
        ft.Text("Multi-Broker API & OAuth Gateway:", weight=ft.FontWeight.BOLD),
        ft.Row([broker_dropdown, api_key_input, api_secret_input], wrap=True),
        ft.Row([open_browser_button, request_token_input], wrap=True),
        broker_connection_status,
        ft.Divider()
    ])

    active_broker_session = {"broker": "Paper Trading", "api_key": "", "api_secret": "", "session_token": "", "connected": False}

    watchlist_input = ft.TextField(
        label="NSE F&O Watchlist (Editable)", 
        value="RELIANCE.NS, TCS.NS, DIVISLAB.NS, LALPATHLAB.NS, GRASIM.NS, ZYDUSLIFE.NS", 
        width=750,
        multiline=True
    )
    
    autocomplete_listView = ft.ListView(expand=1, spacing=2, padding=10, auto_scroll=True)
    autocomplete_container = ft.Container(
        content=autocomplete_listView,
        bgcolor=ft.Colors.GREY_900,
        border_radius=8,
        height=120,
        width=750,
        visible=False,
        border=ft.Border.all(1, ft.Colors.CYAN_700)
    )

    all_fno_stocks = list(NSE_LOT_SIZES.keys())

    def on_watchlist_change(e):
        text_val = watchlist_input.value or ""
        parts = [p.strip() for p in text_val.split(",")]
        current_query = parts[-1].upper() if parts else ""

        if not current_query:
            autocomplete_container.visible = False
            page.update()
            return

        matches = [s for s in all_fno_stocks if current_query in s]
        if matches:
            autocomplete_listView.controls.clear()
            for match in matches[:8]:
                def make_suggestion_click(sym=match):
                    return lambda ev: add_stock_to_watchlist(sym)
                autocomplete_listView.controls.append(
                    ft.ListTile(
                        title=ft.Text(match, color="cyan", weight=ft.FontWeight.BOLD),
                        subtitle=ft.Text(f"Lot Size: {get_lot_size(match)}", size=11, color="grey"),
                        on_click=make_suggestion_click()
                    )
                )
            autocomplete_container.visible = True
        else:
            autocomplete_container.visible = False
        page.update()

    def add_stock_to_watchlist(sym):
        current_text = watchlist_input.value or ""
        parts = [p.strip() for p in current_text.split(",") if p.strip()]
        if parts:
            parts[-1] = sym
        else:
            parts = [sym]
        
        watchlist_input.value = ", ".join(parts) + ", "
        autocomplete_container.visible = False
        page.update()

    watchlist_input.on_change = on_watchlist_change

    htf_dropdown = ft.Dropdown(label="Higher TF (Trend)", width=140, value="1 Day", options=[ft.dropdown.Option("1 Hour"), ft.dropdown.Option("1 Day"), ft.dropdown.Option("1 Week")])
    ltf_dropdown = ft.Dropdown(label="Lower TF (Entry)", width=140, value="5 Min", options=[ft.dropdown.Option("5 Min"), ft.dropdown.Option("15 Min"), ft.dropdown.Option("1 Hour")])

    indicator_1_dropdown = ft.Dropdown(label="Indicator 1", width=160, value="MA Crossover", options=[ft.dropdown.Option("MA Crossover"), ft.dropdown.Option("RSI"), ft.dropdown.Option("MACD"), ft.dropdown.Option("SuperTrend"), ft.dropdown.Option("Bollinger Bands")])
    indicator_2_dropdown = ft.Dropdown(label="Indicator 2", width=160, value="Bollinger Bands", options=[ft.dropdown.Option("None"), ft.dropdown.Option("MA Crossover"), ft.dropdown.Option("RSI"), ft.dropdown.Option("MACD"), ft.dropdown.Option("SuperTrend"), ft.dropdown.Option("Bollinger Bands")])

    ma_fast_input, ma_slow_input = ft.TextField(label="MA Fast", value="9", width=110), ft.TextField(label="MA Slow", value="21", width=110)
    ema_fast_input, ema_slow_input = ft.TextField(label="EMA Fast", value="9", width=110), ft.TextField(label="EMA Slow", value="21", width=110)
    rsi_period_input = ft.TextField(label="RSI Period", value="14", width=110)
    macd_fast_input, macd_slow_input = ft.TextField(label="MACD Fast", value="12", width=110), ft.TextField(label="MACD Slow", value="26", width=110)
    st_period_input, st_mult_input = ft.TextField(label="ST Period", value="10", width=110), ft.TextField(label="ST Multiplier", value="3.0", width=110)
    bb_period_input, bb_std_input = ft.TextField(label="BB Period", value="20", width=110), ft.TextField(label="BB Std Dev", value="2.0", width=110)

    atr_period_input = ft.TextField(label="ATR Period", value="14", width=110)
    atr_mult_dropdown = ft.Dropdown(label="ATR SL Multiplier", width=130, value="2.0x ATR", options=[ft.dropdown.Option("1.0x ATR"), ft.dropdown.Option("2.0x ATR"), ft.dropdown.Option("3.0x ATR")])
    target_rr_dropdown = ft.Dropdown(label="Target (R:R)", width=130, value="1:3", options=[ft.dropdown.Option("1:2"), ft.dropdown.Option("1:3"), ft.dropdown.Option("1:5")])
    
    trailing_sl_checkbox = ft.Checkbox(label="Enable Trailing Stop Loss (TSL)", value=True)
    capital_input = ft.TextField(label="Initial Capital (₹)", value="100000", width=160)
    risk_pct_input = ft.TextField(label="Risk Per Trade (%)", value="1.0", width=130)
    
    auto_trade_checkbox = ft.Checkbox(label="Enable Live/Sandbox Broker Execution", value=False)
    scan_button = ft.Button(content=ft.Text("Scan Broker Candles & Execute Options"))

    portfolio_summary_text = ft.Text("Virtual Cash: ₹100,000.00 | Open Positions: 0 | Total P&L: ₹0.00", size=15, weight=ft.FontWeight.BOLD, color="cyan")
    analytics_summary_text = ft.Text("Win Rate: 0.0% | Total Trades: 0 | Max Drawdown: 0.0%", size=13, weight=ft.FontWeight.BOLD)

    scan_results_table = ft.DataTable(columns=[
        ft.DataColumn(ft.Text("Symbol")), 
        ft.DataColumn(ft.Text("Spot")), 
        ft.DataColumn(ft.Text("HTF")), 
        ft.DataColumn(ft.Text("Signal")), 
        ft.DataColumn(ft.Text("Lots/Qty")), 
        ft.DataColumn(ft.Text("Status")),
        ft.DataColumn(ft.Text("Rationale"))
    ], rows=[])
    
    explanation_card_content = ft.Markdown("Click on any stock's symbol to view its chart, or click 'View Rationale' beside the status column to inspect its quantitative breakdown.")
    explanation_card = ft.Container(content=explanation_card_content, padding=15, bgcolor=ft.Colors.GREY_900, border_radius=8, margin=10)

    open_positions_table = ft.DataTable(columns=[ft.DataColumn(ft.Text("Symbol")), ft.DataColumn(ft.Text("Type")), ft.DataColumn(ft.Text("Strike/Entry")), ft.DataColumn(ft.Text("SL")), ft.DataColumn(ft.Text("Target")), ft.DataColumn(ft.Text("Current")), ft.DataColumn(ft.Text("Qty")), ft.DataColumn(ft.Text("P&L")), ft.DataColumn(ft.Text("Action"))], rows=[])
    trade_history_table = ft.DataTable(columns=[ft.DataColumn(ft.Text("Time")), ft.DataColumn(ft.Text("Symbol")), ft.DataColumn(ft.Text("Type")), ft.DataColumn(ft.Text("Entry")), ft.DataColumn(ft.Text("Exit")), ft.DataColumn(ft.Text("Qty")), ft.DataColumn(ft.Text("P&L")), ft.DataColumn(ft.Text("Reason"))], rows=[])
    status_text = ft.Text("", size=13)

    dashboard_container = ft.Column([
        ft.Divider(), ft.Text("Watchlist & Timeframe Configs:", weight=ft.FontWeight.BOLD),
        watchlist_input,
        autocomplete_container,
        ft.Row([htf_dropdown, ltf_dropdown], wrap=True),
        ft.Row([indicator_1_dropdown, indicator_2_dropdown], wrap=True),
        ft.Divider(), ft.Text("Indicator Fine-Tuning:", weight=ft.FontWeight.BOLD),
        ft.Row([ma_fast_input, ma_slow_input, ema_fast_input, ema_slow_input], wrap=True),
        ft.Row([rsi_period_input, macd_fast_input, macd_slow_input], wrap=True),
        ft.Row([st_period_input, st_mult_input, bb_period_input, bb_std_input], wrap=True),
        ft.Divider(), ft.Text("Risk Management & Execution Controls:", weight=ft.FontWeight.BOLD),
        ft.Row([atr_period_input, atr_mult_dropdown, target_rr_dropdown, capital_input, risk_pct_input], wrap=True),
        ft.Row([trailing_sl_checkbox, auto_trade_checkbox, scan_button], wrap=True),
        ft.Divider(), ft.Text("📊 Active Portfolio & Option Positions:", size=16, weight=ft.FontWeight.BOLD),
        portfolio_summary_text, open_positions_table,
        ft.Divider(), ft.Text("📈 Analytics Summary:", size=14, weight=ft.FontWeight.BOLD),
        analytics_summary_text,
        ft.Divider(),
        ft.Text("💡 Selected Indicator & Algorithmic Rationale:", weight=ft.FontWeight.BOLD), explanation_card,
        ft.Text("📊 Broker Candle Scan Results:", size=14, weight=ft.FontWeight.BOLD), scan_results_table,
        ft.Divider(), ft.Text("📜 Persistent Trade History Ledger:", size=14, weight=ft.FontWeight.BOLD), trade_history_table,
        status_text
    ], visible=False)

    def evaluate_breakout_indicator(ind_name, latest, previous, price):
        if ind_name == "MA Crossover":
            if previous["MA_FAST"] <= previous["MA_SLOW"] and latest["MA_FAST"] > latest["MA_SLOW"]:
                return "BUY", "MA Breakout (Fast MA crossed above Slow MA)"
            elif previous["MA_FAST"] >= previous["MA_SLOW"] and latest["MA_FAST"] < latest["MA_SLOW"]:
                return "SELL", "MA Breakdown (Fast MA crossed below Slow MA)"
        elif ind_name == "RSI":
            if float(previous["RSI"]) <= 50 and float(latest["RSI"]) > 50:
                return "BUY", f"RSI Breakout (RSI crossed above 50 mid-line at {latest['RSI']:.1f})"
            elif float(previous["RSI"]) >= 50 and float(latest["RSI"]) < 50:
                return "SELL", f"RSI Breakdown (RSI crossed below 50 mid-line at {latest['RSI']:.1f})"
        elif ind_name == "MACD":
            if previous["MACD"] <= previous["MACD_SIGNAL"] and latest["MACD"] > latest["MACD_SIGNAL"]:
                return "BUY", "MACD Bullish Crossover (MACD line crossed above Signal line)"
            elif previous["MACD"] >= previous["MACD_SIGNAL"] and latest["MACD"] < latest["MACD_SIGNAL"]:
                return "SELL", "MACD Bearish Crossover (MACD line crossed below Signal line)"
        elif ind_name == "SuperTrend":
            if previous["ST_DIRECTION"] == -1 and latest["ST_DIRECTION"] == 1:
                return "BUY", "SuperTrend Green Flip (Price broke above SuperTrend resistance)"
            elif previous["ST_DIRECTION"] == 1 and latest["ST_DIRECTION"] == -1:
                return "SELL", "SuperTrend Red Flip (Price broke below SuperTrend support)"
        elif ind_name == "Bollinger Bands":
            if previous["Close"] <= previous["BB_HIGH"] and latest["Close"] > latest["BB_HIGH"]:
                return "BUY", "Upper Bollinger Band Breakout (Price closed above Upper Band)"
            elif previous["Close"] >= previous["BB_LOW"] and latest["Close"] < latest["BB_LOW"]:
                return "SELL", "Lower Bollinger Band Breakdown (Price closed below Lower Band)"
        return "NEUTRAL", f"No breakout detected for {ind_name}"

    def close_position(pos, reason="Manual"):
        if pos in portfolio_state["positions"]:
            portfolio_state["positions"].remove(pos)
            db_remove_open_position(pos["symbol"], pos["option_type"])
            exit_price = pos.get("current_price", pos["entry_price"])
            realized_pnl = (exit_price - pos["entry_price"]) * pos["qty"]
            portfolio_state["cash"] += (pos["entry_price"] * pos["qty"]) + realized_pnl
            closed_record = {
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "symbol": pos["symbol"],
                "option_type": pos["option_type"], "entry_price": pos["entry_price"],
                "exit_price": exit_price, "qty": pos["qty"], "realized_pnl": realized_pnl, "reason": reason
            }
            portfolio_state["history"].append(closed_record)
            db_log_closed_trade(closed_record)

    def update_portfolio_ui():
        total_unrealized_pnl = 0.0
        open_rows = []
        for pos in list(portfolio_state["positions"]):
            try:
                live_df = fetch_broker_candles(active_broker_session, pos["symbol"], "1d", "5m")
                current_spot = float(live_df["Close"].iloc[-1]) if not live_df.empty else pos["spot_at_entry"]
            except Exception:
                current_spot = pos["spot_at_entry"]

            spot_diff = current_spot - pos["spot_at_entry"]
            current_price = max(1.0, pos["entry_price"] + (spot_diff * (0.5 if pos["option_type"] == "CE" else -0.5)))
            pos["current_price"] = current_price

            if trailing_sl_checkbox.value and current_price > pos["entry_price"]:
                new_sl = round_to_tick_size(current_price - (pos["entry_price"] - pos["stop_loss"]), 0.10)
                if new_sl > pos["stop_loss"]:
                    pos["stop_loss"] = new_sl
                    db_update_position_sl(pos["symbol"], pos["option_type"], new_sl)
                    if active_broker_session["broker"] == "Zerodha Kite" and pos.get("gtt_id"):
                        modify_broker_gtt_sl(
                            active_broker_session["broker"],
                            active_broker_session["api_key"],
                            active_broker_session["session_token"],
                            pos["symbol"], pos["gtt_id"], new_sl, pos["target_price"]
                        )

            if current_price <= pos["stop_loss"] or current_price >= pos["target_price"]:
                close_position(pos, reason="SL Hit 🛑" if current_price <= pos["stop_loss"] else "Target Hit 🎯")
                continue

            unrealized_pnl = (current_price - pos["entry_price"]) * pos["qty"]
            total_unrealized_pnl += unrealized_pnl
            
            def make_close_handler(p_item):
                return lambda e: (close_position(p_item, reason="Manual Close"), update_portfolio_ui())

            open_rows.append(ft.DataRow(cells=[
                ft.DataCell(ft.Text(pos["symbol"], weight=ft.FontWeight.BOLD)),
                ft.DataCell(ft.Text(pos["option_type"], color="cyan")),
                ft.DataCell(ft.Text(f"{pos['entry_price']:,.2f}")),
                ft.DataCell(ft.Text(f"{pos['stop_loss']:,.2f}", color="red")),
                ft.DataCell(ft.Text(f"{pos['target_price']:,.2f}", color="green")),
                ft.DataCell(ft.Text(f"{current_price:,.2f}")),
                ft.DataCell(ft.Text(str(pos["qty"]))),
                ft.DataCell(ft.Text(f"₹{unrealized_pnl:,.2f}", color="green" if unrealized_pnl >= 0 else "red")),
                ft.DataCell(ft.Button(content=ft.Text("Close", size=10), on_click=make_close_handler(pos))),
            ]))

        open_positions_table.rows = open_rows
        tot_realized = sum([h["realized_pnl"] for h in portfolio_state["history"]])
        portfolio_summary_text.value = f"Virtual Cash: ₹{portfolio_state['cash']:,.2f} | Open: {len(portfolio_state['positions'])} | P&L: ₹{tot_realized + total_unrealized_pnl:,.2f}"
        page.update()

    async def analyze_single_stock_async(symbol, params, htf_str, ltf_str, ind1, ind2, capital, risk_pct, atr_mult, rr_mult):
        timeframe_map = {"5 Min": ("30d", "5m"), "15 Min": ("30d", "15m"), "1 Hour": ("60d", "60m"), "1 Day": ("1y", "1d"), "1 Week": ("2y", "1wk")}
        htf_raw, ltf_raw = await asyncio.gather(
            asyncio.to_thread(fetch_broker_candles, active_broker_session, symbol, timeframe_map.get(htf_str, ("1y", "1d"))[0], timeframe_map.get(htf_str, ("1y", "1d"))[1]),
            asyncio.to_thread(fetch_broker_candles, active_broker_session, symbol, timeframe_map.get(ltf_str, ("30d", "5m"))[0], timeframe_map.get(ltf_str, ("30d", "5m"))[1])
        )
        if htf_raw.empty or ltf_raw.empty: return symbol, 0.0, "ERROR", "Data fetch failed", "0 Qty", "Error", ""

        htf_df, ltf_df = process_indicators(htf_raw, params), process_indicators(ltf_raw, params)
        price, htf_trend = float(ltf_df["Close"].iloc[-1]), ("BULLISH" if htf_df["EMA_FAST"].iloc[-1] > htf_df["EMA_SLOW"].iloc[-1] else "BEARISH")
        
        sig1, desc1 = evaluate_breakout_indicator(ind1, ltf_df.iloc[-1], ltf_df.iloc[-2], price)
        sig2, desc2 = evaluate_breakout_indicator(ind2, ltf_df.iloc[-1], ltf_df.iloc[-2], price) if ind2 != "None" else ("BUY", "Indicator 2 disabled")
        
        final_signal = "BUY" if (sig1 == "BUY" and (sig2 == "BUY" or ind2 == "None") and htf_trend == "BULLISH") else "HOLD"
        option_type = "CE" if final_signal == "BUY" else "HOLD"

        strike_interval = 5.0 if price < 500 else (10.0 if price < 2000 else 50.0)
        atm_strike = round(price / strike_interval) * strike_interval
        estimated_option_premium = round_to_tick_size(max(25.0, price * 0.035), 0.10)

        atr_val = float(ltf_df["ATR"].iloc[-1])
        risk_unit = max(1.0, atr_val * atr_mult * 0.4)
        sl = round_to_tick_size(max(1.0, estimated_option_premium - risk_unit), 0.10)
        tp = round_to_tick_size(estimated_option_premium + (risk_unit * rr_mult), 0.10)
        
        lot_sz = get_lot_size(symbol)
        total_risk_amount = capital * (risk_pct / 100.0)
        risk_per_lot = risk_unit * lot_sz
        lots = max(1, round(total_risk_amount / risk_per_lot)) if risk_per_lot > 0 else 1
        qty = lot_sz * lots

        explanation = (
            f"📌 **Algorithmic Breakdown & Option Strategy for {symbol}**:\n"
            f"• **Spot Price**: ₹{price:,.2f} | **ATM Strike**: {atm_strike} CE\n"
            f"• **HTF Filter ({htf_str})**: {htf_trend}\n"
            f"• **Indicator 1 ({ind1})**: {sig1} ({desc1})\n"
            f"• **Indicator 2 ({ind2})**: {sig2} ({desc2})\n"
            f"• **Action Verdict**: {final_signal} ({option_type} Option)\n"
            f"• **Est. Premium**: ₹{estimated_option_premium:,.2f} | **ATR SL**: ₹{sl:,.2f} | **Target**: ₹{tp:,.2f}\n"
            f"• **Option Qty**: {lots} Lot/s ({qty} Qty)"
        )

        exec_note = f"Signal Verified ({lots} Lot/s)"
        gtt_id = None
        
        if auto_trade_checkbox.value and final_signal == "BUY":
            if active_broker_session["broker"] != "Paper Trading":
                success, msg, gtt_id = place_live_broker_order(
                    active_broker_session["broker"], 
                    active_broker_session["api_key"], 
                    active_broker_session["session_token"], 
                    symbol, "BUY", qty, estimated_option_premium, sl, tp
                )
                exec_note = msg
                if not success:
                    exec_note = f"{msg} -> Logged in App Sandbox"
            
            if not any(p["symbol"] == symbol for p in portfolio_state["positions"]):
                portfolio_state["cash"] -= estimated_option_premium * qty
                new_pos = {"symbol": symbol, "option_type": option_type, "spot_at_entry": price, "entry_price": estimated_option_premium, "current_price": estimated_option_premium, "stop_loss": sl, "target_price": tp, "qty": qty, "gtt_id": gtt_id}
                portfolio_state["positions"].append(new_pos)
                db_save_open_position(new_pos)
        elif final_signal == "BUY" and not auto_trade_checkbox.value:
            if not any(p["symbol"] == symbol for p in portfolio_state["positions"]):
                new_pos = {"symbol": symbol, "option_type": option_type, "spot_at_entry": price, "entry_price": estimated_option_premium, "current_price": estimated_option_premium, "stop_loss": sl, "target_price": tp, "qty": qty, "gtt_id": None}
                portfolio_state["positions"].append(new_pos)
                db_save_open_position(new_pos)

        return symbol, price, htf_trend, option_type, f"{lots} Lot ({qty})", exec_note, explanation

    def render_tradingview_chart(e, target_sym=None):
        sym = target_sym
        if not sym:
            status_text.value = "Error: No symbol selected for charting."
            page.update()
            return
        
        try:
            params = {
                "ma_fast": int(ma_fast_input.value or 9), "ma_slow": int(ma_slow_input.value or 21),
                "ema_fast": int(ema_fast_input.value or 9), "ema_slow": int(ema_slow_input.value or 21),
                "rsi_period": int(rsi_period_input.value or 14), "macd_fast": int(macd_fast_input.value or 12),
                "macd_slow": int(macd_slow_input.value or 26), "st_period": int(st_period_input.value or 10),
                "st_mult": float(st_mult_input.value or 3.0), "bb_window": int(bb_period_input.value or 20),
                "bb_std": float(bb_std_input.value or 2.0), "atr_period": int(atr_period_input.value or 14)
            }
            df_raw = fetch_broker_candles(active_broker_session, sym, "30d", "5m")
            if df_raw.empty:
                status_text.value = f"Error: No candle data returned for {sym}"
                page.update()
                return
            
            df = process_indicators(df_raw, params)
            chart = Chart(title=f"TradingView Workspace - {sym}", width=1100, height=600)
            chart.legend(True)

            chart_df = df.reset_index()
            date_col = next((col for col in chart_df.columns if str(col).lower() in ['date', 'datetime', 'time']), chart_df.columns[0])
            
            chart_df = chart_df.rename(columns={date_col: 'time', 'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume'})
            chart_df['time'] = pd.to_datetime(chart_df['time']).dt.strftime('%Y-%m-%d %H:%M:%S')

            chart.set(chart_df[['time', 'open', 'high', 'low', 'close', 'volume']])

            ind1 = indicator_1_dropdown.value
            if ind1 == "MA Crossover":
                name_fast = f"MA {params['ma_fast']}"
                name_slow = f"MA {params['ma_slow']}"
                chart_df[name_fast] = chart_df['MA_FAST']
                chart_df[name_slow] = chart_df['MA_SLOW']
                
                line_fast = chart.create_line(name=name_fast, price_scale_id='right', color='#22c55e')
                line_fast.set(chart_df[['time', name_fast]])
                line_slow = chart.create_line(name=name_slow, price_scale_id='right', color='#ef4444')
                line_slow.set(chart_df[['time', name_slow]])
            elif ind1 == "SuperTrend":
                chart_df["SuperTrend"] = chart_df['SUPERTREND']
                st_line = chart.create_line(name="SuperTrend", price_scale_id='right', color='#eab308')
                st_line.set(chart_df[['time', "SuperTrend"]])
            elif ind1 == "Bollinger Bands":
                chart_df["BB High"] = chart_df['BB_HIGH']
                chart_df["BB Low"] = chart_df['BB_LOW']
                bb_high = chart.create_line(name="BB High", price_scale_id='right', color='gray')
                bb_high.set(chart_df[['time', "BB High"]])
                bb_low = chart.create_line(name="BB Low", price_scale_id='right', color='gray')
                bb_low.set(chart_df[['time', "BB Low"]])
            elif ind1 == "MACD":
                chart_df["MACD"] = chart_df['MACD']
                chart_df["MACD Signal"] = chart_df['MACD_SIGNAL']
                macd_line = chart.create_line(name="MACD", price_scale_id='right', color='#38bdf8')
                macd_line.set(chart_df[['time', "MACD"]])
                sig_line = chart.create_line(name="MACD Signal", price_scale_id='right', color='#f97316')
                sig_line.set(chart_df[['time', "MACD Signal"]])

            if sym in scan_data_cache:
                explanation_card_content.value = scan_data_cache[sym]

            status_text.value = f"Opening TradingView chart window for {sym}..."
            page.update()

            def run_chart():
                try:
                    chart.show()
                except Exception as ex:
                    print(f"Chart Window Exception: {ex}")

            threading.Thread(target=run_chart, daemon=True).start()

        except Exception as e:
            status_text.value = f"Chart Error: {str(e)}"
            page.update()

    async def scan_watchlist(e=None):
        autocomplete_container.visible = False
        raw_watchlist = str(watchlist_input.value or "")
        symbols = [s.strip().upper() + (".NS" if not s.strip().upper().endswith(".NS") else "") for s in raw_watchlist.split(",") if s.strip()]
        
        params = {
            "ma_fast": int(ma_fast_input.value or 9), "ma_slow": int(ma_slow_input.value or 21),
            "ema_fast": int(ema_fast_input.value or 9), "ema_slow": int(ema_slow_input.value or 21),
            "rsi_period": int(rsi_period_input.value or 14), "macd_fast": int(macd_fast_input.value or 12),
            "macd_slow": int(macd_slow_input.value or 26), "st_period": int(st_period_input.value or 10),
            "st_mult": float(st_mult_input.value or 3.0), "bb_window": int(bb_period_input.value or 20),
            "bb_std": float(bb_std_input.value or 2.0), "atr_period": int(atr_period_input.value or 14)
        }
        results = await asyncio.gather(*[analyze_single_stock_async(sym, params, htf_dropdown.value, ltf_dropdown.value, indicator_1_dropdown.value, indicator_2_dropdown.value, float(capital_input.value), float(risk_pct_input.value), float(atr_mult_dropdown.value.split("x")[0]), float(target_rr_dropdown.value.split(":")[1])) for sym in symbols])

        scan_data_cache.clear()
        for s, p, h, o, q, n, explanation in results:
            scan_data_cache[s] = explanation

        def make_chart_click_handler(target_sym):
            return lambda ev: render_tradingview_chart(None, target_sym=target_sym)

        def make_rationale_click_handler(target_sym):
            return lambda ev: (
                setattr(explanation_card_content, "value", scan_data_cache.get(target_sym, "No rationale available.")),
                page.update()
            )

        scan_results_table.rows = [
            ft.DataRow(cells=[
                ft.DataCell(ft.Row([ft.Button(content=ft.Text(s, size=11), on_click=make_chart_click_handler(s))])),
                ft.DataCell(ft.Text(f"{p:,.2f}")),
                ft.DataCell(ft.Text(h)),
                ft.DataCell(ft.Text(o, color="cyan")),
                ft.DataCell(ft.Text(q)),
                ft.DataCell(ft.Text(n, size=11)),
                ft.DataCell(ft.Button(content=ft.Text("View Rationale", size=10), on_click=make_rationale_click_handler(s)))
            ]) for s, p, h, o, q, n, _ in results
        ]
        
        if results:
            first_sym = results[0][0]
            if first_sym in scan_data_cache:
                explanation_card_content.value = scan_data_cache[first_sym]
        
        update_portfolio_ui()
        page.update()

    scan_button.on_click = scan_watchlist

    def on_login_and_connect(e):
        broker = broker_dropdown.value
        key = str(api_key_input.value or "").strip()
        secret = str(api_secret_input.value or "").strip()
        token = str(request_token_input.value or "").strip()

        if broker == "Paper Trading":
            active_broker_session.update({"broker": "Paper Trading", "connected": True})
            login_card.visible, dashboard_container.visible = False, True
            broker_connection_status.value, broker_connection_status.color = "Connected to Sandbox 🟢", "green"
            page.update()
            return

        if token:
            valid, stoken, msg = exchange_token_and_verify(broker, key, secret, token)
            if valid:
                active_broker_session.update({"broker": broker, "api_key": key, "session_token": stoken, "connected": True})
                login_card.visible, dashboard_container.visible = False, True
            broker_connection_status.value, broker_connection_status.color = msg, ("green" if valid else "red")
            page.update()
            return

        oauth_callback_store["request_token"] = None
        threading.Thread(target=start_local_server, daemon=True).start()
        
        import webbrowser
        if broker == "Zerodha Kite": webbrowser.open(f"https://kite.zerodha.com/connect/login?api_key={key}&v=3")
        elif broker == "Upstox": webbrowser.open(f"https://api-v2.upstox.com/login/authorization/dialog?client_id={key}&redirect_uri=https://127.0.0.1&response_type=code")
        elif broker == "Angel One": webbrowser.open("https://smartapi.angelone.in/login")
        
        broker_connection_status.value, broker_connection_status.color = "Browser opened. Authenticate...", "orange"
        page.update()

        def background_wait():
            while oauth_callback_store["request_token"] is None:
                import time; time.sleep(0.5)
            request_token_input.value = str(oauth_callback_store["request_token"] or "")
            page.update()
            on_login_and_connect(None)
        threading.Thread(target=background_wait, daemon=True).start()

    open_browser_button.on_click = on_login_and_connect

    async def portfolio_refresh_loop():
        while True:
            await asyncio.sleep(3)
            if dashboard_container.visible:
                try:
                    update_portfolio_ui()
                except Exception:
                    pass

    page.run_task(portfolio_refresh_loop)

    page.add(header, subtitle, login_card, dashboard_container)

if __name__ == "__main__":
    ft.run(
        main,
        view=ft.AppView.WEB_BROWSER,
        port=int(os.environ.get("PORT", 8080)),
        host="0.0.0.0"
    )
    if __name__ == "__main__":
     ft.run(
        main, 
        view=ft.AppView.WEB_BROWSER, 
        port=int(os.environ.get("PORT", 8080)), 
        host="0.0.0.0"
    )
