import os
import asyncio
from datetime import datetime, time, timedelta
import hashlib
import http.server
import sqlite3
import threading
import urllib.parse
import webbrowser
import math
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
# NSE LOT SIZES & SECTOR MAPPINGS
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
    "COALINDIA.NS": 1400, "DIVISLAB.NS": 200, "LALPATHLAB.NS": 300, "GRASIM.NS": 250, "ZYDUSLIFE.NS": 400
}

SECTOR_MAP = {
    "IT": ["TCS.NS", "INFY.NS", "WIPRO.NS", "HCLTECH.NS"],
    "Banking & Financials": ["HDFCBANK.NS", "ICICIBANK.NS", "SBIN.NS", "AXISBANK.NS", "KOTAKBANK.NS", "BAJFINANCE.NS", "BAJAJFINSV.NS"],
    "Oil, Gas & Energy": ["RELIANCE.NS", "ONGC.NS", "NTPC.NS", "POWERGRID.NS", "COALINDIA.NS"],
    "Automobile": ["TATAMOTORS.NS", "MARUTI.NS", "M&M.NS"],
    "Pharma & Healthcare": ["SUNPHARMA.NS", "DIVISLAB.NS", "LALPATHLAB.NS", "ZYDUSLIFE.NS"],
    "Consumer & Metals": ["ITC.NS", "HINDUNILVR.NS", "BHARTIARTL.NS", "LT.NS", "ASIANPAINT.NS", "ULTRACEMCO.NS", "TITAN.NS", "TATASTEEL.NS", "GRASIM.NS"]
}

def get_lot_size(symbol: str) -> int:
    clean = str(symbol or "").upper()
    if not clean.endswith(".NS"):
        clean += ".NS"
    return NSE_LOT_SIZES.get(clean, 500)

def round_to_tick_size(price: float, tick_size: float = 0.10) -> float:
    return round(round(price / tick_size) * tick_size, 2)

# ============================================================
# AUTOMATED OPTIONS STRIKE SELECTOR (ATM / DELTA MAPPING)
# ============================================================
def select_dynamic_option_contract(symbol: str, spot_price: float, option_type: str, delta_mode: str = "ATM (Approx 0.5 Delta)"):
    clean_sym = symbol.replace(".NS", "")
    if spot_price > 20000:
        step = 100
    elif spot_price > 5000:
        step = 50
    elif spot_price > 1000:
        step = 20
    else:
        step = 10

    atm_strike = round(spot_price / step) * step
    
    if "OTM" in delta_mode:
        strike = atm_strike + (step * 2) if option_type == "CE" else atm_strike - (step * 2)
    elif "ITM" in delta_mode:
        strike = atm_strike - (step * 2) if option_type == "CE" else atm_strike + (step * 2)
    else:
        strike = atm_strike

    estimated_premium = round_to_tick_size(spot_price * 0.018)
    try:
        tk = yf.Ticker(symbol)
        exp_dates = tk.options
        if exp_dates:
            chain = tk.option_chain(exp_dates[0])
            contracts_df = chain.calls if option_type == "CE" else chain.puts
            if not contracts_df.empty:
                contracts_df['diff'] = (contracts_df['strike'] - strike).abs()
                closest_row = contracts_df.loc[contracts_df['diff'].idxmin()]
                if closest_row['lastPrice'] > 0:
                    estimated_premium = float(closest_row['lastPrice'])
                    strike = int(closest_row['strike'])
    except Exception:
        pass

    contract_symbol = f"{clean_sym}{datetime.now().strftime('%y%m%d')}{strike}{option_type}"
    return contract_symbol, strike, estimated_premium

# ============================================================
# ADVANCED RISK & PORTFOLIO ANALYTICS ENGINE
# ============================================================
def compute_advanced_analytics(history_ledger, initial_capital):
    if not history_ledger:
        return 0.0, 0.0, 0.0, 0.0, 0.0

    total_trades = len(history_ledger)
    gross_profits = sum(t["realized_pnl"] for t in history_ledger if t["realized_pnl"] > 0)
    gross_losses = abs(sum(t["realized_pnl"] for t in history_ledger if t["realized_pnl"] < 0))
    
    win_trades = [t for t in history_ledger if t["realized_pnl"] > 0]
    loss_trades = [t for t in history_ledger if t["realized_pnl"] <= 0]
    
    win_rate = (len(win_trades) / total_trades) * 100.0
    profit_factor = (gross_profits / gross_losses) if gross_losses > 0 else (gross_profits if gross_profits > 0 else 0.0)
    
    avg_win = (gross_profits / len(win_trades)) if win_trades else 0.0
    avg_loss = (gross_losses / len(loss_trades)) if loss_trades else 0.0
    expectancy = ((win_rate / 100.0) * avg_win) - ((1.0 - (win_rate / 100.0)) * avg_loss)

    running_equity = initial_capital
    peak = initial_capital
    max_dd_val = 0.0
    
    for t in history_ledger:
        running_equity += t["realized_pnl"]
        if running_equity > peak:
            peak = running_equity
        drawdown = peak - running_equity
        if drawdown > max_dd_val:
            max_dd_val = drawdown

    max_dd_pct = (max_dd_val / peak * 100.0) if peak > 0 else 0.0
    total_realized = sum(t["realized_pnl"] for t in history_ledger)

    return win_rate, profit_factor, expectancy, max_dd_pct, total_realized

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
# MULTI-BROKER OAUTH TOKEN EXCHANGE & VALIDATION
# ============================================================
def exchange_token_and_verify(broker_name, api_key, api_secret, auth_token):
    try:
        api_key = str(api_key or "").strip()
        api_secret = str(api_secret or "").strip()
        auth_token = str(auth_token or "").strip()

        if not api_key or not api_secret:
            return False, "", "API Key and Secret cannot be empty 🔴"

        if broker_name == "Zerodha Kite":
            if len(auth_token) < 10:
                return False, "", "Zerodha Error: `request_token` should be minimum 10 characters in length. 🔴"
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
                        {"transaction_type": "SELL", "quantity": int(qty), "product": "MIS", "order_type": "LIMIT", "price": rounded_sl},
                        {"transaction_type": "SELL", "quantity": int(qty), "product": "MIS", "order_type": "LIMIT", "price": rounded_tp}
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

def fetch_broker_candles(broker_session, symbol, period, interval):
    yf_sym = symbol if str(symbol or "").endswith(".NS") else f"{symbol}.NS"
    data = yf.download(tickers=yf_sym, period=period, interval=interval, auto_adjust=False, progress=False)
    if data.empty:
        return pd.DataFrame()
    return data.xs(yf_sym, level=1, axis=1) if isinstance(data.columns, getattr(pd, 'MultiIndex', None)) and isinstance(data.columns, pd.MultiIndex) else data.copy()

# ============================================================
# MAIN APPLICATION SETUP (FLET UI)
# ============================================================
def main(page: ft.Page):
    page.title = "Everybody Can Trade - Market Breadth, Backtesting & Advanced Controls"
    page.theme_mode = ft.ThemeMode.DARK
    page.padding = 20
    page.scroll = ft.ScrollMode.AUTO

    portfolio_state = {
        "cash": 100000.0, "initial_capital": 100000.0,
        "positions": db_load_open_positions(), "history": db_load_trade_history()
    }
    
    scan_data_cache = {}

    header = ft.Text("🇮🇳 Everybody Can Trade (Advanced Engine + Heatmap + Backtest Sandbox + Auto-Refresh)", size=24, weight=ft.FontWeight.BOLD)
    subtitle = ft.Text("Market breadth analysis, sector heatmaps, rigorous strategy backtesting, automated strikes, and strict risk controls.", size=13)

    broker_dropdown = ft.Dropdown(
        label="Select Broker API", width=170, value="Paper Trading",
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
    bypass_button = ft.Button(content=ft.Text("🚀 Bypass & Launch Dashboard"), bgcolor=ft.Colors.INDIGO_700, color="white")
    broker_connection_status = ft.Text("Status: Paper Trading Active (Default) 🟢", color="green", size=13)

    login_card = ft.Column([
        ft.Text("Multi-Broker API & OAuth Gateway:", weight=ft.FontWeight.BOLD),
        ft.Row([broker_dropdown, api_key_input, api_secret_input], wrap=True),
        ft.Row([open_browser_button, bypass_button, request_token_input], wrap=True),
        broker_connection_status,
        ft.Divider()
    ])

    active_broker_session = {"broker": "Paper Trading", "api_key": "", "api_secret": "", "session_token": "", "connected": True}

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

    delta_profile_dropdown = ft.Dropdown(
        label="Options Strike Mapping Profile", width=220, value="ATM (Approx 0.5 Delta)",
        options=[
            ft.dropdown.Option("ITM (Approx 0.7 Delta)"),
            ft.dropdown.Option("ATM (Approx 0.5 Delta)"),
            ft.dropdown.Option("OTM (Approx 0.3 Delta)")
        ]
    )

    auto_refresh_checkbox = ft.Checkbox(label="Enable Auto-Refresh Scan", value=False)
    refresh_interval_input = ft.TextField(label="Interval (Sec)", value="2", width=110)

    ma_fast_input, ma_slow_input = ft.TextField(label="MA Fast", value="9", width=110), ft.TextField(label="MA Slow", value="21", width=110)
    ema_fast_input, ema_slow_input = ft.TextField(label="EMA Fast", value="9", width=110), ft.TextField(label="EMA Slow", value="21", width=110)
    rsi_period_input = ft.TextField(label="RSI Period", value="14", width=110)
    macd_fast_input, macd_slow_input = ft.TextField(label="MACD Fast", value="12", width=110), ft.TextField(label="MACD Slow", value="26", width=110)
    st_period_input, st_mult_input = ft.TextField(label="ST Period", value="10", width=110), ft.TextField(label="ST Multiplier", value="3.0", width=110)
    bb_period_input, bb_std_input = ft.TextField(label="BB Period", value="20", width=110), ft.TextField(label="BB Std Dev", value="2.0", width=110)

    atr_period_input = ft.TextField(label="ATR Period", value="14", width=110)
    atr_mult_dropdown = ft.Dropdown(label="ATR SL Multiplier", width=130, value="2.0x ATR", options=[ft.dropdown.Option("1.0x ATR"), ft.dropdown.Option("2.0x ATR"), ft.dropdown.Option("3.0x ATR")])
    target_rr_dropdown = ft.Dropdown(label="Target (R:R)", width=130, value="1:3", options=[ft.dropdown.Option("1:2"), ft.dropdown.Option("1:3"), ft.dropdown.Option("1:5")])
    
    max_daily_loss_input = ft.TextField(label="Max Daily Loss Limit (₹)", value="5000", width=170)
    max_open_pos_input = ft.TextField(label="Max Open Positions", value="3", width=140)
    scale_out_checkbox = ft.Checkbox(label="Enable Scale-Out Partial Profits (Book 50% at 1:1.5)", value=True)
    auto_squareoff_checkbox = ft.Checkbox(label="Intraday Square-Off at 3:15 PM", value=True)

    trailing_sl_checkbox = ft.Checkbox(label="Enable Trailing Stop Loss (TSL)", value=True)
    capital_input = ft.TextField(label="Initial Capital (₹)", value="100000", width=150)
    risk_pct_input = ft.TextField(label="Risk Per Trade (%)", value="1.0", width=130)
    
    auto_trade_checkbox = ft.Checkbox(label="Enable Live/Sandbox Broker Execution", value=False)
    scan_button = ft.Button(content=ft.Text("Scan Candles & Map Options Strikes"))

    backtest_symbol_input = ft.TextField(label="Backtest Symbol", value="RELIANCE.NS", width=150)
    backtest_period_dropdown = ft.Dropdown(label="Historical Period", width=140, value="6mo", options=[ft.dropdown.Option("1mo"), ft.dropdown.Option("3mo"), ft.dropdown.Option("6mo"), ft.dropdown.Option("1y")])
    backtest_button = ft.Button(content=ft.Text("Run Strategy Backtest"))
    backtest_results_text = ft.Text("Backtest Results: No test executed yet.", size=13, weight=ft.FontWeight.BOLD)

    breadth_summary_text = ft.Text("Market Breadth: Advancing Stocks: 0 | Declining Stocks: 0 | Advance/Decline Ratio: 0.00", size=13, weight=ft.FontWeight.BOLD)
    heatmap_container = ft.Row([], wrap=True)
    refresh_breadth_button = ft.Button(content=ft.Text("Refresh Sector Heatmap & Breadth"))

    portfolio_summary_text = ft.Text("Virtual Cash: ₹100,000.00 | Open Positions: 0 | Floating P&L: ₹0.00", size=15, weight=ft.FontWeight.BOLD, color="cyan")
    analytics_summary_text = ft.Text("Win Rate: 0.0% | Profit Factor: 0.0 | Expectancy: ₹0.00 | Max Drawdown: 0.0% | Total Realized P&L: ₹0.00", size=13, weight=ft.FontWeight.BOLD)
    circuit_breaker_status = ft.Text("Risk Circuit Breaker: NORMAL 🟢", size=14, weight=ft.FontWeight.BOLD, color="green")

    scan_results_table = ft.DataTable(columns=[
        ft.DataColumn(ft.Text("Symbol")), 
        ft.DataColumn(ft.Text("Spot")), 
        ft.DataColumn(ft.Text("Mapped Strike Contract")), 
        ft.DataColumn(ft.Text("Signal")), 
        ft.DataColumn(ft.Text("Lots/Qty")), 
        ft.DataColumn(ft.Text("Status")),
        ft.DataColumn(ft.Text("Rationale"))
    ], rows=[])
    
    explanation_card_content = ft.Markdown("Click on any stock symbol to view its historical chart, or click 'View Rationale' beside the status column to inspect its quantitative breakdown.")
    explanation_card = ft.Container(content=explanation_card_content, padding=15, bgcolor=ft.Colors.GREY_900, border_radius=8, margin=10)

    open_positions_table = ft.DataTable(columns=[ft.DataColumn(ft.Text("Symbol / Contract")), ft.DataColumn(ft.Text("Type")), ft.DataColumn(ft.Text("Entry Price")), ft.DataColumn(ft.Text("SL")), ft.DataColumn(ft.Text("Target")), ft.DataColumn(ft.Text("Current")), ft.DataColumn(ft.Text("Qty")), ft.DataColumn(ft.Text("P&L")), ft.DataColumn(ft.Text("Action"))], rows=[])
    trade_history_table = ft.DataTable(columns=[ft.DataColumn(ft.Text("Time")), ft.DataColumn(ft.Text("Symbol / Contract")), ft.DataColumn(ft.Text("Type")), ft.DataColumn(ft.Text("Entry")), ft.DataColumn(ft.Text("Exit")), ft.DataColumn(ft.Text("Qty")), ft.DataColumn(ft.Text("P&L")), ft.DataColumn(ft.Text("Reason"))], rows=[])
    status_text = ft.Text("", size=13)

    dashboard_container = ft.Column([
        ft.Divider(), ft.Text("🌐 Market Breadth & Sector Heatmap Dashboard:", size=16, weight=ft.FontWeight.BOLD),
        breadth_summary_text,
        refresh_breadth_button,
        heatmap_container,
        ft.Divider(), ft.Text("🧪 Backtesting & Strategy Performance Sandbox:", size=16, weight=ft.FontWeight.BOLD),
        ft.Row([backtest_symbol_input, backtest_period_dropdown, backtest_button], wrap=True),
        backtest_results_text,
        ft.Divider(), ft.Text("Watchlist & Timeframe Configs:", weight=ft.FontWeight.BOLD),
        watchlist_input,
        autocomplete_container,
        ft.Row([htf_dropdown, ltf_dropdown, delta_profile_dropdown], wrap=True),
        ft.Row([indicator_1_dropdown, indicator_2_dropdown], wrap=True),
        ft.Row([auto_refresh_checkbox, refresh_interval_input], wrap=True),
        ft.Divider(), ft.Text("Indicator Fine-Tuning:", weight=ft.FontWeight.BOLD),
        ft.Row([ma_fast_input, ma_slow_input, ema_fast_input, ema_slow_input], wrap=True),
        ft.Row([rsi_period_input, macd_fast_input, macd_slow_input], wrap=True),
        ft.Row([st_period_input, st_mult_input, bb_period_input, bb_std_input], wrap=True),
        ft.Divider(), ft.Text("Risk Management & Safety Controls:", weight=ft.FontWeight.BOLD),
        ft.Row([max_daily_loss_input, max_open_pos_input, scale_out_checkbox, auto_squareoff_checkbox], wrap=True),
        ft.Row([atr_period_input, atr_mult_dropdown, target_rr_dropdown, capital_input, risk_pct_input], wrap=True),
        ft.Row([trailing_sl_checkbox, auto_trade_checkbox, scan_button], wrap=True),
        circuit_breaker_status,
        ft.Divider(), ft.Text("📊 Active Portfolio & Option Positions:", size=16, weight=ft.FontWeight.BOLD),
        portfolio_summary_text, open_positions_table,
        ft.Divider(), ft.Text("📈 Advanced Risk & Portfolio Analytics:", size=14, weight=ft.FontWeight.BOLD),
        analytics_summary_text,
        ft.Divider(),
        ft.Text("💡 Selected Indicator & Algorithmic Rationale:", weight=ft.FontWeight.BOLD), explanation_card,
        ft.Text("📊 Broker Candle Scan & Automated Strike Results:", size=14, weight=ft.FontWeight.BOLD), scan_results_table,
        ft.Divider(), ft.Text("📜 Persistent Trade History Ledger:", size=14, weight=ft.FontWeight.BOLD), trade_history_table,
        status_text
    ], visible=True)

    def update_heatmap_dashboard(e=None):
        heatmap_container.controls.clear()
        advances = 0
        declines = 0

        for sector_name, sym_list in SECTOR_MAP.items():
            sector_returns = []
            for sym in sym_list:
                try:
                    df = yf.download(sym, period="2d", interval="1d", progress=False)
                    if not df.empty:
                        if isinstance(df.columns, pd.MultiIndex):
                            df = df.xs(sym, level=1, axis=1)
                        ret = ((df["Close"].iloc[-1] - df["Close"].iloc[-2]) / df["Close"].iloc[-2]) * 100.0
                        sector_returns.append(ret)
                        if ret >= 0:
                            advances += 1
                        else:
                            declines += 1
                except Exception:
                    pass

            avg_sector_ret = sum(sector_returns) / len(sector_returns) if sector_returns else 0.0
            bg_color = ft.Colors.GREEN_800 if avg_sector_ret >= 0 else ft.Colors.RED_800

            card = ft.Container(
                content=ft.Column([
                    ft.Text(sector_name, weight=ft.FontWeight.BOLD, color="white", size=13),
                    ft.Text(f"Avg Change: {avg_sector_ret:+.2f}%", size=12, color="white")
                ], alignment=ft.MainAxisAlignment.CENTER, horizontal_alignment=ft.CrossAxisAlignment.CENTER),
                bgcolor=bg_color,
                padding=10,
                border_radius=8,
                width=180,
                height=70
            )
            heatmap_container.controls.append(card)

        total_breadth = advances + declines
        ratio = (advances / declines) if declines > 0 else float(advances)
        breadth_summary_text.value = f"Market Breadth: Advancing Stocks: {advances} | Declining Stocks: {declines} | Advance/Decline Ratio: {ratio:.2f}"
        page.update()

    refresh_breadth_button.on_click = update_heatmap_dashboard

    def run_strategy_backtest(e):
        sym = backtest_symbol_input.value.strip()
        period = backtest_period_dropdown.value
        backtest_results_text.value = f"Running backtest for {sym} over {period}..."
        page.update()

        df = yf.download(tickers=sym, period=period, interval="1d", auto_adjust=False, progress=False)
        if df.empty:
            backtest_results_text.value = f"Backtest failed: No historical data returned for {sym}."
            page.update()
            return

        if isinstance(df.columns, pd.MultiIndex):
            df = df.xs(sym, level=1, axis=1)

        params = {
            "ma_fast": int(ma_fast_input.value or 9),
            "ma_slow": int(ma_slow_input.value or 21),
            "rsi_period": int(rsi_period_input.value or 14),
            "bb_window": int(bb_period_input.value or 20),
            "bb_std": float(bb_std_input.value or 2.0),
            "st_period": int(st_period_input.value or 10),
            "st_mult": float(st_mult_input.value or 3.0)
        }
        df_ind = process_indicators(df, params)

        trades = []
        in_position = False
        entry_price = 0.0

        for i in range(1, len(df_ind)):
            prev, curr = df_ind.iloc[i - 1], df_ind.iloc[i]
            price = float(curr["Close"])

            sig, _ = evaluate_breakout_indicator(indicator_1_dropdown.value, curr, prev, price)

            if not in_position and sig == "BUY":
                in_position = True
                entry_price = price
            elif in_position and sig == "SELL":
                pnl = price - entry_price
                trades.append(pnl)
                in_position = False

        if in_position:
            trades.append(float(df_ind.iloc[-1]["Close"]) - entry_price)

        if not trades:
            backtest_results_text.value = f"Backtest finished: 0 trades triggered for {sym} using current parameters."
            page.update()
            return

        wins = [t for t in trades if t > 0]
        losses = [t for t in trades if t <= 0]
        win_rate = (len(wins) / len(trades)) * 100.0
        total_pnl = sum(trades)

        backtest_results_text.value = (
            f"Backtest Results for {sym} ({len(trades)} trades): "
            f"Win Rate: {win_rate:.1f}% | Total Points Captured: {total_pnl:+.2f} pts | "
            f"Avg Profit/Loss per trade: {total_pnl/len(trades):+.2f} pts"
        )
        page.update()

    backtest_button.on_click = run_strategy_backtest

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
            if price > latest["BB_HIGH"]:
                return "BUY", "Bollinger Band Upper Breakout"
            elif price < latest["BB_LOW"]:
                return "SELL", "Bollinger Band Lower Breakdown"
        return "NEUTRAL", "No breakout detected"

    def connect_broker(e):
        selected = broker_dropdown.value
        active_broker_session["broker"] = selected
        active_broker_session["api_key"] = api_key_input.value
        active_broker_session["api_secret"] = api_secret_input.value

        if selected == "Paper Trading":
            active_broker_session["connected"] = True
            broker_connection_status.value = "Connected to Paper Trading Sandbox 🟢"
            broker_connection_status.color = "green"
            dashboard_container.visible = True
            update_heatmap_dashboard()
            page.update()
            return

        app_key = api_key_input.value.strip()
        if not app_key:
            broker_connection_status.value = "API Key / Client ID cannot be empty 🔴"
            broker_connection_status.color = "red"
            page.update()
            return

        if selected == "Zerodha Kite":
            auth_url = f"https://kite.trade/connect/login?api_key={app_key}&v=3"
            webbrowser.open(auth_url)
            broker_connection_status.value = "Browser opened for Zerodha login. Complete login and paste the request token."
            broker_connection_status.color = "orange"
            page.update()
            return

        token = request_token_input.value
        if not token and oauth_callback_store["request_token"]:
            token = oauth_callback_store["request_token"]
            request_token_input.value = token

        success, sess_token, msg = exchange_token_and_verify(
            selected, api_key_input.value, api_secret_input.value, token
        )
        if success:
            active_broker_session["session_token"] = sess_token
            active_broker_session["connected"] = True
            broker_connection_status.value = msg
            broker_connection_status.color = "green"
            dashboard_container.visible = True
            update_heatmap_dashboard()
        else:
            active_broker_session["connected"] = False
            broker_connection_status.value = msg
            broker_connection_status.color = "red"
        page.update()

    def bypass_login(e):
        active_broker_session["broker"] = "Paper Trading"
        active_broker_session["connected"] = True
        broker_connection_status.value = "Bypassed Authentication (Paper Mode Active) 🟢"
        broker_connection_status.color = "green"
        dashboard_container.visible = True
        update_heatmap_dashboard()
        page.update()

    open_browser_button.on_click = connect_broker
    bypass_button.on_click = bypass_login

    def open_lightweight_chart(symbol):
        df = scan_data_cache.get(symbol)
        if df is None or df.empty:
            status_text.value = f"No candle data cached for {symbol} to display chart."
            page.update()
            return

        chart = Chart(toolbox=True)
        chart.set(df.reset_index())
        chart.show(block=False)

    def render_tables():
        now_time = datetime.now().time()
        square_off_limit = time(15, 15)
        if auto_squareoff_checkbox.value and now_time >= square_off_limit and portfolio_state["positions"]:
            for pos in list(portfolio_state["positions"]):
                close_position(pos, reason="Intraday 3:15 PM Auto Square-Off")

        open_positions_table.rows.clear()
        total_pnl = 0.0
        for pos in portfolio_state["positions"]:
            unrealized = (pos["current_price"] - pos["entry_price"]) * pos["qty"]
            total_pnl += unrealized

            if scale_out_checkbox.value and pos.get("qty", 0) > 50:
                mid_target = pos["entry_price"] + ((pos["target_price"] - pos["entry_price"]) * 0.5)
                if pos["current_price"] >= mid_target and not pos.get("scaled_out", False):
                    pos["scaled_out"] = True
                    half_qty = pos["qty"] // 2
                    partial_pnl = (mid_target - pos["entry_price"]) * half_qty
                    portfolio_state["cash"] += (mid_target * half_qty)
                    pos["qty"] -= half_qty
                    
                    trade_record = {
                        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "symbol": pos["symbol"],
                        "option_type": pos["option_type"],
                        "entry_price": pos["entry_price"],
                        "exit_price": mid_target,
                        "qty": half_qty,
                        "realized_pnl": partial_pnl,
                        "reason": "Scale-Out Partial Target 50%"
                    }
                    portfolio_state["history"].append(trade_record)
                    db_log_closed_trade(trade_record)
            
            def make_close_handler(p=pos):
                return lambda ev: close_position(p)

            open_positions_table.rows.append(
                ft.DataRow(cells=[
                    ft.DataCell(ft.Text(pos["symbol"])),
                    ft.DataCell(ft.Text(pos["option_type"])),
                    ft.DataCell(ft.Text(f"₹{pos['entry_price']:.2f}")),
                    ft.DataCell(ft.Text(f"₹{pos['stop_loss']:.2f}")),
                    ft.DataCell(ft.Text(f"₹{pos['target_price']:.2f}")),
                    ft.DataCell(ft.Text(f"₹{pos['current_price']:.2f}")),
                    ft.DataCell(ft.Text(str(pos["qty"]))),
                    ft.DataCell(ft.Text(f"₹{unrealized:.2f}", color="green" if unrealized >= 0 else "red")),
                    ft.DataCell(ft.Button(content=ft.Text("Exit"), on_click=make_close_handler()))
                ])
            )

        trade_history_table.rows.clear()
        history = portfolio_state["history"]
        for trade in history:
            pnl = trade["realized_pnl"]
            trade_history_table.rows.append(
                ft.DataRow(cells=[
                    ft.DataCell(ft.Text(trade["time"])),
                    ft.DataCell(ft.Text(trade["symbol"])),
                    ft.DataCell(ft.Text(trade["option_type"])),
                    ft.DataCell(ft.Text(f"₹{trade['entry_price']:.2f}")),
                    ft.DataCell(ft.Text(f"₹{trade['exit_price']:.2f}")),
                    ft.DataCell(ft.Text(str(trade["qty"]))),
                    ft.DataCell(ft.Text(f"₹{pnl:.2f}", color="green" if pnl >= 0 else "red")),
                    ft.DataCell(ft.Text(trade["reason"]))
                ])
            )

        init_cap = float(capital_input.value or 100000)
        win_rate, profit_factor, expectancy, max_dd_pct, total_realized = compute_advanced_analytics(history, init_cap)
        
        max_daily_loss = float(max_daily_loss_input.value or 5000)
        if total_realized + total_pnl <= -max_daily_loss:
            circuit_breaker_status.value = "Risk Circuit Breaker: TRIGGERED (Max Daily Loss Reached) 🛑"
            circuit_breaker_status.color = "red"
        else:
            circuit_breaker_status.value = "Risk Circuit Breaker: NORMAL 🟢"
            circuit_breaker_status.color = "green"

        portfolio_summary_text.value = f"Virtual Cash: ₹{portfolio_state['cash']:.2f} | Open Positions: {len(portfolio_state['positions'])} | Floating P&L: ₹{total_pnl:.2f}"
        analytics_summary_text.value = (
            f"Win Rate: {win_rate:.1f}%  |  "
            f"Profit Factor: {profit_factor:.2f}  |  "
            f"Expectancy: ₹{expectancy:.2f} per trade  |  "
            f"Max Drawdown: {max_dd_pct:.2f}%  |  "
            f"Realized P&L: ₹{total_realized:.2f}"
        )

    def close_position(pos, reason="Manual Exit"):
        pnl = (pos["current_price"] - pos["entry_price"]) * pos["qty"]
        portfolio_state["cash"] += (pos["current_price"] * pos["qty"])
        if pos in portfolio_state["positions"]:
            portfolio_state["positions"].remove(pos)
        db_remove_open_position(pos["symbol"], pos["option_type"])

        trade_record = {
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "symbol": pos["symbol"],
            "option_type": pos["option_type"],
            "entry_price": pos["entry_price"],
            "exit_price": pos["current_price"],
            "qty": pos["qty"],
            "realized_pnl": pnl,
            "reason": reason
        }
        portfolio_state["history"].append(trade_record)
        db_log_closed_trade(trade_record)
        render_tables()
        page.update()

    def run_scan_engine(e=None):
        init_cap = float(capital_input.value or 100000)
        _, _, _, _, total_realized = compute_advanced_analytics(portfolio_state["history"], init_cap)
        max_daily_loss = float(max_daily_loss_input.value or 5000)
        if total_realized <= -max_daily_loss:
            status_text.value = "Scan blocked! Max Daily Loss circuit breaker is active 🛑"
            page.update()
            return

        max_allowed_pos = int(max_open_pos_input.value or 3)
        if len(portfolio_state["positions"]) >= max_allowed_pos:
            status_text.value = f"Scan blocked! Max open positions limit ({max_allowed_pos}) reached."
            page.update()
            return

        status_text.value = "Fetching candle data, calculating indicators, and mapping option strikes..."
        page.update()

        watchlist = [s.strip() for s in watchlist_input.value.split(",") if s.strip()]
        params = {
            "ma_fast": int(ma_fast_input.value or 9),
            "ma_slow": int(ma_slow_input.value or 21),
            "ema_fast": int(ema_fast_input.value or 9),
            "ema_slow": int(ema_slow_input.value or 21),
            "rsi_period": int(rsi_period_input.value or 14),
            "macd_fast": int(macd_fast_input.value or 12),
            "macd_slow": int(macd_slow_input.value or 26),
            "st_period": int(st_period_input.value or 10),
            "st_mult": float(st_mult_input.value or 3.0),
            "bb_window": int(bb_period_input.value or 20),
            "bb_std": float(bb_std_input.value or 2.0),
            "atr_period": int(atr_period_input.value or 14)
        }

        scan_results_table.rows.clear()

        for sym in watchlist:
            df_htf = fetch_broker_candles(active_broker_session, sym, "1mo", "1d")
            df_ltf = fetch_broker_candles(active_broker_session, sym, "5d", "5m")

            if df_htf.empty or df_ltf.empty:
                continue

            df_htf = process_indicators(df_htf, params)
            df_ltf = process_indicators(df_ltf, params)

            if len(df_ltf) < 2 or len(df_htf) < 2:
                continue

            scan_data_cache[sym] = df_ltf

            latest_ltf, prev_ltf = df_ltf.iloc[-1], df_ltf.iloc[-2]
            latest_htf = df_htf.iloc[-1]
            spot_price = float(latest_ltf["Close"])

            htf_trend = "BULLISH" if latest_htf["MA_FAST"] > latest_htf["MA_SLOW"] else "BEARISH"

            sig1, r1 = evaluate_breakout_indicator(indicator_1_dropdown.value, latest_ltf, prev_ltf, spot_price)
            sig2, r2 = ("NEUTRAL", "None") if indicator_2_dropdown.value == "None" else evaluate_breakout_indicator(indicator_2_dropdown.value, latest_ltf, prev_ltf, spot_price)

            signal = "HOLD"
            rationale = "No aligned breakout."

            if sig1 == "BUY" and (sig2 in ["BUY", "NEUTRAL"]) and htf_trend == "BULLISH":
                signal = "BUY CALL (CE)"
                rationale = f"{r1} | HTF Trend is Bullish"
            elif sig1 == "SELL" and (sig2 in ["SELL", "NEUTRAL"]) and htf_trend == "BEARISH":
                signal = "BUY PUT (PE)"
                rationale = f"{r1} | HTF Trend is Bearish"

            lot_qty = get_lot_size(sym)
            status_str = "Scanned"
            mapped_contract_display = sym

            if signal != "HOLD":
                option_type = "CE" if "CALL" in signal else "PE"
                contract_sym, strike_val, opt_premium = select_dynamic_option_contract(
                    sym, spot_price, option_type, delta_profile_dropdown.value
                )
                mapped_contract_display = contract_sym

                if auto_trade_checkbox.value:
                    atr_val = float(latest_ltf["ATR"])
                    mult = float(atr_mult_dropdown.value.split("x")[0])
                    sl_dist = atr_val * mult
                    
                    entry_price = opt_premium
                    sl_price = round_to_tick_size(max(1.0, entry_price - (sl_dist * 0.05)))
                    target_ratio = float(target_rr_dropdown.value.split(":")[1])
                    target_price = round_to_tick_size(entry_price + ((entry_price - sl_price) * target_ratio))

                    if active_broker_session["broker"] != "Paper Trading" and active_broker_session["connected"]:
                        success, msg, gtt_id = place_live_broker_order(
                            active_broker_session["broker"], active_broker_session["api_key"],
                            active_broker_session["session_token"], contract_sym, "BUY", lot_qty, entry_price, sl_price, target_price
                        )
                        status_str = "Executed Live" if success else "Execution Failed"
                    else:
                        new_pos = {
                            "symbol": contract_sym, "option_type": option_type, "spot_at_entry": spot_price,
                            "entry_price": entry_price, "current_price": entry_price,
                            "stop_loss": sl_price, "target_price": target_price,
                            "qty": lot_qty, "gtt_id": None
                        }
                        portfolio_state["positions"].append(new_pos)
                        db_save_open_position(new_pos)
                        status_str = "Executed Virtual"

            def make_chart_handler(s=sym):
                return lambda ev: open_lightweight_chart(s)

            def make_rationale_handler(r=rationale):
                return lambda ev: update_rationale_display(r)

            scan_results_table.rows.append(
                ft.DataRow(cells=[
                    ft.DataCell(ft.Text(sym, color="cyan"), on_click=make_chart_handler()),
                    ft.DataCell(ft.Text(f"₹{spot_price:.2f}")),
                    ft.DataCell(ft.Text(mapped_contract_display, weight=ft.FontWeight.BOLD)),
                    ft.DataCell(ft.Text(signal, weight=ft.FontWeight.BOLD)),
                    ft.DataCell(ft.Text(str(lot_qty))),
                    ft.DataCell(ft.Text(status_str)),
                    ft.DataCell(ft.Button(content=ft.Text("View Rationale"), on_click=make_rationale_handler()))
                ])
            )

        render_tables()
        status_text.value = f"Scan complete at {datetime.now().strftime('%H:%M:%S')}."
        page.update()

    def update_rationale_display(r_text):
        explanation_card_content.value = f"### Quant Breakdown\n{r_text}"
        page.update()

    scan_button.on_click = run_scan_engine

    async def auto_refresh_background_loop():
        while True:
            await asyncio.sleep(1)
            if auto_refresh_checkbox.value and dashboard_container.visible:
                try:
                    interval = float(refresh_interval_input.value or 2)
                except ValueError:
                    interval = 2.0
                
                await asyncio.sleep(interval)
                if auto_refresh_checkbox.value:
                    try:
                        run_scan_engine()
                        update_heatmap_dashboard()
                    except Exception:
                        pass

    page.add(
        header,
        subtitle,
        login_card,
        dashboard_container
    )
    
    render_tables()
    update_heatmap_dashboard()
    
    threading.Thread(target=start_local_server, daemon=True).start()
    page.run_task(auto_refresh_background_loop)


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
