# app.py — Teapplix HD LTL BOL 產生器 + 推送前人工修改（整合 importorder.py 可用版本 & 修正成功偵測）
import os
import io
import zipfile
from datetime import datetime, timedelta

import requests
import streamlit as st
import re

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

from dotenv import load_dotenv
import json
import csv
import base64
from typing import List, Dict, Any, Optional
import fitz  # PyMuPDF

# ------------------------------------------------
# 基本設定
# ------------------------------------------------
APP_TITLE = "HD LTL / BOL 產生 + WMS 手動覆核後上傳"
st.set_page_config(page_title=APP_TITLE, layout="wide")

load_dotenv()

TEAPPLIX_TOKEN = os.getenv("TEAPPLIX_TOKEN", "")
PASSWORD = os.getenv("APP_PASSWORD", "")
TIMEZONE = os.getenv("APP_TZ", "America/Phoenix")

BASE_URL = "https://teapplix.com/api2/api.php"
HEADERS = {"User-Agent": "FestivalNeo-Tools/1.0"}

# ------------------------------------------------
# WAREHOUSE 配置（你可改成自己的環境變數）
# ------------------------------------------------
def _sec(key: str, default: str = "") -> str:
    return os.getenv(key, default)

WAREHOUSE_ADDR = {
    "CA 91789": {
        "name": _sec("W1_NAME", "Festival Neo CA"),
        "addr": _sec("W1_ADDR", "5500 Mission Blvd"),
        "citystatezip": _sec("W1_CITYSTATEZIP", "Montclair, CA 91763"),
        "sid": _sec("W1_SID", "CA-001"),
    },
    "NJ 08816": {
        "name": _sec("W2_NAME", "Festival Neo NJ"),
        "addr": _sec("W2_ADDR", "10 Main St"),
        "citystatezip": _sec("W2_CITYSTATEZIP", "East Brunswick, NJ 08816"),
        "sid": _sec("W2_SID", "NJ-001"),
    },
}

# WMS 設定（倉庫對應）
WMS_CONFIGS = {
    "CA 91789": {
        "WAREHOUSE_CODE": _sec("WMS_CA_CODE", "CA_MONTCLAIR"),
        "ENDPOINT_URL": _sec("WMS_CA_URL", "https://api.example.com/ca"),
        "APP_TOKEN": _sec("WMS_CA_APP_TOKEN", ""),
        "APP_KEY": _sec("WMS_CA_APP_KEY", ""),
    },
    "NJ 08816": {
        "WAREHOUSE_CODE": _sec("WMS_NJ_CODE", "NJ_EASTBRUNSWICK"),
        "ENDPOINT_URL": _sec("WMS_NJ_URL", "https://api.example.com/nj"),
        "APP_TOKEN": _sec("WMS_NJ_APP_TOKEN", ""),
        "APP_KEY": _sec("WMS_NJ_APP_KEY", ""),
    },
}

# ------------------------------------------------
# 小工具
# ------------------------------------------------
def get_headers():
    return HEADERS

def _tznow():
    try:
        tz = ZoneInfo(TIMEZONE)
    except Exception:
        tz = None
    return datetime.now(tz)

def _fmt_date(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")

def _fmt_datetime(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def _safe_int(x, default=0):
    try:
        return int(x)
    except Exception:
        return default

def _str(x):
    return "" if x is None else str(x)

# ------------------------------------------------
# TEAPPLIX 介面
# ------------------------------------------------
def teapplix_orders_query(po_list: List[str], paid_since_days: int = 14, shipped: str = "0") -> List[dict]:
    """
    依 PO 清單抓取訂單（PaymentDate 在最近 N 天）
    shipped: "0" = 未出貨, "1" = 已出貨, 其它 = 不過濾
    """
    if not TEAPPLIX_TOKEN:
        raise RuntimeError("TEAPPLIX_TOKEN 未設定")

    end = _tznow()
    start = end - timedelta(days=paid_since_days)
    ps = _fmt_date(start)
    pe = _fmt_date(end)

    results: List[dict] = []
    for oid in po_list:
        params = {
            "token": TEAPPLIX_TOKEN,
            "call": "GetTransactions",
            "Format": "json",
            "ResultCount": "1",
            "OriginalTxnId": oid,
            "PaymentDateStart": ps,
            "PaymentDateEnd": pe,
        }
        if shipped in ("0", "1"):
            params["Shipped"] = shipped
        try:
            r = requests.get(BASE_URL, headers=get_headers(), params=params, timeout=45)
        except Exception as e:
            st.error(f"PO {oid} 連線錯誤：{e}"); continue
        if r.status_code != 200:
            st.error(f"PO {oid} API 錯誤: {r.status_code}\n{r.text[:400]}"); continue
        try:
            data = r.json()
        except Exception:
            st.error(f"PO {oid} 回傳非 JSON：{r.text[:400]}"); continue

        txns = data.get("Transactions", {}).get("Transactions", [])
        if isinstance(txns, dict):
            txns = [txns]
        # 只取 OriginalTxnId = oid 的
        matched = [t for t in txns if _str(t.get("OriginalTxnId")) == _str(oid)]
        if not matched:
            st.warning(f"PO {oid} 沒找到對應訂單（或超過查詢時窗）")
            continue
        # 按時間排序取最新
        def _pdt(t):
            s = t.get("PaymentDate") or t.get("OrderDate") or ""
            try:
                return datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S")
            except Exception:
                return datetime.min
        matched.sort(key=_pdt, reverse=True)
        results.append(matched[0])
    return results

# ------------------------------------------------
# SCAC / Carrier 轉換（表單輸入 → 標準碼）
# ------------------------------------------------
SCAC_MAP = {
    "RL": "R+L",
    "R+L": "R+L",
    "R&L": "R+L",
    "R L": "R+L",
    "R L CARRIERS": "R+L",
    "ROADRUNNER": "ROADRUNNER",
    "ROAD RUNNER": "ROADRUNNER",
    "XPO": "XPO",
    "SAIA": "SAIA",
    "FEDEX": "FEDEX",
    "UPS": "UPS",
}

def normalize_scac(scac: str) -> str:
    s = (scac or "").strip().upper()
    return SCAC_MAP.get(s, scac)

# ------------------------------------------------
# BOL 產生（範例：從欄位帶入 PDF）
# ------------------------------------------------
BOL_TEMPLATE_PDF = os.getenv("BOL_TEMPLATE_PDF", "")

def fill_bol_pdf(fields: dict, out_path: str):
    """
    使用 PyMuPDF 將文字寫入至 BOL 模板，僅示意（實務上改為對應欄位座標）
    """
    if not BOL_TEMPLATE_PDF or not os.path.exists(BOL_TEMPLATE_PDF):
        raise FileNotFoundError("BOL 模板不存在，請設定 BOL_TEMPLATE_PDF 環境變數")

    doc = fitz.open(BOL_TEMPLATE_PDF)
    page = doc[0]
    # 範例把幾個欄位寫上去（可自訂座標）
    page.insert_text((72, 72), f"Carrier: {fields.get('carrier','')}")
    page.insert_text((72, 96), f"SCAC: {fields.get('scac','')}")
    page.insert_text((72, 120), f"Pickup: {fields.get('pickup_date','')}")
    page.insert_text((72, 144), f"Ship From: {fields.get('ship_from','')}")
    page.insert_text((72, 168), f"Ship To: {fields.get('ship_to','')}")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    doc.save(out_path, deflate=True)
    doc.close()

# ------------------------------------------------
# WMS 需求：參數組裝 & 上傳
# ------------------------------------------------
def _aggregate_items_by_sku(group):
    sku_qty = {}
    for od in group:
        items = od.get("OrderItems") or []
        if isinstance(items, dict):
            items = [items]
        for it in items:
            sku = (it.get("ItemSKU") or "").strip()
            if not sku:
                continue
            try:
                q = int(it.get("Quantity") or 0)
            except Exception:
                q = 0
            if q <= 0:
                continue
            sku_qty[sku] = sku_qty.get(sku, 0) + q
    items_arr = [{"product_sku": sku, "quantity": qty} for sku, qty in sku_qty.items()]
    return items_arr

def _compute_shipping_method(wh_key: str, items: list[dict]) -> str:
    """
    根據倉別與 SKU/數量決定 shipping_method。
    - NJ 08816 : 一律 CUSTOMER_SHIP
    - CA 91789 : 只有一個 SKU 且 quantity == 1 -> SELF_LTL-SINGLE，否則 ALL_SELF_LTL
    其他倉（若未定義）預設 CUSTOMER_SHIP。
    """
    wh = (wh_key or "").strip()
    if wh == "NJ 08816":
        return "CUSTOMER_SHIP"
    if wh == "CA 91789":
        valid_items = [it for it in (items or []) if int(it.get("quantity", 0)) > 0]
        if len(valid_items) == 1:
            only = valid_items[0]
            try:
                qty = int(only.get("quantity", 0))
            except Exception:
                qty = 0
            if qty == 1:
                return "SELF_LTL-SINGLE"
        return "ALL_SELF_LTL"
    return "CUSTOMER_SHIP"

def build_wms_params_from_group(oid: str, group: list, wh_key: str, pickup_date_str: str) -> dict:
    first = group[0]
    to = first.get("To") or {}
    od = first.get("OrderDetails") or {}

    province = (to.get("State") or "").strip()
    city = (to.get("City") or "").strip()
    street = (to.get("Street") or "").strip()
    street2 = (to.get("Street2") or "").strip()
    zipcode = (to.get("ZipCode") or "").strip()
    company = (to.get("Company") or "").strip()
    name = (to.get("Name") or "").strip()
    phone = (to.get("PhoneNumber") or "").strip()
    shipclass = (od.get("ShipClass") or "").strip()

    items = _aggregate_items_by_sku(group)
    shipping_method = _compute_shipping_method(wh_key, items)
    test_oid = f"test1-{oid}".strip()

    params = {
        "platform": "OTHER",
        "allocated_auto": "0",
        "warehouse_code": WMS_CONFIGS.get(wh_key, {}).get("WAREHOUSE_CODE", ""),
        "shipping_method": shipping_method,
        "reference_no": test_oid,                     # 測試：test- + PO
        "order_desc": f"pick up: {pickup_date_str}" if pickup_date_str else "",
        "remark": "",
        "country_code": "US",
        "province": province,
        "city": city,
        "district": city,
        "address1": street,
        "address2": street2,
        "address3": "",
        "zipcode": zipcode,
        "company": company,
        "name": name,
        "phone": phone,
        "cell_phone": "",
        "phone_extension": "",
        "email": "",
        "platform_shop": shipclass,
        "items": items,
        "tracking_no": test_oid,                      # 測試：test- + PO
    }
    return params

def send_create_order(endpoint_url: str, app_token: str, app_key: str, params: dict, service: str = "createOrder") -> requests.Response:
    """
    通用：送 WMS 建單
    """
    url = endpoint_url
    payload = {
        "service": service,
        "app_token": app_token,
        "app_key": app_key,
        "data": json.dumps(params),
    }
    return requests.post(url, data=payload, timeout=60)

# ------------------------------------------------
# 介面：密碼保護
# ------------------------------------------------
with st.sidebar:
    input_pwd = st.text_input("密碼", type="password")

if input_pwd != PASSWORD:
    st.warning("請輸入正確密碼後才能使用。")
    st.stop()

st.title(APP_TITLE)

# 說明
st.markdown("""
**說明：**
1. 可能會錯, 請仔細核對
2. ABCD
""")

if not TEAPPLIX_TOKEN:
    st.error("找不到 TEAPPLIX_TOKEN，請在 .env 設定 TEAPPLIX_TOKEN")
    st.stop()

# ------------------------------------------------
# 抓取訂單區
# ------------------------------------------------
st.header("抓取訂單 / 搜尋 PO")
col1, col2 = st.columns([2, 1])
with col1:
    po_text = st.text_area("輸入 PO（每行一個 OriginalTxnId）", height=160, placeholder="例如：\nHD123456\nHD789012")
with col2:
    days = st.number_input("付款日往回查幾天", min_value=1, max_value=60, value=14, step=1)
    shipped_filter = st.selectbox("是否出貨", options=[("未出貨", "0"), ("已出貨", "1"), ("全部", "all")], index=0, format_func=lambda x: x[0])
    do_fetch = st.button("抓取訂單")

orders = []
if do_fetch:
    ids = [s.strip() for s in po_text.splitlines() if s.strip()]
    if not ids:
        st.warning("請輸入至少一個 PO")
    else:
        try:
            orders = teapplix_orders_query([i for i in ids], paid_since_days=int(days), shipped=shipped_filter[1])
        except Exception as e:
            st.error(f"抓取失敗：{e}")

if not do_fetch:
    st.info("請先在左側按『抓取訂單』或『搜尋 PO（14 天內）』。")

# ------------------------------------------------
# 顯示與處理每筆訂單
# ------------------------------------------------
if orders:
    st.header("結果 / 編輯 & 產出 / 上傳 WMS")
    for rec in orders:
        oid = rec.get("OriginalTxnId", "")
        st.subheader(f"PO: {oid}")

        # -- 收件/出貨資訊 --
        to = rec.get("To") or {}
        od = rec.get("OrderDetails") or {}
        shipclass = (od.get("ShipClass") or "").strip()

        colA, colB, colC = st.columns(3)
        with colA:
            wh_key = st.selectbox("Warehouse（來源倉）", list(WAREHOUSE_ADDR.keys()), index=0, key=f"wh_{oid}")
            pickup_date = st.date_input("Pickup 日期", value=_tznow().date(), key=f"pick_{oid}")
        with colB:
            carrier = st.text_input("Carrier（顯示用）", value="", key=f"car_{oid}")
            scac = st.text_input("SCAC（標準碼）", value="", key=f"scac_{oid}")
        with colC:
            tracking = st.text_input("Tracking No.", value=f"test1-{oid}", key=f"trk_{oid}")  # 預設 test
            reference_no = st.text_input("Reference No.", value=f"test1-{oid}", key=f"ref_{oid}")  # 預設 test

        # -- BOL 匯出（範例使用 PDF） --
        if st.button("🧾 產生 BOL PDF", key=f"bol_{oid}"):
            try:
                fields = {
                    "carrier": carrier,
                    "scac": normalize_scac(scac),
                    "pickup_date": str(pickup_date),
                    "ship_from": f"{WAREHOUSE_ADDR[wh_key]['name']} / {WAREHOUSE_ADDR[wh_key]['addr']} / {WAREHOUSE_ADDR[wh_key]['citystatezip']}",
                    "ship_to": f"{to.get('Name','')} / {to.get('Street','')} / {to.get('City','')} {to.get('State','')} {to.get('ZipCode','')}",
                }
                out_path = f"/tmp/BOL_{oid}.pdf"
                fill_bol_pdf(fields, out_path)
                with open(out_path, "rb") as f:
                    b = f.read()
                st.download_button("下載 BOL PDF", data=b, file_name=f"BOL_{oid}.pdf", mime="application/pdf")
            except Exception as e:
                st.error(f"BOL 產生失敗：{e}")

        # -- 匯入 WMS（先組資料） --
        group = [rec]  # 若同 PO 拆多筆，這裡可以放同組
        p = build_wms_params_from_group(oid, group, wh_key, str(pickup_date))

        st.markdown("**WMS 建單參數（可覆核）：**")
        st.code(json.dumps(p, indent=2, ensure_ascii=False))

        with st.expander("（可選）覆核/修改後再上傳 WMS"):
            col1, col2 = st.columns(2)
            with col1:
                new_wh_code = st.text_input("warehouse_code", value=p.get("warehouse_code",""), key=f"nw_{oid}")
                new_tracking = st.text_input("tracking_no", value=p.get("tracking_no",""), key=f"nt_{oid}")
                new_ref = st.text_input("reference_no", value=p.get("reference_no",""), key=f"nr_{oid}")
                new_platform_shop = st.text_input("platform_shop", value=p.get("platform_shop",""), key=f"ps_{oid}")
            with col2:
                new_pickup_date = st.date_input("（覆核）pickup 日期", value=_tznow().date(), key=f"np_{oid}")
                new_remark = st.text_input("remark", value=p.get("remark",""), key=f"rm_{oid}")

            st.markdown("**Items（可修改）**")
            new_items = []
            for idx, it in enumerate(p.get("items", [])):
                colx, coly = st.columns([2, 1])
                with colx:
                    new_sku = st.text_input(f"SKU #{idx+1}", value=it.get("product_sku",""), key=f"{oid}_sku_{idx}")
                with coly:
                    new_qty = st.number_input(f"quantity #{idx+1}", value=int(it.get("quantity",1)), min_value=1, step=1, key=f"{oid}_qty_{idx}")
                new_items.append({"product_sku": new_sku.strip(), "quantity": int(new_qty)})

            if st.button("📤 送出此筆", key=f"send_{oid}"):
                new_order_desc = f"pick up: {new_pickup_date.isoformat()}"
                new_params = dict(p)
                new_params.update({
                    "warehouse_code": new_wh_code.strip(),
                    "tracking_no": new_tracking.strip(),
                    "reference_no": new_ref.strip(),
                    "order_desc": new_order_desc,
                    "platform_shop": new_platform_shop.strip(),
                    "remark": new_remark,
                    "items": new_items,
                })

                # 由 warehouse_code 反查倉別鍵（或保留原來選的倉）
                target_wh_key = None
                for k, cfg in WMS_CONFIGS.items():
                    if cfg.get("WAREHOUSE_CODE") == new_params.get("warehouse_code"):
                        target_wh_key = k
                        break
                if not target_wh_key:
                    target_wh_key = rec.get("Warehouse", "NJ 08816")

                                        # 依最新倉別 + items 重新計算 shipping_method
                new_params["shipping_method"] = _compute_shipping_method(target_wh_key, new_params.get("items") or [])

                cfg = WMS_CONFIGS.get(target_wh_key, {})
                endpoint = cfg.get("ENDPOINT_URL","").strip()
                app_token = cfg.get("APP_TOKEN","").strip()
                app_key = cfg.get("APP_KEY","").strip()

                if not (endpoint and app_token and app_key):
                    st.error(f"{target_wh_key} WMS 設定不完整（endpoint/app_token/app_key）。")
                else:
                    try:
                        resp2 = send_create_order(endpoint, app_token, app_key, new_params, service="createOrder")
                    except Exception as e:
                        st.error(f"上傳連線失敗：{e}")
                        resp2 = None

                    if resp2 is not None:
                        try:
                            text2 = resp2.text
                            st.code(text2[:2000])
                            # 盡量通吃各家格式：JSON/字串 + ask=Success / error_code=0
                            parsed2 = None
                            try:
                                parsed2 = resp2.json()
                            except Exception:
                                parsed2 = None
                            if parsed2:
                                if (str(parsed2.get("ask", "")).lower() in ("success","ok","true")) or (str(parsed2.get("error_code", "")) == "0"):
                                    st.success("✅ WMS 上傳成功！")
                                else:
                                    st.warning("⚠️ WMS 回傳非成功狀態，請檢查上方 JSON/回應內容。")
                            else:
                                # 沒抓到 JSON，但若關鍵字含 Success 也當成功提示
                                if ("\"ask\":\"Success\"" in text2) or ("\"message\":\"Success\"" in text2):
                                    st.success("✅ WMS 上傳成功！")
                                else:
                                    st.info(f"HTTP {resp2.status_code}，請檢查回應內容。")

# 收尾提示
else:
    st.info("請先在左側按『抓取訂單』或『搜尋 PO（14 天內）』。")
