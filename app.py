# app1.py — Streamlit BOL 產生器（UI 優化：抓取訂單移到側邊；PO 搜尋固定 14 天）
import os
import io
import zipfile
from datetime import datetime, timedelta

import requests
import streamlit as st
import re
from importorder import build_soap_envelope, requests_session_with_retry, call_soap, send_create_order

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

try:
    import fitz  # PyMuPDF
except Exception:
    st.error("PyMuPDF 未安裝，請在環境中安裝：pip install pymupdf")
    raise

import json
import base64
from urllib.parse import urlencode
from dotenv import load_dotenv

load_dotenv(override=False)

APP_TITLE = "BOL 工具 & WMS 推單（測試）"
TEMPLATE_PDF = "bol_template.pdf"
OUTPUT_DIR = "output_bols"
BASE_URL  = "https://api.teapplix.com/api2/OrderNotification"
STORE_KEY = "HD"
SHIPPED_DEFAULT = "0"   # 一般抓單預設：未出貨
PAGE_SIZE = 500

CHECKBOX_FIELDS   = {"MasterBOL", "Term_Pre", "Term_Collect", "Term_CustChk", "FromFOB", "ToFOB"}
FORCE_TEXT_FIELDS = {"PrePaid", "Collect", "3rdParty"}

BILL_NAME         = "THE HOME DEPOT"
BILL_ADDRESS      = "2455 PACES FERRY RD"
BILL_CITYSTATEZIP = "ATLANTA, GA 30339"

# ---------- secrets / env ----------
def _sec(name, default=""):
    return st.secrets.get(name, os.getenv(name, default))

TEAPPLIX_TOKEN = _sec("TEAPPLIX_TOKEN", "")
AUTH_BEARER    = _sec("TEAPPLIX_AUTH_BEARER", "")
X_API_KEY      = _sec("TEAPPLIX_X_API_KEY", "")
PASSWORD       = _sec("APP_PASSWORD", "")

# UI 倉庫代號
WAREHOUSES = {
    "CA 91789": {
        "name": _sec("W1_NAME", "Festival Neo CA"),
        "addr": _sec("W1_ADDR", "5500 Mission Blvd"),
        "citystatezip": _sec("W1_CITYSTATEZIP", "Montclair, CA 91763"),
        "sid": _sec("W1_SID", "CA-001"),
        "tel": _sec("W1_TEL", "909-000-0000"),
        "SCAC": _sec("W1_SCAC", "FEDXG"),
        "ENDPOINT_URL": _sec("W1_WMS_ENDPOINT", ""),
        "APP_TOKEN": _sec("W1_WMS_APP_TOKEN", ""),
        "APP_KEY": _sec("W1_WMS_APP_KEY", ""),
        "WAREHOUSE_CODE": _sec("W1_WMS_CODE", "CAW"),
    },
    "NJ 08816": {
        "ENDPOINT_URL": _sec("W2_WMS_ENDPOINT", ""),
        "APP_TOKEN": _sec("W2_WMS_APP_TOKEN", ""),
        "APP_KEY": _sec("W2_WMS_APP_KEY", ""),
        "WAREHOUSE_CODE": _sec("W2_WMS_CODE", "NJW"),
    },
}

# ---------- utils ----------
def phoenix_range_days(days=3):
    """回傳 Phoenix 時區的 [開始, 結束] ISO 字串（涵蓋 days 天到當日 23:59:59）。"""
    tz = ZoneInfo("America/Phoenix")
    now = datetime.now(tz)
    end   = now.replace(hour=23, minute=59, second=59, microsecond=0)
    start = (end - timedelta(days=days-1)).replace(hour=0, minute=0, second=0, microsecond=0)
    fmt = "%Y-%m-%dT%H:%M:%S"
    return start.strftime(fmt), end.strftime(fmt)

def default_pickup_date_str():
    """回傳 Phoenix 時區兩天後的日期（YYYY-MM-DD）。"""
    tz = ZoneInfo("America/Phoenix")
    return (datetime.now(tz) + timedelta(days=2)).date().isoformat()

def get_headers():
    hdr = {
        "APIToken": TEAPPLIX_TOKEN,
        "Content-Type": "application/json;charset=UTF-8",
        "Accept": "application/json",
    }
    if AUTH_BEARER:
        hdr["Authorization"] = f"Bearer {AUTH_BEARER}"
    if X_API_KEY:
        hdr["X-API-KEY"] = X_API_KEY
    return hdr

def call_api(path, payload: dict):
    url = f"{BASE_URL}/{path}"
    hdr = get_headers()
    try:
        r = requests.post(url, json=payload, headers=hdr, timeout=60)
        return r
    except Exception as e:
        st.error(f"API 連線錯誤：{e}")
        raise

def _safe(v, default=""):
    return (v if v is not None else default)

def _first_nonempty(*vals):
    for v in vals:
        if v:
            return v
    return ""

def _safe_get_nested(dct, *keys, default=""):
    cur = dct or {}
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
        if cur in (None, ""):
            return default
    return cur

def _qty_from_order(o):
    od = o.get("OrderDetails") or {}
    try:
        q = int(od.get("Quantity") or 0)
    except Exception:
        q = 0
    return q

def summarize_packages(o):
    od = o.get("OrderDetails") or {}
    pkgs = _first_nonempty(od.get("Packages"), od.get("NoOfPackages"), od.get("PackagesQty"), od.get("Quantity"), 1)
    weight = _first_nonempty(od.get("WeightLBS"), od.get("Weight"), od.get("totalWeight"), od.get("WeightLbs"), 0)
    return pkgs, weight

def _sku8_from_order(o):
    od = o.get("OrderDetails") or {}
    sku = _first_nonempty(od.get("SKU"), od.get("Sku"), od.get("ItemName2"), od.get("ItemName"), od.get("BuyerNotes"))
    return (sku or "")[:8] if sku else ""

def summarize_packages_group(group: list):
    total_pkgs = 0
    total_lb = 0.0
    for od in group:
        pkgs, lb = summarize_packages(od)
        total_pkgs += int(pkgs or 0)
        total_lb   += float(lb or 0.0)
    return total_pkgs, int(round(total_lb))

def _parse_order_date_str(first_order):
    """只顯示日期（mm/dd/yy）"""
    tz_phx = ZoneInfo("America/Phoenix")
    od = first_order.get("OrderDetails") or {}
    candidates = [
        od.get("PaymentDate"),
        od.get("OrderDate"),
        first_order.get("PaymentDate"),
        first_order.get("Created"),
        first_order.get("CreateDate"),
    ]
    raw = next((v for v in candidates if v), None)
    if not raw: return ""
    val = str(raw).strip()
    dt = None
    try:
        if "T" in val:
            dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
        else:
            for fmt in ("%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d", "%Y-%m-%d"):
                try:
                    dt = datetime.strptime(val, fmt); break
                except Exception:
                    pass
    except Exception:
        pass
    if not dt: return ""
    dt = dt.astimezone(ZoneInfo("America/Phoenix"))
    return dt.strftime("%m/%d/%y")

def fetch_orders(days=3, shipped=SHIPPED_DEFAULT):
    start_iso, end_iso = phoenix_range_days(days)
    payload = {
        "StoreKey": STORE_KEY,
        "StartTime": start_iso,
        "EndTime": end_iso,
        "PageSize": PAGE_SIZE,
        "PageNo": 1,
        "Shipped": shipped
    }
    try:
        r = call_api("GetOrdersByUpdateTime", payload)
    except Exception as e:
        st.error(f"抓單失敗：{e}")
        return []
    if r.status_code != 200:
        st.error(f"抓單 API 錯誤: {r.status_code}\n{r.text[:400]}")
        return []
    try:
        data = r.json()
    except Exception:
        st.error(f"抓單回傳非 JSON：{r.text[:400]}")
        return []
    return data.get("orders") or data.get("Orders") or []

def fetch_orders_by_pos(polist: list, shipped=""):
    """
    以 PO（OriginalTxnId）嚴格等於查詢；只看最近 14 天
    """
    results = []
    shipped = str(shipped or "").strip()
    start_iso, end_iso = phoenix_range_days(14)
    headers = get_headers()
    for oid in polist:
        payload = {
            "StoreKey": STORE_KEY,
            "OriginalTxns": [oid],
            "StartTime": start_iso,
            "EndTime": end_iso,
            "PageSize": PAGE_SIZE,
            "PageNo": 1,
        }
        if shipped in ("0", "1"):
            payload["Shipped"] = shipped
        try:
            r = requests.post(f"{BASE_URL}/GetOrdersByOriginalTxnId", json=payload, headers=headers, timeout=60)
        except Exception as e:
            st.error(f"PO {oid} 連線錯誤：{e}"); continue
        if r.status_code != 200:
            st.error(f"PO {oid} API 錯誤: {r.status_code}\n{r.text[:400]}"); continue
        try:
            data = r.json()
        except Exception:
            st.error(f"PO {oid} 回傳非 JSON：{r.text[:400]}"); continue

        raw_orders = data.get("orders") or data.get("Orders") or []

        # 嚴格等於過濾 + 排除 UNSP_CG
        for o in raw_orders:
            if str(o.get("OriginalTxnId") or "").strip() == oid:
                od = o.get("OrderDetails") or {}
                if (od.get("ShipClass") or "").strip().upper() != "UNSP_CG":
                    results.append(o)

        if raw_orders and not any(str(o.get("OriginalTxnId") or "").strip() == oid for o in raw_orders):
            st.info(f"提示：API 在最近 14 天回 {len(raw_orders)} 筆，但無『OriginalTxnId 等於 {oid}』資料。")

    if shipped in ("0", "1"):
        results = [o for o in results if str(o.get("Shipped") or o.get("shipped") or "").strip() == shipped]
    return results

# ---------- PDF 填寫 ----------
def set_widget_value(widget, name, value):
    try:
        is_checkbox_type  = (widget.field_type == fitz.PDF_WIDGET_TYPE_CHECKBOX)
        is_checkbox_named = (name in CHECKBOX_FIELDS)
        if is_checkbox_type and is_checkbox_named:
            widget.field_value = "Yes" if str(value).strip().upper() in ("YES", "1", "TRUE", "ON", "CHECKED") else "Off"
        else:
            widget.field_value = str(value)
    except Exception:
        try:
            widget.field_value = str(value)
        except Exception:
            pass

def build_row_from_group(oid: str, group: list, wh_key: str):
    total_pkgs, total_lb = summarize_packages_group(group)
    WH = WAREHOUSES.get(wh_key, {})
    first = group[0] if group else {}
    to = first.get("To") or {}
    shipto_name = _first_nonempty(to.get("FullName"), to.get("Name"), to.get("BuyerName"), to.get("Receiver"))
    shipto_street = _first_nonempty(to.get("Address1"), to.get("Street"), to.get("Addr1"))
    shipto_city = _first_nonempty(to.get("City"), to.get("Town"))
    shipto_state = _first_nonempty(to.get("State"), to.get("Province"))
    shipto_zip = _first_nonempty(to.get("Zip"), to.get("ZipCode"))

    row = {
        "SCAC": (WH.get("SCAC") or "FEDXG"),
        "BILL_TO": BILL_NAME,
        "Bill_address": BILL_ADDRESS,
        "Bill_citystatezip": BILL_CITYSTATEZIP,
        "FromName": WH.get("name",""),
        "FromAddress": WH.get("addr",""),
        "FromCityStateZip": WH.get("citystatezip",""),
        "FromPhone": WH.get("tel",""),
        "ToName": shipto_name or "",
        "ToAddress": shipto_street or "",
        "ToCityStateZip": f"{shipto_city}, {shipto_state} {shipto_zip}".strip().strip(","),
        "ToPhone": _safe_get_nested(first, "To", "Phone", default=""),
        "PO#": oid,
        "PRO#": "",
        "SCAC_code": (WH.get("SCAC") or "FEDXG"),
        "Terms": "Prepaid",
        "MasterBOL": "Yes",
        "PrePaid": "X",
        "Collect": "",
        "3rdParty": "",
        "FromFOB": "X",
        "ToFOB": "",
        "NumPkgs": str(total_pkgs) if total_pkgs else "",
        "TotalPkgs": str(total_pkgs) if total_pkgs else "",
        "Total_Weight": str(total_lb) if total_lb else "",
        "Date": datetime.now().strftime("%Y/%m/%d"),
        "Page_ttl": "1",
        "NMFC1": "69420",
        "Class1": "125",
    }

    total_qty_sum = 0
    for idx, od_item in enumerate(group, start=1):
        desc_val = _desc_value_from_order(od_item)
        qty = _qty_from_order(od_item)
        if desc_val:
            row[f"Desc_{idx}"] = desc_val
            row[f"HU_Type_{idx}"]  = "piece"
            row[f"Pkg_Type_{idx}"] = "piece"
            row[f"HU_QTY_{idx}"]   = str(qty) if qty else ""
            row[f"Pkg_QTY_{idx}"]  = str(qty) if qty else ""
            total_qty_sum += qty
            row[f"NMFC{idx}"] = "69420"
            row[f"Class{idx}"] = "125"

    row["NumPkgs1"] = str(total_qty_sum)
    row["Weight1"] = "130 lbs" if total_qty_sum <= 1 else f"{130 + (total_qty_sum - 1) * 30} lbs"
    return row, WH

def fill_pdf(row: dict, out_path: str):
    if not os.path.exists(TEMPLATE_PDF):
        raise FileNotFoundError(f"找不到 BOL 模板：{TEMPLATE_PDF}")
    doc = fitz.open(TEMPLATE_PDF)
    for page in doc:
        widgets = page.widgets()
        if not widgets: continue
        for w in widgets:
            name = (w.field_name or "").strip()
            if not name: continue
            val = row.get(name, "")
            set_widget_value(w, name, val)
        page.apply_redactions()
    doc.save(out_path)
    doc.close()

def _desc_value_from_order(o):
    od = o.get("OrderDetails") or {}
    sku = _first_nonempty(od.get("SKU"), od.get("Sku"), od.get("ItemName2"), od.get("ItemName"), "")
    return (sku or "Furniture")[:40]

def build_wms_params_from_group(oid: str, group: list, wh_key: str, pickup_date_str: str):
    first = group[0] if group else {}
    to = first.get("To") or {}
    street = _first_nonempty(to.get("Address1"), to.get("Street"), to.get("Addr1"))
    street2 = _first_nonempty(to.get("Address2"), to.get("Addr2"))
    city = _first_nonempty(to.get("City"), to.get("Town"))
    province = _first_nonempty(to.get("State"), to.get("Province"))
    zipcode = _first_nonempty(to.get("Zip"), to.get("ZipCode"))
    company = _first_nonempty(to.get("Company"), to.get("BuyerName"))
    name = _first_nonempty(to.get("FullName"), to.get("Name"), to.get("Receiver"))
    phone = _first_nonempty(to.get("Phone"), to.get("Tel"))

    items = []
    for o in group:
        od = o.get("OrderDetails") or {}
        sku = _first_nonempty(od.get("SKU"), od.get("Sku"), od.get("ItemName2"), od.get("ItemName"))
        qty = int(_first_nonempty(od.get("Quantity"), 1))
        if sku:
            items.append({"product_sku": str(sku).strip(), "quantity": int(qty)})

    cfg = WAREHOUSES.get(wh_key) or {}
    warehouse_code = cfg.get("WAREHOUSE_CODE", "")

    # 依 Teapplix ship class 塞入 platform_shop（可在 UI 修改）
    od1 = first.get("OrderDetails") or {}
    shipclass = str(od1.get("ShipClass") or "").strip()

    # 測試：tracking_no 放 test + oid
    test_oid = f"test-{oid}"

    params = {
        "reference_no": oid,                     # ← 必填：你的 PO/單號
        "order_desc": f"pick up: {pickup_date_str}",
        "warehouse_code": warehouse_code,        # ← WMS 倉別代碼（可在 UI 改）
        "to_address": {
            "country_code": "US",
            "province": province,
            "city": city,
            "district": city,                        # ← 同 City
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
            "is_por": "0",
            "is_door": "1",
        },
        "platform_shop": shipclass,
        "items": items,
        "tracking_no": test_oid,                 # ← test + OriginalTxnId
    }
    return params


def _extract_wms_json(resp_text: str) -> dict:
    """
    Try to extract JSON segment from SOAP response text.
    Looks for the first {...} block and parses it.
    """
    if not isinstance(resp_text, str) or not resp_text:
        return {}

    # 找出第一個 "{" 開始到最後一個 "}" 結束的 JSON 片段
    try:
        first = resp_text.find("{")
        last  = resp_text.rfind("}")
        if first == -1 or last == -1 or last <= first:
            return {}
        j = resp_text[first:last+1]
        j2 = j.replace("&quot;", '"').replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
        try:
            return json.loads(j2)
        except Exception:
            return {}
    except Exception:
        return {}

def push_group_to_wms(oid: str, group: list, wh_key: str, pickup_date_str: str):
    """Send the grouped order to the WMS endpoint for the given warehouse key."""
    cfg = WAREHOUSES.get(wh_key) or {}
    endpoint = cfg.get("ENDPOINT_URL") or ""
    app_token = cfg.get("APP_TOKEN") or ""
    app_key = cfg.get("APP_KEY") or ""

    if not (endpoint and app_token and app_key):
        return {"ok": False, "message": f"{wh_key} WMS config is missing (endpoint/app_token/app_key)."}

    params = build_wms_params_from_group(oid, group, wh_key, pickup_date_str)
    try:
        resp = send_create_order(endpoint, app_token, app_key, params, service="createOrder")
        text = resp.text[:5000]
        parsed = _extract_wms_json(text)
        return {"ok": (200 <= resp.status_code < 300), "status": resp.status_code, "response": text, "parsed": parsed, "params": params}
    except Exception as e:
        return {"ok": False, "message": f"request error: {e}", "params": params}

# ---------- Streamlit UI ----------
st.set_page_config(page_title=APP_TITLE, layout="wide")

# 密碼驗證
st.sidebar.subheader("🔐 驗證區")
input_pwd = st.sidebar.text_input("請輸入密碼", type="password")
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

# ---- 側邊：抓取訂單（最近 N 天） ----
st.sidebar.markdown("---")
st.sidebar.subheader("🧲 抓取訂單（一般）")
days = st.sidebar.selectbox("天數", options=[1,2,3,4,5,6,7], index=2, help="套用於『抓取訂單』")
if st.sidebar.button("抓取訂單", width="stretch"):
    st.session_state["orders_raw"] = fetch_orders(days)
    st.session_state.pop("table_rows_override", None)
    st.sidebar.success(f"已抓取最近 {days} 天的一般訂單。")


# ---- 側邊：WMS 推送設定 ----
# （已改：取件日改到人工修改區，預設兩天後）

# ---- 側邊：以 PO 搜尋（最近 14 天） ----
st.sidebar.markdown("---")
st.sidebar.subheader("🔎 PO 搜尋（最近 14 天）")
po_text = st.sidebar.text_area(
    "輸入 PO（每行一個）",
    placeholder="例如：\n32585340\n46722012",
    height=120,
)
shipped_choice = st.sidebar.selectbox(
    "出貨狀態（Shipped）",
    options=["不限", "未出貨(0)", "已出貨(1)"],
    index=0,
    help="0 = 未出貨，1 = 已出貨；不限則不帶此參數",
)
if st.sidebar.button("搜尋 PO（14 天內）", width="stretch"):
    raw_lines = (po_text or "").splitlines()
    pos_list = [ln.strip() for ln in raw_lines if ln.strip()]
    if not pos_list:
        st.warning("請輸入至少一個 PO（每行一個）。")
    else:
        shipped_val = ""
        if shipped_choice.endswith("(0)"): shipped_val = "0"
        elif shipped_choice.endswith("(1)"): shipped_val = "1"

        orders = fetch_orders_by_pos(pos_list, shipped_val)  # ★ 不再依 days，固定 14 天
        st.session_state["orders_raw"] = orders
        st.session_state.pop("table_rows_override", None)
        st.success(f"PO 搜尋完成（14 天內）：輸入 {len(pos_list)} 筆 PO，取得 {len(orders)} 筆原始訂單，"
                   f"並依 PO 合併顯示於下方表格。")

# ======== 合併表（依 OriginalTxnId 合併） + 產 BOL ========
orders_raw = st.session_state.get("orders_raw", None)

def build_table_rows_from_orders(orders_raw):
    grouped = {}
    for o in orders_raw or []:
        oid = str(o.get("OriginalTxnId") or o.get("original_txn_id") or "").strip()
        if not oid: 
            # 沒有 OriginalTxnId 的資料忽略
            continue
        grouped.setdefault(oid, []).append(o)

    table_rows = []
    for oid, group in grouped.items():
        # 預設倉
        wh_key = "CA 91789"
        first = group[0]
        od1 = first.get("OrderDetails") or {}
        order_date_str = _parse_order_date_str(first)
        sku8 = _sku8_from_order(first)
        scac = (WAREHOUSES.get(wh_key) or {}).get("SCAC") or "FEDXG"
        table_rows.append({
            "Select": False,
            "Warehouse": "CA 91789",
            "OriginalTxnId": oid,
            "SKU8": sku8,
            "SCAC": scac,
            "ToState": (first.get("To") or {}).get("State",""),
            "OrderDate": order_date_str,
        })
    return grouped, table_rows

if orders_raw:
    grouped, table_rows = build_table_rows_from_orders(orders_raw)
    st.caption(f"共 {len(table_rows)} 筆（依 OriginalTxnId 合併）")

    # 批次修改倉庫
    bulk_col1, bulk_col2, bulk_col3 = st.columns([1,1,6])
    with bulk_col1:
        bulk_wh = st.selectbox("批次指定倉庫", options=list(WAREHOUSES.keys()), index=0)
    with bulk_col2:
        apply_to = st.selectbox("套用對象", options=["勾選列", "全部"], index=0)
    with bulk_col3:
        if st.button("套用批次倉庫"):
            new_rows = []
            if apply_to == "全部":
                for r in table_rows:
                    r2 = dict(r); r2["Warehouse"] = bulk_wh; new_rows.append(r2)
            else:
                for r in table_rows:
                    r2 = dict(r)
                    if r2.get("Select"): r2["Warehouse"] = bulk_wh
                    new_rows.append(r2)
            st.session_state["table_rows_override"] = new_rows

    # 顯示資料表
    source_rows = st.session_state.get("table_rows_override") or table_rows
    edited = st.data_editor(
        source_rows,
        use_container_width=True,
        num_rows="dynamic",
        column_config={
            "Select": st.column_config.CheckboxColumn("選取", help="勾選要處理的列"),
            "Warehouse": st.column_config.SelectboxColumn("倉庫", options=list(WAREHOUSES.keys())),
            "OriginalTxnId": "PO / OriginalTxnId",
            "SKU8": "SKU(前8)",
            "SCAC": "SCAC",
            "ToState": "州",
            "OrderDate": "Order Date",
        },
        key="orders_table",
    )

    # 產出 BOL
    if st.button("產生 BOL（勾選列）", type="primary", width="stretch"):
        selected = [r for r in edited if r.get("Select")]
        if not selected:
            st.warning("尚未選取任何訂單。")
        else:
            os.makedirs(OUTPUT_DIR, exist_ok=True)
            made_files = []
            for row_preview in selected:
                oid = row_preview["OriginalTxnId"]
                wh_key = row_preview["Warehouse"]
                group = grouped.get(oid, [])
                if not group:
                    continue
                row_dict, WH = build_row_from_group(oid, group, wh_key)
                sku8 = row_preview["SKU8"] or (_sku8_from_order(group[0]) or "NOSKU")[:8]
                wh2 = (WH["name"][:2].upper() if WH["name"] else "WH")
                scac = (row_preview["SCAC"] or "").upper() or "NOSCAC"
                filename = f"BOL_{oid}_{sku8}_{wh2}_{scac}.pdf".replace(" ", "")
                out_path = os.path.join(OUTPUT_DIR, filename)
                fill_pdf(row_dict, out_path)
                made_files.append(out_path)

            if made_files:
                st.success(f"已產生 {len(made_files)} 份 BOL。")
                mem_zip = io.BytesIO()
                with zipfile.ZipFile(mem_zip, "w", zipfile.ZIP_DEFLATED) as zf:
                    for p in made_files:
                        zf.write(p, arcname=os.path.basename(p))
                mem_zip.seek(0)
                st.download_button(
                    "下載全部 BOL (ZIP)",
                    data=mem_zip,
                    file_name=f"BOL_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip",
                    mime="application/zip",
                    width="stretch",
                )
            else:
                st.warning("沒有產生任何檔案。")

    # ======== 先建立預設上傳參數 -> 顯示人工修改 UI -> 再送出 ========
    if st.button("推送到 WMS（先人工修改）", type="primary", use_container_width=True):
        selected = [r for r in edited if r.get("Select")]
        if not selected:
            st.warning("尚未選取任何訂單。")
        else:
            edit_map = {}
            for row_preview in selected:
                oid = row_preview["OriginalTxnId"]
                wh_key = row_preview["Warehouse"]
                group = grouped.get(oid, [])
                if not group:
                    continue
                pickup_str = default_pickup_date_str()
                params = build_wms_params_from_group(oid, group, wh_key, pickup_str)
                edit_map[oid] = {"Warehouse": wh_key, "params": params}
            st.session_state["wms_edit_map"] = edit_map
            st.session_state["wms_groups"] = grouped
            st.success(f"已建立 {len(edit_map)} 筆預設上傳資料，請在下方逐筆人工修改後送出。")

    wms_edit_map = st.session_state.get("wms_edit_map")
    if wms_edit_map:
        st.markdown("### 📝 推送前人工修改")
        st.caption("每筆資料都可修改（含取件日期、SKU/數量等），確認後再送出。")

        for oid, rec in wms_edit_map.items():
            p = rec["params"]
            import re as _re
            m = _re.search(r"pick up:\s*(\d{4}-\d{2}-\d{2})", p.get("order_desc") or "")
            pickup_default = m.group(1) if m else default_pickup_date_str()

            with st.expander(f"🛠 人工修改：{oid}"):
                col_pd, col_wc = st.columns(2)
                with col_pd:
                    new_pickup_date = st.date_input("Pick up date", value=datetime.fromisoformat(pickup_default).date(), key=f"{oid}_pickup")
                with col_wc:
                    new_wh_code = st.text_input("warehouse_code", value=p.get("warehouse_code",""), key=f"{oid}_whc")

                c1, c2 = st.columns(2)
                with c1:
                    new_tracking = st.text_input("tracking_no", value=p.get("tracking_no",""), key=f"{oid}_trk")
                    new_platform_shop = st.text_input("platform_shop", value=p.get("platform_shop",""), key=f"{oid}_pshop")
                with c2:
                    new_ref = st.text_input("reference_no", value=p.get("reference_no",""), key=f"{oid}_ref")
                    new_remark = st.text_input("remark", value=p.get("remark",""), key=f"{oid}_remark")

                st.markdown("**Items**")
                new_items = []
                for idx, it in enumerate(p.get("items", [])):
                    col1, col2 = st.columns(2)
                    with col1:
                        new_sku = st.text_input(f"product_sku #{idx+1}", value=it.get("product_sku",""), key=f"{oid}_sku_{idx}")
                    with col2:
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

                    target_wh_key = None
                    for k, cfg in WAREHOUSES.items():
                        if cfg.get("WAREHOUSE_CODE") == new_params.get("warehouse_code"):
                            target_wh_key = k
                            break
                    if not target_wh_key:
                        target_wh_key = rec.get("Warehouse", "NJ 08816")

                    cfg = WAREHOUSES.get(target_wh_key, {})
                    try:
                        resp2 = send_create_order(cfg.get("ENDPOINT_URL",""), cfg.get("APP_TOKEN",""), cfg.get("APP_KEY",""), new_params, service="createOrder")
                        text2 = resp2.text[:5000]
                        parsed2 = _extract_wms_json(text2)
                        st.info(f"HTTP {resp2.status_code}")
                        st.text_area("回應（前 5000 字）", text2, height=160)
                        if parsed2:
                            st.json(parsed2)
                    except Exception as e:
                        st.error(f"上傳失敗：{e}")
else:
    st.info("請先在左側按『抓取訂單』或『搜尋 PO（14 天內）』。")
