# app.py — Teapplix HD LTL BOL 產生器 + 推送/產生前人工修改（移除批次倉庫與倉庫欄）
import os
import io
import zipfile
from datetime import datetime, timedelta
import re

import requests
import streamlit as st

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

from dotenv import load_dotenv
import fitz  # PyMuPDF

# ★ SOAP/送單封裝
from importorder import send_create_order  # endpoint, app_token, app_key, params, service

# ---------- 應用設定 ----------
APP_TITLE = "Teapplix HD LTL BOL 產生器"
TEMPLATE_PDF = "BOL.pdf"
OUTPUT_DIR = "output_bols"
BASE_URL  = "https://api.teapplix.com/api2/OrderNotification"
STORE_KEY = "HD"
SHIPPED_DEFAULT = "0"
PAGE_SIZE = 500

CHECKBOX_FIELDS   = {"MasterBOL", "Term_Pre", "Term_Collect", "Term_CustChk", "FromFOB", "ToFOB"}
FORCE_TEXT_FIELDS = {"PrePaid", "Collect", "3rdParty"}

BILL_NAME         = "THE HOME DEPOT"
BILL_ADDRESS      = "2455 PACES FERRY RD"
BILL_CITYSTATEZIP = "ATLANTA, GA 30339"

# ---------- secrets / env ----------
load_dotenv(override=False)
def _sec(name, default=""):
    return st.secrets.get(name, os.getenv(name, default))

TEAPPLIX_TOKEN = _sec("TEAPPLIX_TOKEN", "")
AUTH_BEARER    = _sec("TEAPPLIX_AUTH_BEARER", "")
X_API_KEY      = _sec("TEAPPLIX_X_API_KEY", "")
PASSWORD       = _sec("APP_PASSWORD", "")

WMS_SERVICE = _sec("WMS_SERVICE", "createOrder")

# UI 倉庫基本資料（BOL 用）
WAREHOUSES = {
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

# WMS 送單憑證（依倉別）
WMS_CONFIGS = {
    "CA 91789": {
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

# ---------- 常用工具 ----------
def phoenix_range_days(days=3):
    tz = ZoneInfo("America/Phoenix")
    now = datetime.now(tz)
    end   = now.replace(hour=23, minute=59, second=59, microsecond=0)
    start = (end - timedelta(days=days-1)).replace(hour=0, minute=0, second=0, microsecond=0)
    fmt = "%Y-%m-%dT%H:%M:%S"
    return start.strftime(fmt), end.strftime(fmt)

def default_pickup_date_str():
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
        hdr["x-api-key"] = X_API_KEY
    return hdr

def oz_to_lb(oz):
    try:
        return round(float(oz)/16.0, 2)
    except Exception:
        return None

def summarize_packages(order):
    details = order.get("ShippingDetails") or []
    total_pkgs = 0
    total_lb = 0.0
    for sd in details:
        pkg = sd.get("Package") or {}
        count = int(pkg.get("IdenticalPackageCount") or 1)
        wt = pkg.get("Weight") or {}
        lb = oz_to_lb(wt.get("Value")) or 0.0
        total_pkgs += max(1, count)
        total_lb   += lb * max(1, count)
    return total_pkgs, int(round(total_lb))

def override_carrier_name_by_scac(scac: str, current_name: str) -> str:
    s = (scac or "").strip().upper()
    mapping = {
        "EXLA": "Estes Express Lines",
        "AACT": "AAA Cooper Transportation",
        "CTII": "Central Transport Inc.",
        "CETR": "Central Transport Inc.",
        "ABF":  "ABF",
        "PITD": "PITT Ohio",
        "FXFE": "FedEx Freight",
        "UPGF": "UPS Freight",
        "RLCA": "R+L Carriers",
        "SAIA": "SAIA",
        "ODFL": "Old Dominion",
    }
    return mapping.get(s, current_name)

def group_by_original_txn(orders):
    grouped = {}
    for order in orders:
        oid = (order.get("OriginalTxnId") or "").strip()
        if not oid:
            continue
        grouped.setdefault(oid, []).append(order)
    return grouped

def _first_item(order):
    items = order.get("OrderItems") or []
    if isinstance(items, list) and items:
        return items[0]
    if isinstance(items, dict):
        return items
    return {}

def _desc_value_from_order(order):
    sku = (_first_item(order).get("ItemSKU") or "")
    return f"{sku}  (Electric Fireplace)".strip()

def _sku8_from_order(order):
    sku = (_first_item(order).get("ItemSKU") or "")
    return sku[:8] if sku else ""

def _qty_from_order(order):
    it = _first_item(order)
    try:
        return int(it.get("Quantity") or 0)
    except Exception:
        return 0

def _sum_group_totals(group):
    total_pkgs = 0
    total_lb = 0.0
    for od in group:
        pkgs, lb = summarize_packages(od)
        total_pkgs += int(pkgs or 0)
        total_lb   += float(lb or 0.0)
    return total_pkgs, int(round(total_lb))

def _parse_order_date_str(first_order):
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
                except Exception: continue
    except Exception:
        dt = None
    if dt is None:
        try: dt = datetime.fromisoformat(val[:19])
        except Exception: return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz_phx)
    dt_phx = dt.astimezone(tz_phx)
    return dt_phx.strftime("%m/%d/%y")

def _try_extract_json(resp_text: str):
    if not isinstance(resp_text, str) or not resp_text:
        return {}
    start = resp_text.find("{")
    end = resp_text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return {}
    import json
    js = resp_text[start:end+1]
    try:
        return json.loads(js)
    except Exception:
        j2 = js.replace("&quot;", '"').replace("&lt;", "<").replace("&gt;", ">")
        try:
            return json.loads(j2)
        except Exception:
            return {}

# ---------- API：抓單 ----------
def fetch_orders(days: int):
    ps, pe = phoenix_range_days(days)
    page = 1
    all_orders = []
    while True:
        params = {
            "PaymentDateStart": ps,
            "PaymentDateEnd": pe,
            "Shipped": SHIPPED_DEFAULT,
            "StoreKey": STORE_KEY,
            "PageSize": str(PAGE_SIZE),
            "PageNumber": str(page),
            "Combine": "combine",
            "DetailLevel": "shipping|inventory|marketplace",
        }
        r = requests.get(BASE_URL, headers=get_headers(), params=params, timeout=45)
        if r.status_code != 200:
            st.error(f"API 錯誤: {r.status_code}\n{r.text}"); break
        try:
            data = r.json()
        except Exception:
            st.error(f"JSON 解析錯誤：{r.text[:1000]}"); break
        orders = data.get("orders") or data.get("Orders") or []
        if not orders: break
        for o in orders:
            od = o.get("OrderDetails") or {}
            if (od.get("ShipClass") or "").strip().upper() != "UNSP_CG":
                all_orders.append(o)
        if len(orders) < PAGE_SIZE: break
        page += 1
    return all_orders

# ---------- API：以 PO 搜尋（固定 14 天） ----------
def fetch_orders_by_pos(pos_list, shipped: str):
    ps, pe = phoenix_range_days(14)
    results = []
    for oid in pos_list:
        oid = (oid or "").strip()
        if not oid:
            continue
        params = {
            "StoreKey": STORE_KEY,
            "DetailLevel": "shipping|inventory|marketplace",
            "Combine": "combine",
            "PageSize": str(PAGE_SIZE),
            "PageNumber": "1",
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

        raw_orders = data.get("orders") or data.get("Orders") or []
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
        is_forced_text    = (name in FORCE_TEXT_FIELDS)
        is_checkbox       = (is_checkbox_type or is_checkbox_named) and not is_forced_text
        if is_checkbox:
            v = str(value).strip().lower()
            widget.field_value = "Yes" if v in {"on","yes","1","true","x","✔"} else "Off"
        else:
            widget.field_value = "" if value is None else str(value)
        widget.update()
        return True
    except Exception as e:
        st.warning(f"填欄位 {name} 失敗：{e}"); return False

def build_row_from_group(oid, group, wh_key: str):
    first = group[0]
    to = first.get("To") or {}
    od = first.get("OrderDetails") or {}

    ship_details = (first.get("ShippingDetails") or [{}])[0] or {}
    pkg = ship_details.get("Package") or {}
    tracking = pkg.get("TrackingInfo") or {}

    scac_from_shipclass = (od.get("ShipClass") or "").strip()
    carrier_name_raw = (tracking.get("CarrierName") or "").strip()
    carrier_name_final = override_carrier_name_by_scac(scac_from_shipclass, carrier_name_raw)

    street  = (to.get("Street") or "")
    street2 = (to.get("Street2") or "")
    to_address = (street + (" " + street2 if street2 else "")).strip()
    custom_code = (od.get("Custom") or "").strip()

    total_pkgs, total_lb = _sum_group_totals(group)
    bol_num = (od.get("Invoice") or "").strip() or (oid or "").strip()

    WH = WAREHOUSES.get(wh_key, list(WAREHOUSES.values())[0])

    row = {
        "BillName": BILL_NAME,
        "BillAddress": BILL_ADDRESS,
        "BillCityStateZip": BILL_CITYSTATEZIP,
        "ToName": to.get("Name", ""),
        "ToAddress": to_address,
        "ToCityStateZip": f"{to.get('City','')}, {to.get('State','')} {to.get('ZipCode','')}".strip().strip(", "),
        "ToCID": to.get("PhoneNumber", ""),
        "FromName": WH["name"],
        "FromAddr": WH["addr"],
        "FromCityStateZip": WH["citystatezip"],
        "FromSIDNum": WH["sid"],
        "3rdParty": "X", "PrePaid": "", "Collect": "",
        "BOLnum": bol_num,
        "CarrierName": carrier_name_final,
        "SCAC": scac_from_shipclass,
        "PRO": tracking.get("TrackingNumber", ""),
        "CustomerOrderNumber": custom_code,
        "BillInstructions": f"PO#{oid or bol_num}",
        "OrderNum1": custom_code,
        "SpecialInstructions": "",
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
        for w in (page.widgets() or []):
            name = w.field_name
            if name and name in row:
                set_widget_value(w, name, row[name])
    try: doc.need_appearances = True
    except Exception: pass
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    doc.save(out_path, deflate=True, incremental=False, encryption=fitz.PDF_ENCRYPT_KEEP)
    doc.close()

# ---------- WMS 參數組裝 ----------
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
    test_oid = f"test1-{oid}".strip()

    params = {
        "platform": "OTHER",
        "allocated_auto": "0",
        "warehouse_code": WMS_CONFIGS.get(wh_key, {}).get("WAREHOUSE_CODE", ""),
        "shipping_method": "CUSTOMER_SHIP",
        "reference_no": test_oid,
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
        "tracking_no": test_oid,
    }
    return params

# ---------- Streamlit UI ----------
st.set_page_config(page_title=APP_TITLE, layout="wide")

# 密碼驗證（保留）
st.sidebar.subheader("🔐 驗證區")
input_pwd = st.sidebar.text_input("請輸入密碼", type="password")
if input_pwd != PASSWORD:
    st.warning("請輸入正確密碼後才能使用。")
    st.stop()

st.title(APP_TITLE)

# 說明
st.markdown("""
**說明：**
- 先在左側進行一般抓單或 PO 搜尋。
- 之後於結果表勾選想處理的 PO，再點「BOL（先人工修改）」或「推送到 WMS（先人工修改）」進入逐筆修改與生成/送出。
- 可能會錯，請仔細核對。
""")

if not TEAPPLIX_TOKEN:
    st.error("找不到 TEAPPLIX_TOKEN，請在 .env 或 Streamlit Secrets 設定。")
    st.stop()

# 側邊：抓單（GET） —— 保留
days = st.sidebar.selectbox("抓取天數（一般抓單）", options=[1,2,3,4,5,6,7], index=2)
if st.sidebar.button("抓取訂單", use_container_width=True):
    st.session_state["orders_raw"] = fetch_orders(days)
    st.session_state.pop("table_rows_override", None)
    st.sidebar.success(f"已抓取最近 {days} 天的一般訂單。")

# 側邊：PO 搜尋（固定 14 天） —— 保留
st.sidebar.markdown("---")
st.sidebar.subheader("🔎 PO 搜尋（最近 14 天）")
po_text = st.sidebar.text_area("輸入 PO（每行一個）", placeholder="例如：\n32585340\n46722012", height=120)
shipped_choice = st.sidebar.selectbox("出貨狀態（Shipped）", options=["不限", "未出貨(0)", "已出貨(1)"], index=0)
if st.sidebar.button("搜尋 PO（14 天內）", use_container_width=True):
    raw_lines = (po_text or "").splitlines()
    pos_list = [ln.strip() for ln in raw_lines if ln.strip()]
    if not pos_list:
        st.warning("請輸入至少一個 PO（每行一個）。")
    else:
        shipped_val = "0" if shipped_choice.endswith("(0)") else ("1" if shipped_choice.endswith("(1)") else "")
        orders = fetch_orders_by_pos(pos_list, shipped_val)
        st.session_state["orders_raw"] = orders
        st.session_state.pop("table_rows_override", None)
        st.success(f"PO 搜尋完成（14 天內）：輸入 {len(pos_list)} 筆 PO，取得 {len(orders)} 筆原始訂單，並依 PO 合併顯示於下方表格。")

# ======== 合併表（依 OriginalTxnId 合併） ========
orders_raw = st.session_state.get("orders_raw", None)

def build_table_rows_from_orders(orders_raw):
    grouped = group_by_original_txn(orders_raw or [])
    table_rows = []
    for oid, group in grouped.items():
        first = group[0]
        od = first.get("OrderDetails") or {}
        scac = (od.get("ShipClass") or "").strip()
        sku8 = _sku8_from_order(first)
        order_date_str = _parse_order_date_str(first)
        table_rows.append({
            "Select": True,
            # ★ 不顯示「倉庫」欄；移除所有批次倉庫功能
            "OriginalTxnId": oid,
            "SKU8": sku8,
            "SCAC": scac,
            "ToState": (first.get("To") or {}).get("State",""),
            "OrderDate": order_date_str,
        })
    return grouped, table_rows

def _items_from_row_dict(row_dict: dict):
    """將 row_dict 中的 Desc_i / HU_QTY_i / Pkg_QTY_i 等回推為 items list 以供人工編輯"""
    items = []
    idx = 1
    while True:
        dkey = f"Desc_{idx}"
        if dkey not in row_dict:
            break
        items.append({
            "Desc": row_dict.get(dkey, ""),
            "HU_Type": row_dict.get(f"HU_Type_{idx}", "piece"),
            "Pkg_Type": row_dict.get(f"Pkg_Type_{idx}", "piece"),
            "HU_QTY": row_dict.get(f"HU_QTY_{idx}", "1"),
            "Pkg_QTY": row_dict.get(f"Pkg_QTY_{idx}", "1"),
            "NMFC": row_dict.get(f"NMFC{idx}", "69420"),
            "Class": row_dict.get(f"Class{idx}", "125"),
        })
        idx += 1
    if not items:
        items = [{
            "Desc": "",
            "HU_Type": "piece",
            "Pkg_Type": "piece",
            "HU_QTY": "1",
            "Pkg_QTY": "1",
            "NMFC": "69420",
            "Class": "125",
        }]
    return items

def _row_dict_apply_items(row_dict: dict, items: list):
    """將人工編輯後 items list 寫回 row_dict"""
    # 先清理舊的
    for k in list(row_dict.keys()):
        if re.match(r"^(Desc|HU_Type|Pkg_Type|HU_QTY|Pkg_QTY)\_\d+$", k) or re.match(r"^(NMFC|Class)\d+$", k):
            row_dict.pop(k, None)
    # 寫回新的
    for idx, it in enumerate(items, start=1):
        row_dict[f"Desc_{idx}"]      = it.get("Desc", "")
        row_dict[f"HU_Type_{idx}"]   = it.get("HU_Type", "piece")
        row_dict[f"Pkg_Type_{idx}"]  = it.get("Pkg_Type", "piece")
        row_dict[f"HU_QTY_{idx}"]    = str(it.get("HU_QTY", "1"))
        row_dict[f"Pkg_QTY_{idx}"]   = str(it.get("Pkg_QTY", "1"))
        row_dict[f"NMFC{idx}"]       = it.get("NMFC", "69420")
        row_dict[f"Class{idx}"]      = it.get("Class", "125")
    return row_dict

if orders_raw:
    grouped, table_rows = build_table_rows_from_orders(orders_raw)
    st.caption(f"共 {len(table_rows)} 筆（依 OriginalTxnId 合併）")

    # 可編輯表格（無倉庫欄、無批次操作）
    edited = st.data_editor(
        st.session_state.get("table_rows_override", table_rows),
        num_rows="fixed",
        hide_index=True,
        column_config={
            "Select": st.column_config.CheckboxColumn("選取", default=True),
            "OriginalTxnId": st.column_config.TextColumn("PO", disabled=True),
            "SKU8": st.column_config.TextColumn("SKU", disabled=True),
            "SCAC": st.column_config.TextColumn("SCAC", disabled=True),
            "ToState": st.column_config.TextColumn("州", disabled=True),
            "OrderDate": st.column_config.TextColumn("訂單日期 (mm/dd/yy)", disabled=True),
        },
        key="orders_table",
        use_container_width=True,
    )

    # ======== BOL（先人工修改） ========
    if st.button("BOL（先人工修改）", type="primary", use_container_width=True):
        selected = [r for r in edited if r.get("Select")]
        if not selected:
            st.warning("尚未選取任何訂單。")
        else:
            bol_map = {}
            default_wh_key = next(iter(WAREHOUSES.keys()))
            for row_preview in selected:
                oid = row_preview["OriginalTxnId"]
                group = grouped.get(oid, [])
                if not group:
                    continue
                row_dict, _wh = build_row_from_group(oid, group, default_wh_key)
                bol_map[oid] = {"row": row_dict, "group": group}
            st.session_state["bol_edit_map"] = bol_map
            st.success(f"已建立 {len(bol_map)} 筆 BOL 預設資料，請在下方逐筆人工修改後生成。")

    bol_edit_map = st.session_state.get("bol_edit_map")
    if bol_edit_map:
        st.markdown("### 📝 BOL 推送前人工修改（逐筆）")
        st.caption("修改欄位（含寄件端/收件端/承運資訊/件數重量/明細等），確認後再生成 BOL。")

        made_files_for_zip = []

        for oid, rec in bol_edit_map.items():
            row_dict = dict(rec["row"])
            items = _items_from_row_dict(row_dict)

            with st.expander(f"📄 BOL 人工修改：{oid}", expanded=False):
                # From / To 區塊
                c1, c2 = st.columns(2)
                with c1:
                    st.subheader("From（寄件端）")
                    row_dict["FromName"] = st.text_input("FromName", value=row_dict.get("FromName",""), key=f"{oid}_FromName")
                    row_dict["FromAddr"] = st.text_input("FromAddr", value=row_dict.get("FromAddr",""), key=f"{oid}_FromAddr")
                    row_dict["FromCityStateZip"] = st.text_input("FromCityStateZip", value=row_dict.get("FromCityStateZip",""), key=f"{oid}_FromCSZ")
                    row_dict["FromSIDNum"] = st.text_input("FromSIDNum", value=row_dict.get("FromSIDNum",""), key=f"{oid}_FromSID")
                with c2:
                    st.subheader("To（收件端）")
                    row_dict["ToName"] = st.text_input("ToName", value=row_dict.get("ToName",""), key=f"{oid}_ToName")
                    row_dict["ToAddress"] = st.text_input("ToAddress", value=row_dict.get("ToAddress",""), key=f"{oid}_ToAddress")
                    row_dict["ToCityStateZip"] = st.text_input("ToCityStateZip", value=row_dict.get("ToCityStateZip",""), key=f"{oid}_ToCSZ")
                    row_dict["ToCID"] = st.text_input("ToCID", value=row_dict.get("ToCID",""), key=f"{oid}_ToCID")

                # 運輸資訊
                st.subheader("承運與單號")
                c3, c4, c5, c6 = st.columns(4)
                with c3:
                    row_dict["CarrierName"] = st.text_input("CarrierName", value=row_dict.get("CarrierName",""), key=f"{oid}_Carrier")
                with c4:
                    row_dict["SCAC"] = st.text_input("SCAC", value=row_dict.get("SCAC",""), key=f"{oid}_SCAC")
                with c5:
                    row_dict["PRO"] = st.text_input("PRO", value=row_dict.get("PRO",""), key=f"{oid}_PRO")
                with c6:
                    row_dict["BOLnum"] = st.text_input("BOLnum", value=row_dict.get("BOLnum",""), key=f"{oid}_BOLnum")

                # 客戶/備註
                st.subheader("客戶與備註")
                c7, c8, c9 = st.columns(3)
                with c7:
                    row_dict["CustomerOrderNumber"] = st.text_input("CustomerOrderNumber", value=row_dict.get("CustomerOrderNumber",""), key=f"{oid}_CustNo")
                with c8:
                    row_dict["BillInstructions"] = st.text_input("BillInstructions", value=row_dict.get("BillInstructions",""), key=f"{oid}_BillInstr")
                with c9:
                    row_dict["OrderNum1"] = st.text_input("OrderNum1", value=row_dict.get("OrderNum1",""), key=f"{oid}_OrderNum1")
                row_dict["SpecialInstructions"] = st.text_area("SpecialInstructions", value=row_dict.get("SpecialInstructions",""), key=f"{oid}_SpecInstr")

                # 件數重量與日期
                st.subheader("件數、重量與日期")
                c10, c11, c12 = st.columns(3)
                with c10:
                    row_dict["TotalPkgs"] = st.text_input("TotalPkgs", value=row_dict.get("TotalPkgs",""), key=f"{oid}_TotalPkgs")
                with c11:
                    row_dict["Total_Weight"] = st.text_input("Total_Weight", value=row_dict.get("Total_Weight",""), key=f"{oid}_TotalWt")
                with c12:
                    row_dict["Date"] = st.text_input("Date", value=row_dict.get("Date",""), key=f"{oid}_Date")

                # 明細（可編輯）
                st.subheader("明細（可編輯）")
                items_editor = st.data_editor(
                    items,
                    hide_index=True,
                    key=f"{oid}_bol_items",
                    use_container_width=True,
                    num_rows="dynamic",
                    column_config={
                        "Desc": st.column_config.TextColumn("Desc"),
                        "HU_Type": st.column_config.TextColumn("HU_Type"),
                        "Pkg_Type": st.column_config.TextColumn("Pkg_Type"),
                        "HU_QTY": st.column_config.TextColumn("HU_QTY"),
                        "Pkg_QTY": st.column_config.TextColumn("Pkg_QTY"),
                        "NMFC": st.column_config.TextColumn("NMFC"),
                        "Class": st.column_config.TextColumn("Class"),
                    },
                )

                # 生成此 BOL
                if st.button("📄 生成此 BOL（PDF）", key=f"gen_bol_{oid}"):
                    final_row = _row_dict_apply_items(dict(row_dict), items_editor)
                    os.makedirs(OUTPUT_DIR, exist_ok=True)
                    # 檔名：BOL_{PO}_{日期}.pdf
                    filename = f"BOL_{oid}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf".replace(" ", "")
                    out_path = os.path.join(OUTPUT_DIR, filename)
                    try:
                        fill_pdf(final_row, out_path)
                        with open(out_path, "rb") as f:
                            pdf_bytes = f.read()
                        st.success(f"✅ 已生成 BOL：{filename}")
                        st.download_button(
                            "下載此 BOL（PDF）",
                            data=pdf_bytes,
                            file_name=filename,
                            mime="application/pdf",
                            use_container_width=True,
                        )
                        made_files_for_zip.append(out_path)
                    except Exception as e:
                        st.error(f"產生 BOL 失敗：{e}")

        # 若當次有多筆成功，可合併下載
        if made_files_for_zip:
            mem_zip = io.BytesIO()
            with zipfile.ZipFile(mem_zip, "w", zipfile.ZIP_DEFLATED) as zf:
                for p in made_files_for_zip:
                    if os.path.exists(p):
                        zf.write(p, arcname=os.path.basename(p))
            mem_zip.seek(0)
            st.download_button(
                "下載本次所有 BOL（ZIP）",
                data=mem_zip,
                file_name=f"BOL_ALL_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip",
                mime="application/zip",
                use_container_width=True,
            )

    # ======== 推送到 WMS（先人工修改） ========
    if st.button("推送到 WMS（先人工修改）", type="primary", use_container_width=True):
        selected = [r for r in edited if r.get("Select")]
        if not selected:
            st.warning("尚未選取任何訂單。")
        else:
            edit_map = {}
            default_wh_key = next(iter(WAREHOUSES.keys()))
            for row_preview in selected:
                oid = row_preview["OriginalTxnId"]
                group = grouped.get(oid, [])
                if not group:
                    continue
                pickup_str = default_pickup_date_str()
                params = build_wms_params_from_group(oid, group, default_wh_key, pickup_str)
                edit_map[oid] = {"params": params}
            st.session_state["wms_edit_map"] = edit_map
            st.success(f"已建立 {len(edit_map)} 筆預設上傳資料，請在下方逐筆人工修改後送出。")

    wms_edit_map = st.session_state.get("wms_edit_map")
    if wms_edit_map:
        st.markdown("### 📝 推送前人工修改（WMS）")
        st.caption("每筆資料都可修改（含取件日期、SKU/數量、warehouse_code 等），確認後再送出。")

        for oid, rec in wms_edit_map.items():
            p = rec["params"]
            m = re.search(r"pick up:\s*(\d{4}-\d{2}-\d{2})", p.get("order_desc") or "")
            pickup_default = m.group(1) if m else default_pickup_date_str()

            with st.expander(f"🛠 人工修改：{oid}"):
                col_pd, col_wc = st.columns(2)
                with col_pd:
                    new_pickup_date = st.date_input(
                        "Pick up date",
                        value=datetime.fromisoformat(pickup_default).date(),
                        key=f"{oid}_pickup",
                    )
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

                    # 由 warehouse_code 反查倉別鍵（若找不到就隨機選一個）
                    target_wh_key = None
                    for k, cfg in WMS_CONFIGS.items():
                        if cfg.get("WAREHOUSE_CODE") == new_params.get("warehouse_code"):
                            target_wh_key = k
                            break
                    if not target_wh_key:
                        target_wh_key = next(iter(WMS_CONFIGS.keys()))

                    cfg = WMS_CONFIGS.get(target_wh_key, {})
                    endpoint = cfg.get("ENDPOINT_URL","").strip()
                    app_token = cfg.get("APP_TOKEN","").strip()
                    app_key = cfg.get("APP_KEY","").strip()

                    if not (endpoint and app_token and app_key):
                        st.error(f"{target_wh_key} WMS 設定不完整（endpoint/app_token/app_key）。")
                    else:
                        try:
                            resp2 = send_create_order(endpoint, app_token, app_key, new_params, service=WMS_SERVICE)
                            text2 = resp2.text[:5000]
                            st.text_area("回應（前 5000 字）", text2, height=160)

                            parsed2 = _try_extract_json(text2)
                            if parsed2:
                                st.json(parsed2)
                                if (str(parsed2.get("ask", "")).lower() == "success") or (str(parsed2.get("error_code", "")) == "0"):
                                    st.success("✅ WMS 上傳成功！")
                                else:
                                    st.warning("⚠️ WMS 回傳非成功狀態，請檢查上方 JSON/回應內容。")
                            else:
                                if ("\"ask\":\"Success\"" in text2) or ("\"message\":\"Success\"" in text2):
                                    st.success("✅ WMS 上傳成功！")
                                else:
                                    st.info(f"HTTP {resp2.status_code}，請檢查回應內容。")
                        except Exception as e:
                            st.error(f"上傳失敗：{e}")
else:
    st.info("請先在左側按『抓取訂單』或『搜尋 PO（14 天內）』。")
