# app1.py — Streamlit BOL 產生器（含左側「以 PO 搜尋」, 帶時間範圍）
# 更新要點：
# - 左側以 OriginalTxnId(=PO) 搜尋（每行一個），Shipped 可選 0/1/不限
# - ★ 依「抓取天數」帶入 PaymentDateStart / PaymentDateEnd，滿足 API 最小參數要求
# - 查詢使用 GET，避免 POST Submit 導致 TxnId 必填
# - Header 含 APIToken；如需 Authorization / x-api-key 可在 secrets 或 .env 設定
# - Streamlit 全面改 width="stretch"

import os
import io
import zipfile
from datetime import datetime, timedelta

import requests
import streamlit as st

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

from dotenv import load_dotenv
import fitz  # PyMuPDF

APP_TITLE = "Teapplix HD LTL BOL 產生器"
TEMPLATE_PDF = "BOL.pdf"
OUTPUT_DIR = "output_bols"
BASE_URL  = "https://api.teapplix.com/api2/OrderNotification"
STORE_KEY = "HD"
SHIPPED_DEFAULT = "0"   # 一般抓單的預設（未出貨）
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
AUTH_BEARER    = _sec("TEAPPLIX_AUTH_BEARER", "")  # 若你的租戶也需要 Authorization: Bearer，可設定
X_API_KEY      = _sec("TEAPPLIX_X_API_KEY", "")    # 若需要 x-api-key，可設定
PASSWORD       = _sec("APP_PASSWORD", "")

# UI 倉庫代號
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

# ---------- utils ----------
def phoenix_range_days(days=3):
    """回傳 Phoenix 時區的 [開始, 結束] ISO 字串（含當天 23:59:59）。"""
    tz = ZoneInfo("America/Phoenix")
    now = datetime.now(tz)
    end   = now.replace(hour=23, minute=59, second=59, microsecond=0)
    start = (end - timedelta(days=days-1)).replace(hour=0, minute=0, second=0, microsecond=0)
    fmt = "%Y-%m-%dT%H:%M:%S"
    return start.strftime(fmt), end.strftime(fmt)

def get_headers():
    """依租戶需求帶入憑證。"""
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
    try: return round(float(oz)/16.0, 2)
    except Exception: return None

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
    try: return int(it.get("Quantity") or 0)
    except Exception: return 0

def _sum_group_totals(group):
    total_pkgs = 0
    total_lb = 0.0
    for od in group:
        pkgs, lb = summarize_packages(od)
        total_pkgs += int(pkgs or 0)
        total_lb   += float(lb or 0.0)
    return total_pkgs, int(round(total_lb))

# 訂單時間：只顯示日期（mm/dd/yy）
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
    if not raw:
        return ""
    val = str(raw).strip()

    dt = None
    try:
        if "T" in val:
            dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
        else:
            for fmt in ("%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d", "%Y-%m-%d"):
                try:
                    dt = datetime.strptime(val, fmt)
                    break
                except Exception:
                    continue
    except Exception:
        dt = None

    if dt is None:
        try:
            dt = datetime.fromisoformat(val[:19])
        except Exception:
            return ""

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz_phx)
    dt_phx = dt.astimezone(tz_phx)
    return dt_phx.strftime("%m/%d/%y")  # 僅日期

# ---------- API：抓取一般訂單（GET） ----------
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
            st.error(f"API 錯誤: {r.status_code}\n{r.text}")
            break
        try:
            data = r.json()
        except Exception:
            st.error(f"JSON 解析錯誤：{r.text[:1000]}")
            break

        orders = data.get("orders") or data.get("Orders") or []
        if not orders:
            break

        for o in orders:
            od = o.get("OrderDetails") or {}
            if (od.get("ShipClass") or "").strip().upper() != "UNSP_CG":
                all_orders.append(o)

        if len(orders) < PAGE_SIZE:
            break
        page += 1
    return all_orders

# ---------- API：以 PO(OriginalTxnId) 透過 GET 查詢（帶 PaymentDateStart/End） ----------
def fetch_orders_by_pos(pos_list, shipped: str, days: int):
    """
    以 OriginalTxnId(=PO) 清單查單；每個 PO 發一個 GET。
    - 一律附帶 PaymentDateStart/End（依左側抓取天數）以滿足最小查詢條件。
    - shipped: "0"=未出貨, "1"=已出貨, ""=不限
    回傳: list[order dict]
    """
    ps, pe = phoenix_range_days(days)
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
            # ★ 加入時間範圍，避免 "Minimum list of parameters" 400
            "PaymentDateStart": ps,
            "PaymentDateEnd": pe,
        }
        # 若有指定 Shipped 就帶上（不限則不帶）
        if shipped in ("0", "1"):
            params["Shipped"] = shipped

        try:
            r = requests.get(BASE_URL, headers=get_headers(), params=params, timeout=45)
        except Exception as e:
            st.error(f"PO {oid} 連線錯誤：{e}")
            continue

        if r.status_code != 200:
            st.error(f"PO {oid} API 錯誤: {r.status_code}\n{r.text[:400]}")
            continue

        try:
            data = r.json()
        except Exception:
            st.error(f"PO {oid} 回傳非 JSON：{r.text[:400]}")
            continue

        orders = data.get("orders") or data.get("Orders") or []
        for o in orders:
            od = o.get("OrderDetails") or {}
            # 依你的規則排除 UNSP_CG（Channel Gate）
            if (od.get("ShipClass") or "").strip().upper() != "UNSP_CG":
                results.append(o)

    # 若指定 shipped，再做一次本地過濾（不同租戶欄位大小寫可能不同）
    if shipped in ("0", "1"):
        filtered = []
        for o in results:
            shp = str(o.get("Shipped") or o.get("shipped") or "").strip()
            if shp == shipped:
                filtered.append(o)
        results = filtered

    return results

# ---------- PDF 欄位建構/填寫（保留原有產 BOL 示意） ----------
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
        st.warning(f"填欄位 {name} 失敗：{e}")
        return False

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

# ---------- Streamlit UI ----------
st.set_page_config(page_title=APP_TITLE, layout="wide")

# ---------- 密碼驗證 ----------
st.sidebar.subheader("🔐 驗證區")
input_pwd = st.sidebar.text_input("請輸入密碼", type="password")
if input_pwd != PASSWORD:
    st.warning("請輸入正確密碼後才能使用。")
    st.stop()

st.title(APP_TITLE)

# 解說欄位（顯示在標題下方）
st.markdown("""
**說明：**
1. 可能會錯, 請仔細核對
2. ABCD
""")

if not TEAPPLIX_TOKEN:
    st.error("找不到 TEAPPLIX_TOKEN，請在 .env 或 Streamlit Secrets 設定。")
    st.stop()

# 左側 Sidebar：抓取天數（同時供 PO 搜尋與「抓取訂單」使用）
days = st.sidebar.selectbox("抓取天數", options=[1,2,3,4,5,6,7], index=2, help="預設 3 天（index=2）")

# === 左側「以 PO 搜尋（每行一個）」 ===
st.sidebar.markdown("---")
st.sidebar.subheader("🔎 以 PO 搜尋（每行一個）")
po_text = st.sidebar.text_area(
    "輸入 PO（OriginalTxnId）",
    placeholder="例如：\nHD-PO-12345\nHD-PO-67890",
    height=120,
)
shipped_choice = st.sidebar.selectbox(
    "出貨狀態（Shipped）",
    options=["不限", "未出貨(0)", "已出貨(1)"],
    index=0,
    help="0 = 未出貨，1 = 已出貨",
)
if st.sidebar.button("搜尋 PO", width="stretch"):
    raw_lines = (po_text or "").splitlines()
    pos_list = [ln.strip() for ln in raw_lines if ln.strip()]
    if not pos_list:
        st.warning("請輸入至少一個 PO（每行一個）。")
    else:
        shipped_val = ""
        if shipped_choice.endswith("(0)"):
            shipped_val = "0"
        elif shipped_choice.endswith("(1)"):
            shipped_val = "1"

        po_orders = fetch_orders_by_pos(pos_list, shipped_val, days)
        st.session_state["po_search_results"] = po_orders
        st.success(f"搜尋完成：輸入 {len(pos_list)} 筆 PO，找到 {len(po_orders)} 筆訂單（含同 PO 多項）。")

# 操作：抓單（原本功能）
if st.button("抓取訂單", width="stretch"):
    st.session_state["orders_raw"] = fetch_orders(days)
    st.session_state.pop("table_rows_override", None)

# ======== PO 搜尋結果呈現 ========
po_search_results = st.session_state.get("po_search_results", None)
if po_search_results is not None:
    st.header("🔎 PO 搜尋結果")
    if not po_search_results:
        st.info("沒有找到符合的訂單。")
    else:
        preview_rows = []
        for o in po_search_results:
            to = o.get("To") or {}
            od = o.get("OrderDetails") or {}
            ship_details = (o.get("ShippingDetails") or [{}])[0] or {}
            pkg = ship_details.get("Package") or {}
            tracking = pkg.get("TrackingInfo") or {}
            preview_rows.append({
                "PO": (o.get("OriginalTxnId") or "").strip(),
                "Invoice": (od.get("Invoice") or "").strip(),
                "ToName": to.get("Name", ""),
                "City": to.get("City", ""),
                "State": to.get("State", ""),
                "Zip": to.get("ZipCode", ""),
                "SCAC": (od.get("ShipClass") or "").strip(),
                "Carrier": tracking.get("CarrierName", ""),
                "Tracking": tracking.get("TrackingNumber", ""),
            })
        st.dataframe(preview_rows, width="stretch")
        with st.expander("顯示原始 JSON（每筆訂單）", expanded=False):
            for idx, o in enumerate(po_search_results, start=1):
                st.write(f"--- 訂單 #{idx} ---")
                st.json(o, expanded=False)

# ======== 原本「抓取訂單」流程的呈現與產 BOL（保留） ========
orders_raw = st.session_state.get("orders_raw", None)

def build_table_rows_from_orders(orders_raw):
    grouped = group_by_original_txn(orders_raw)
    table_rows = []
    for oid, group in grouped.items():
        first = group[0]
        od = first.get("OrderDetails") or {}
        scac = (od.get("ShipClass") or "").strip()
        sku8 = _sku8_from_order(first)
        order_date_str = _parse_order_date_str(first)
        table_rows.append({
            "Select": True,
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
                    if r2.get("Select"):
                        r2["Warehouse"] = bulk_wh
                    new_rows.append(r2)
            st.session_state["table_rows_override"] = new_rows
            table_rows = new_rows
            st.success("已套用批次倉庫變更。")

    # 表格（僅允許編輯 Warehouse 與 Select）
    edited = st.data_editor(
        st.session_state.get("table_rows_override", table_rows),
        num_rows="fixed",
        hide_index=True,
        column_config={
            "Select": st.column_config.CheckboxColumn("選取", default=True),
            "Warehouse": st.column_config.SelectboxColumn("倉庫", options=list(WAREHOUSES.keys())),
            "OriginalTxnId": st.column_config.TextColumn("PO", disabled=True),
            "SKU8": st.column_config.TextColumn("SKU", disabled=True),
            "SCAC": st.column_config.TextColumn("SCAC", disabled=True),
            "ToState": st.column_config.TextColumn("州", disabled=True),
            "OrderDate": st.column_config.TextColumn("訂單日期 (mm/dd/yy)", disabled=True),
        },
        key="orders_table",
    )

    # 產出 BOL（示意）
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
else:
    st.info("請先按『抓取訂單』或使用左側『以 PO 搜尋』。")
