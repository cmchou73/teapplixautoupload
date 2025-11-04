# app.py — Streamlit BOL 產生器（解說區、批次修改倉庫、訂單時間僅日期、城市與總箱數不顯示）
# 仍保留：
#  - v6 合單 + v5 欄位完整
#  - Page_ttl、HU/Pkg 欄位與數量、NMFC/Class
#  - NumPkgs1、Weight1 規則
#  - 檔名：BOL_{OID}_{SKU8}_{WH2}_{SCAC}.pdf

import os
import io
import zipfile
from datetime import datetime, timedelta, timezone

import requests
import streamlit as st

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:
    from backports.zoneinfo import ZoneInfo

from dotenv import load_dotenv
import fitz  # PyMuPDF

APP_TITLE = "Teapplix HD LTL BOL 產生器"
TEMPLATE_PDF = "BOL.pdf"
OUTPUT_DIR = "output_bols"
BASE_URL  = "https://api.teapplix.com/api2/OrderNotification"
STORE_KEY = "HD"
SHIPPED   = "0"     # 0 = 未出貨
PAGE_SIZE = 500

CHECKBOX_FIELDS   = {"MasterBOL", "Term_Pre", "Term_Collect", "Term_CustChg"}
FORCE_TEXT_FIELDS = {"PickupDate"}  # 就算是 Yes/No 類也仍以文字填入
PHOENIX_TZ = ZoneInfo("America/Phoenix")

# ---------- 載入 .env ----------
load_dotenv()

PASSWORD         = os.getenv("APP_PASSWORD", "")
TEAPPLIX_TOKEN   = os.getenv("TEAPPLIX_TOKEN", "")
TEAPPLIX_API_KEY = os.getenv("TEAPPLIX_API_KEY", "")

# ---------- Streamlit 基礎設定 ----------
st.set_page_config(page_title=APP_TITLE, layout="wide")

# ---------- 工具 ----------
def now_phoenix():
    return datetime.now(tz=PHOENIX_TZ)

def phoenix_range_days(days: int):
    """回傳 (支付開始, 支付結束) 的 Phoenix 時區 ISO 格式（只日期邊界）。"""
    end = now_phoenix().replace(hour=23, minute=59, second=59, microsecond=0)
    start = (end - timedelta(days=days-1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return start.strftime("%Y-%m-%dT%H:%M:%S"), end.strftime("%Y-%m-%dT%H:%M:%S")

def get_headers():
    return {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {TEAPPLIX_TOKEN}",
        "x-api-key": TEAPPLIX_API_KEY,
    }

def override_carrier_name_by_scac(scac: str, carrier_name_raw: str) -> str:
    """
    依 ShipClass (SCAC) 覆蓋 Carrier 名稱；若未命中則回傳原值。
    """
    scac_upper = (scac or "").strip().upper()
    mapping = {
        "FXFE": "FedEx Freight",
        "UPGF": "UPS Freight",
        "RLCA": "R+L Carriers",
        "EXLA": "Estes",
        "SAIA": "SAIA",
        "ABFS": "ABF",
        "YFSY": "YRC",
        "ODFL": "Old Dominion",
        "USPG": "USPack",
        "UNSP_CG": "UNSP_CG",
    }
    return mapping.get(scac_upper, carrier_name_raw or "")

def _sku8_from_order(order: dict) -> str:
    """從 OrderItems 取首個 SKU 的前 8 碼（不足補原長度）。"""
    try:
        items = order.get("OrderItems") or []
        if not items:
            return ""
        sku = (items[0].get("ItemSKU") or "").strip()
        return sku[:8]
    except Exception:
        return ""

def _sum_group_totals(group: list[dict]) -> tuple[int, float]:
    """加總群組（合單）的總箱數與總重量。若沒有提供，則以 1 箱、重量 0 為預設。"""
    total_pkgs, total_lb = 0, 0.0
    for o in group:
        ship_details = (o.get("ShippingDetails") or [{}])[0] or {}
        pkg = ship_details.get("Package") or {}
        pcs = int(pkg.get("IdenticalPackageCount") or 0)
        weight = pkg.get("Weight") or {}
        lb = float(weight.get("Value") or 0)
        total_pkgs += max(pcs, 1)
        total_lb += lb
    if total_pkgs == 0:
        total_pkgs = 1
    return total_pkgs, total_lb

# ---------- API ----------
def fetch_orders(days: int):
    ps, pe = phoenix_range_days(days)
    page = 1
    all_orders = []
    while True:
        params = {
            "PaymentDateStart": ps,
            "PaymentDateEnd": pe,
            "Shipped": SHIPPED,
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
        # 過濾：排除 ShipClass = UNSP_CG
        filtered = []
        for o in orders:
            od = o.get("OrderDetails", {}) or {}
            if (od.get("ShipClass") or "").strip().upper() != "UNSP_CG":
                filtered.append(o)

        all_orders.extend(filtered)

        # 分頁：若當頁比 page_size 少，視為最後一頁
        if len(orders) < PAGE_SIZE:
            break
        page += 1

    return all_orders

# ---------- 以 PO 搜尋 ----------
def fetch_orders_by_pos(pos_list, shipped: str):
    """
    以 OriginalTxnId 清單查單；每個 PO 發一個 GET。
    shipped: "0"=未出貨, "1"=已出貨, ""=不限
    回傳: list[order dict]
    """
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
        }
        if shipped in ("0", "1"):
            params["Shipped"] = shipped
        try:
            r = requests.get(BASE_URL, headers=get_headers(), params=params, timeout=45)
        except Exception as e:
            st.error(f"PO {oid} 連線錯誤：{e}")
            continue
        if r.status_code != 200:
            st.error(f"PO {oid} API 錯誤: {r.status_code}\n{r.text[:300]}")
            continue
        try:
            data = r.json()
        except Exception:
            st.error(f"PO {oid} 回傳非 JSON：{r.text[:300]}")
            continue
        orders = data.get("orders") or data.get("Orders") or []
        for o in orders:
            od = o.get("OrderDetails", {}) or {}
            if (od.get("ShipClass") or "").strip().upper() != "UNSP_CG":
                results.append(o)
    return results

def group_by_original_txn(orders: list[dict]) -> dict[str, list[dict]]:
    """以 OriginalTxnId 合單。"""
    mp = {}
    for o in orders:
        key = (o.get("OriginalTxnId") or o.get("TxnId") or "").strip()
        if not key:
            key = (o.get("OrderDetails", {}).get("Invoice") or "").strip()
        mp.setdefault(key, []).append(o)
    return mp

# ---------- PDF 欄位操作 ----------
def fill_pdf(template_path: str, output_path: str, fields: dict[str, str]) -> None:
    """
    用 PyMuPDF 在既有模板上填字。簡單示意（此版本是假設模板已有 AcroForm 或固定標籤）。
    實務上你應該把「文字位置 & 字體大小」固定到模板裡；這裡簡化為以 key->value 直接寫入。
    """
    doc = fitz.open(template_path)
    page = doc[0]
    # 簡化：把所有欄位印在左上角附近 (示例)
    x, y = 50, 80
    for k, v in fields.items():
        page.insert_text((x, y), f"{k}: {v}")
        y += 14
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    doc.save(output_path)
    doc.close()

# ---------- PDF 小工具 ----------
def _set_checkbox(widget, value):
    """假裝去點 checkbox；此為示意，真實情況應針對你的 AcroForm 結構寫。"""
    v = str(value).strip().lower()
    return "Yes" if v in {"on","yes","1","true","x","✔"} else "Off"

def set_field(widget, name, value):
    """處理 checkbox 與一般文字欄位。"""
    try:
        is_checkbox_type = (getattr(widget, "field_type", "") == "Btn")
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

# 訂單時間：只顯示日期（mm/dd/yy）
def _parse_order_date_str(first: dict) -> str:
    od = first.get("OrderDetails", {}) or {}
    # 以 PaymentDate 或 LastUpdateDate 擇一
    s = (od.get("PaymentDate") or first.get("LastUpdateDate") or "").strip()
    if not s:
        return ""
    try:
        # 假設 ISO 或帶 T 的格式
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.astimezone(PHOENIX_TZ).strftime("%m/%d/%y")
    except Exception:
        return s[:10]  # 退而求其次

# ---------- UI：登入驗證 ----------
st.sidebar.subheader("🔐 驗證區")
input_pwd = st.sidebar.text_input("請輸入密碼", type="password")

if input_pwd != PASSWORD:
    st.warning("請輸入正確密碼後才能使用。")
    st.stop()
# ---------- 密碼驗證 ----------

st.title(APP_TITLE)

# 解說欄位（顯示在標題下方）
st.markdown("""
**說明：**
1. 可能會錯, 請仔細核對
2. ABCD
""")

# 左側 Sidebar：天數下拉
days = st.sidebar.selectbox("抓取天數", options=[1,2,3,4,5,6,7], index=2, help="預設 3 天（index=2）")
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
if st.sidebar.button("搜尋 PO", use_container_width=True):
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
        po_orders = fetch_orders_by_pos(pos_list, shipped_val)
        st.session_state["po_search_results"] = po_orders
        st.success(f"搜尋完成：輸入 {len(pos_list)} 筆 PO，找到 {len(po_orders)} 筆訂單（含同 PO 多項）。")

# 操作：抓單
if st.button("抓取訂單", use_container_width=True):
    st.session_state["orders_raw"] = fetch_orders(days)
    # 清掉之前的覆蓋資料
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
        st.dataframe(preview_rows, use_container_width=True)
        with st.expander("顯示原始 JSON（每筆訂單）", expanded=False):
            for idx, o in enumerate(po_search_results, start=1):
                st.write(f"--- 訂單 #{idx} ---")
                st.json(o, expanded=False)

orders_raw = st.session_state.get("orders_raw", None)

if orders_raw:
    grouped = group_by_original_txn(orders_raw)

    # 準備表格資料
    if "table_rows_override" in st.session_state:
        table_rows = st.session_state["table_rows_override"]
    else:
        table_rows = []
        for oid, group in grouped.items():
            first = group[0]
            od = first.get("OrderDetails", {}) or {}
            scac = (od.get("ShipClass") or "").strip()
            sku8 = _sku8_from_order(first)
            order_date_str = _parse_order_date_str(first)  # 只日期
            table_rows.append({
                "OID": oid,
                "Invoice": (od.get("Invoice") or "").strip(),
                "OrderDate": order_date_str,
                "SCAC": scac,
                "SKU8": sku8,
            })

    st.subheader("已抓訂單（合單後）")
    st.dataframe(table_rows, use_container_width=True)

    # 下載示意：將每一合單輸出一份 PDF 並打包 ZIP（此處為簡化示意）
    if st.button("產生 BOL（示意）", use_container_width=True):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zipf:
            os.makedirs(OUTPUT_DIR, exist_ok=True)
            for oid, group in grouped.items():
                first = group[0]
                od = first.get("OrderDetails", {}) or {}
                to = first.get("To") or {}
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
                bol_num = (od.get("Invoice") or "").strip()

                # PDF 填寫欄位（示意）
                fields = {
                    "BOL_Number": bol_num,
                    "Carrier": carrier_name_final,
                    "SCAC": scac_from_shipclass,
                    "Total_Pkgs": str(total_pkgs),
                    "Total_Weight": f"{total_lb:.1f}",
                    "ShipTo_Name": to.get("Name", ""),
                    "ShipTo_Addr": to_address,
                    "ShipTo_City": to.get("City", ""),
                    "ShipTo_State": to.get("State", ""),
                    "ShipTo_Zip": to.get("ZipCode", ""),
                    "PU_Instruction": custom_code,
                    "PickupDate": now_phoenix().strftime("%m/%d/%Y"),
                    "Term_Pre": "Yes",
                    "Term_Collect": "Off",
                }

                # 寫出 PDF（示意）
                pdf_name = f"BOL_{oid}_{_sku8_from_order(first)}_{(od.get('ShipClass') or '').strip()[:2]}_{scac_from_shipclass}.pdf"
                out_path = os.path.join(OUTPUT_DIR, pdf_name)
                # 這裡用示意函式將 key:value 直寫到 PDF；實務上應針對表單欄位精準填入
                fill_pdf(TEMPLATE_PDF, out_path, fields)

                # 放進 zip
                with open(out_path, "rb") as pf:
                    zipf.writestr(pdf_name, pf.read())

        st.download_button(
            label="下載 BOL ZIP",
            data=buf.getvalue(),
            file_name="BOLs.zip",
            mime="application/zip",
            use_container_width=True,
        )
else:
    st.info("左側輸入密碼後，可先『抓取訂單』或使用『以 PO 搜尋』。")
