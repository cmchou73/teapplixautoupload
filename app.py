#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Streamlit app — 單檔完整版本
需求重點：
1) 「產生 BOL」與「推送到 WMS」兩顆按鈕改成左右並排（不再一上一下），顏色更好看。
2) 取消「批次指定倉庫 / 套用對象 / 套用批次倉庫」與搜尋結果中的「倉庫」欄。
   並改為：在「人工修改」介面裡，以「倉庫」下拉選擇，然後才進行「產生 BOL / 推送到 WMS」。
   （因此原本上層的「產生 BOL / 推送到 WMS」改成「進入人工修改（勾選列）」）
"""
from __future__ import annotations

import io
import os
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any

try:
    import streamlit as st
except Exception as e:  # pragma: no cover
    raise SystemExit("請先安裝 streamlit：pip install streamlit") from e

# ======（可依實際環境調整）======
OUTPUT_DIR = "output_bol"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 倉庫清單（示例）
WAREHOUSES: Dict[str, Dict[str, Any]] = {
    "Walnut, CA": {"name": "Walnut, CA", "code": "CA91789"},
    "East Brunswick, NJ": {"name": "East Brunswick, NJ", "code": "NJ08816"},
    "Dallas, TX": {"name": "Dallas, TX", "code": "TX75241"},
}

# WMS 連線設定（示例）— 依倉庫鍵對應；實務請替換為正確憑證
WMS_CONFIGS: Dict[str, Dict[str, str]] = {
    "Walnut, CA": {
        "ENDPOINT_URL": "",  # 例: "https://wms.example.com/api/createOrder"
        "APP_TOKEN": "",
        "APP_KEY": "",
        "WAREHOUSE_CODE": "CA91789",
    },
    "East Brunswick, NJ": {
        "ENDPOINT_URL": "",
        "APP_TOKEN": "",
        "APP_KEY": "",
        "WAREHOUSE_CODE": "NJ08816",
    },
    "Dallas, TX": {
        "ENDPOINT_URL": "",
        "APP_TOKEN": "",
        "APP_KEY": "",
        "WAREHOUSE_CODE": "TX75241",
    },
}

# ====== 假資料載入（你可以改成讀取真實訂單來源）======
def load_orders() -> List[Dict[str, Any]]:
    """回傳一份『已分組』的訂單清單（每個 OriginalTxnId 可能含多筆行項）。
    每個元素代表一筆 line-item，欄位示例：
        - OriginalTxnId: str（訂單編號）
        - SKU8: str（SKU / 取前 8 碼顯示用）
        - SCAC: str（承運商）
        - ToState: str（州）
        - OrderDate: str（"YYYY-MM-DD"）
        - Qty: int
    """
    today = datetime.now().date()
    demo = [
        {
            "OriginalTxnId": "PO10001",
            "SKU8": "FFP12345",
            "SCAC": "FEDEX",
            "ToState": "AZ",
            "OrderDate": str(today - timedelta(days=1)),
            "Qty": 1,
        },
        {
            "OriginalTxnId": "PO10001",
            "SKU8": "FFP12345",
            "SCAC": "FEDEX",
            "ToState": "AZ",
            "OrderDate": str(today - timedelta(days=1)),
            "Qty": 2,
        },
        {
            "OriginalTxnId": "PO10002",
            "SKU8": "TVS99999",
            "SCAC": "UPS",
            "ToState": "CA",
            "OrderDate": str(today - timedelta(days=2)),
            "Qty": 1,
        },
        {
            "OriginalTxnId": "PO10003",
            "SKU8": "VAN00001",
            "SCAC": "R+L",
            "ToState": "NJ",
            "OrderDate": str(today - timedelta(days=3)),
            "Qty": 1,
        },
    ]
    return demo


def _group_by_order(lines: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in lines:
        oid = row.get("OriginalTxnId", "")
        grouped.setdefault(oid, []).append(row)
    return grouped


def default_pickup_date_str(days: int = 2) -> str:
    return (datetime.now() + timedelta(days=days)).date().isoformat()


def build_table_rows_from_orders(grouped: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """建立上層 data_editor 使用的合併列（不顯示倉庫欄）。"""
    rows: List[Dict[str, Any]] = []
    for oid, group in grouped.items():
        first = group[0]
        sku8 = str(first.get("SKU8", ""))[:8]
        scac = str(first.get("SCAC", ""))
        to_state = str(first.get("ToState", ""))
        dt = first.get("OrderDate", "")
        try:
            order_date = datetime.strptime(dt, "%Y-%m-%d").strftime("%m/%d/%y") if dt else ""
        except Exception:
            order_date = dt
        rows.append({
            "Select": True,
            "OriginalTxnId": oid,
            "SKU8": sku8,
            "SCAC": scac,
            "ToState": to_state,
            "OrderDate": order_date,
        })
    return rows


def _safe_import_reportlab():
    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.pdfgen import canvas
        return True, (letter, canvas)
    except Exception:
        return False, (None, None)


def generate_bol_pdf_stub(order_id: str, wh_name: str, wh_code: str, scac: str, items: List[Dict[str, Any]]) -> bytes:
    """以 reportlab 建一份極簡 BOL；若缺少 reportlab，退回純文字檔 bytes。"""
    ok, (letter, canvas) = _safe_import_reportlab()
    if ok:
        buf = io.BytesIO()
        c = canvas.Canvas(buf, pagesize=letter)
        width, height = letter
        y = height - 50
        c.setFont("Helvetica-Bold", 14)
        c.drawString(30, y, f"BOL — Order {order_id}")
        y -= 24
        c.setFont("Helvetica", 11)
        c.drawString(30, y, f"Warehouse: {wh_name} ({wh_code})")
        y -= 16
        c.drawString(30, y, f"SCAC: {scac}")
        y -= 24
        c.setFont("Helvetica-Bold", 12)
        c.drawString(30, y, "Items:")
        y -= 18
        c.setFont("Helvetica", 10)
        for it in items:
            line = f"- {it.get('product_sku','')}  x{it.get('quantity',1)}"
            c.drawString(40, y, line)
            y -= 14
            if y < 60:
                c.showPage()
                y = height - 50
        c.showPage()
        c.save()
        pdf = buf.getvalue()
        buf.close()
        return pdf
    else:
        # fallback 純文字
        lines = [
            f"BOL — Order {order_id}",
            f"Warehouse: {wh_name} ({wh_code})",
            f"SCAC: {scac}",
            "Items:",
        ] + [f"- {it.get('product_sku','')}  x{it.get('quantity',1)}" for it in items]
        return "\n".join(lines).encode("utf-8")


def send_create_order(endpoint: str, app_token: str, app_key: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """推送到 WMS（示例：僅模擬回傳）。
    若你要實接 API，可改成 requests.post(...) 並回傳 JSON。
    """
    # 模擬耗時
    time.sleep(0.3)
    if not endpoint:
        # 若沒有設定 endpoint，直接回傳成功 = False 的訊息
        return {"ask": "Error", "message": "ENDPOINT_URL 未設定，僅模擬回傳。", "echo": params}
    # 真實情境可改：
    # import requests
    # r = requests.post(endpoint, json=params, headers={"X-APP-TOKEN": app_token, "X-APP-KEY": app_key}, timeout=30)
    # return r.json()
    return {"ask": "Success", "message": "OK (mock)", "echo": params}


def _items_from_group(group: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """把 line-items 轉為 WMS 需要的 items 結構（簡化版）。"""
    items = []
    for r in group:
        items.append({"product_sku": r.get("SKU8", ""), "quantity": int(r.get("Qty", 1))})
    return items


def _sku8_from_group(group: List[Dict[str, Any]]) -> str:
    if not group:
        return "NOSKU"
    return str(group[0].get("SKU8", ""))[:8] or "NOSKU"


def _nice_two_letters(s: str) -> str:
    if not s:
        return "WH"
    return "".join(ch for ch in s if ch.isalpha()).upper()[:2] or "WH"


# ============ Streamlit UI ============
st.set_page_config(page_title="BOL & WMS Tool", page_icon="🧾", layout="wide")

# (1) 少量 CSS：讓主按鈕更顯眼、左右並排更緊湊
st.markdown("""
<style>
/* 讓 primary 顏色更飽和一點（不破壞主題） */
.stButton > button[kind="primary"] {
    border-radius: 12px;
    box-shadow: 0 4px 10px rgba(0,0,0,0.12);
    font-weight: 600;
}
/* expander header 加強 */
.streamlit-expanderHeader {
    font-weight: 700 !important;
}
</style>
""", unsafe_allow_html=True)

st.title("🧾 BOL 產生 & 🚚 WMS 推送 — 新流程")

# 載入資料
orders = load_orders()
grouped = _group_by_order(orders)

# (2) 上層搜尋/篩選（簡化示例）
with st.container():
    qcol1, qcol2, qcol3 = st.columns([2,1,1])
    with qcol1:
        kw = st.text_input("關鍵字搜尋（PO / SKU / SCAC / 州）", value="")
    with qcol2:
        date_from = st.date_input("起日", value=(datetime.now().date() - timedelta(days=10)))
    with qcol3:
        date_to = st.date_input("迄日", value=datetime.now().date())

# 依關鍵字過濾（示例）
if kw:
    grouped = {
        oid: g for oid, g in grouped.items()
        if (kw.lower() in oid.lower()
            or any(kw.lower() in str(x.get("SKU8","")).lower() for x in g)
            or any(kw.lower() in str(x.get("SCAC","")).lower() for x in g)
            or any(kw.lower() in str(x.get("ToState","")).lower() for x in g))
    }

# 建合併列（不含倉庫欄位）
table_rows = build_table_rows_from_orders(grouped)

st.subheader("訂單清單（勾選後 → 進入人工修改）")
edited_rows = st.data_editor(
    table_rows,
    num_rows="fixed",
    hide_index=True,
    use_container_width=True,
    column_config={
        "Select": st.column_config.CheckboxColumn("選取", default=True),
        "OriginalTxnId": st.column_config.TextColumn("PO", disabled=True),
        "SKU8": st.column_config.TextColumn("SKU", disabled=True),
        "SCAC": st.column_config.TextColumn("SCAC", disabled=True),
        "ToState": st.column_config.TextColumn("州", disabled=True),
        "OrderDate": st.column_config.TextColumn("訂單日期 (mm/dd/yy)", disabled=True),
    },
    key="orders_table",
)

# 取代過去上層兩顆按鈕 → 只保留「進入人工修改」
act_l, act_r = st.columns(2)
with act_l:
    if st.button("🛠 進入人工修改（勾選列）", type="primary", use_container_width=True, key="enter_manual_edit"):
        selected = [r for r in edited_rows if r.get("Select")]
        if not selected:
            st.warning("尚未選取任何訂單。")
        else:
            edit_map = {}
            for row in selected:
                oid = row["OriginalTxnId"]
                grp = grouped.get(oid, [])
                if not grp:
                    continue
                # 預設 + 空白 WMS 參數
                pickup_str = default_pickup_date_str()
                items = _items_from_group(grp)
                edit_map[oid] = {
                    "WarehouseKey": None,
                    "params": {
                        "warehouse_code": "",     # 由下拉選擇後注入
                        "tracking_no": "",
                        "reference_no": "",
                        "order_desc": f"pick up: {pickup_str}",
                        "platform_shop": "",
                        "remark": "",
                        "items": items,
                    },
                    "pickup_default": pickup_str,
                }
            st.session_state["wms_edit_map"] = edit_map
            st.session_state["wms_groups"] = grouped
            st.success(f"已建立 {len(edit_map)} 筆預設資料，請於下方逐筆人工修改。")

with act_r:
    st.caption("選取上方訂單後，點擊 ⬅️ 進入人工修改。")

st.divider()

# ===== 下方逐筆人工修改（若已建立） =====
edit_map = st.session_state.get("wms_edit_map") or {}
wms_groups = st.session_state.get("wms_groups") or {}

if edit_map:
    st.subheader("人工修改（逐筆）")
    for oid, rec in edit_map.items():
        grp = wms_groups.get(oid, [])
        if not grp:
            continue
        sku8 = _sku8_from_group(grp)
        scac = str(grp[0].get("SCAC", ""))
        pickup_default = rec.get("pickup_default", default_pickup_date_str())
        p = rec.get("params", {})

        with st.expander(f"PO: {oid} — SKU: {sku8} — SCAC: {scac}", expanded=True):
            col_pd, col_wc = st.columns(2)
            with col_pd:
                new_pickup_date = st.date_input(
                    "Pick up date",
                    value=datetime.fromisoformat(pickup_default).date(),
                    key=f"{oid}_pickup",
                )
            with col_wc:
                wh_key = st.selectbox(
                    "倉庫",
                    options=list(WAREHOUSES.keys()),
                    index=0,
                    key=f"{oid}_whkey",
                )
                wh_code_preview = WMS_CONFIGS.get(wh_key, {}).get("WAREHOUSE_CODE", "")
                st.text_input("warehouse_code（自動）", value=wh_code_preview, key=f"{oid}_whc", disabled=True)

            c1, c2 = st.columns(2)
            with c1:
                new_tracking = st.text_input("tracking_no", value=p.get("tracking_no",""), key=f"{oid}_trk")
                new_platform_shop = st.text_input("platform_shop", value=p.get("platform_shop",""), key=f"{oid}_pshop")
            with c2:
                new_ref = st.text_input("reference_no", value=p.get("reference_no",""), key=f"{oid}_ref")
                new_remark = st.text_input("remark", value=p.get("remark",""), key=f"{oid}_remark")

            st.markdown("**Items**")
            new_items: List[Dict[str, Any]] = []
            for idx, it in enumerate(p.get("items", [])):
                col1, col2 = st.columns(2)
                with col1:
                    new_sku = st.text_input(f"product_sku #{idx+1}", value=it.get("product_sku",""), key=f"{oid}_sku_{idx}")
                with col2:
                    new_qty = st.number_input(f"quantity #{idx+1}", value=int(it.get("quantity",1)), min_value=1, step=1, key=f"{oid}_qty_{idx}")
                new_items.append({"product_sku": new_sku.strip(), "quantity": int(new_qty)})

            # === 左右並排主操作按鈕 ===
            bl, br = st.columns(2)

            with bl:
                if st.button("🧾 產生 BOL（此筆）", type="primary", use_container_width=True, key=f"make_bol_{oid}"):
                    wh_name = WAREHOUSES.get(wh_key, {}).get("name", wh_key)
                    wh_code = WMS_CONFIGS.get(wh_key, {}).get("WAREHOUSE_CODE", "")
                    pdf_bytes = generate_bol_pdf_stub(order_id=oid, wh_name=wh_name, wh_code=wh_code, scac=scac, items=new_items)
                    sku2 = _nice_two_letters(sku8)
                    wh2 = _nice_two_letters(wh_name)
                    sc2 = _nice_two_letters(scac)
                    filename = f"BOL_{oid}_{sku2}_{wh2}_{sc2}.pdf"
                    path = os.path.join(OUTPUT_DIR, filename)
                    try:
                        with open(path, "wb") as f:
                            f.write(pdf_bytes)
                        st.download_button("⬇️ 下載本筆 BOL", data=pdf_bytes, file_name=filename, mime="application/pdf", use_container_width=True)
                        st.success(f"已產生 BOL：{filename}")
                    except Exception as e:
                        st.error(f"產生/寫入 BOL 檔案失敗：{e}")

            with br:
                if st.button("🚚 推送到 WMS（此筆）", type="primary", use_container_width=True, key=f"send_wms_{oid}"):
                    new_order_desc = f"pick up: {new_pickup_date.isoformat()}"
                    wh_code = WMS_CONFIGS.get(wh_key, {}).get("WAREHOUSE_CODE", "")
                    new_params = dict(p)
                    new_params.update({
                        "warehouse_code": wh_code,
                        "tracking_no": new_tracking.strip(),
                        "reference_no": new_ref.strip(),
                        "order_desc": new_order_desc,
                        "platform_shop": new_platform_shop.strip(),
                        "remark": new_remark,
                        "items": new_items,
                    })
                    cfg = WMS_CONFIGS.get(wh_key, {})
                    endpoint = cfg.get("ENDPOINT_URL","").strip()
                    app_token = cfg.get("APP_TOKEN","").strip()
                    app_key = cfg.get("APP_KEY","").strip()

                    # 基本檢查
                    if not (wh_code):
                        st.error("此倉庫的 WAREHOUSE_CODE 未設定。")
                    else:
                        try:
                            resp = send_create_order(endpoint, app_token, app_key, new_params)
                            st.text_area("回應（JSON or Text）", json.dumps(resp, ensure_ascii=False, indent=2), height=160)
                            if str(resp.get("ask","")).lower() == "success":
                                st.success("✅ WMS 上傳成功！")
                            else:
                                st.warning("⚠️ WMS 回傳非成功狀態，請檢查上方回應內容。")
                        except Exception as e:
                            st.error(f"上傳失敗：{e}")
else:
    st.info("尚未建立人工修改資料。請先於上方勾選訂單並點擊「進入人工修改」。")


st.caption("小提示：若需要客製顏色或品牌化樣式，可再加入全域 CSS 或切換主題。")
