#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import xml.etree.ElementTree as ET

# ====== 必填：改成你的實際 API 入口與憑證 ======
ENDPOINT_URL = "http://ecs.yunwms.com/default/svc/web-service"  # ← 替換為實際 SOAP 端點
APP_TOKEN = "27c1069aba561f1f34ef9d59abc44f7d"  # ← 你的 appToken
APP_KEY = "fc5b0b451744cd1fac6e5d0c1722cf71"      # ← 你的 appKey
SERVICE = "createOrder" # ← 服務名
# ===========================================

def build_params_dict() -> dict:
    """把你給的參數整理成 Python dict（確保序列化後是合法 JSON）。"""
    return {
        "platform": "OTHER",
        "allocated_auto": "0",
        "warehouse_code": "NJW",
        "shipping_method": "CUSTOMER_SHIP",
        "reference_no": "test-po2",
        #"aliexpress_order_no": "8000777788889999",
        "order_desc": "instruction?",
        "remark": "test-remark",
        #"order_business_type": "b2c",
        #"lp_order_number": "lp_123456",
        "country_code": "US",
        "province": "MA",
        "city": "Worcester",
        "district": "Worcester",
        "address1": "58 Fruit St.",
        "address2": "Unit 1010",
        "address3": "",
        "zipcode": "01609",
        #"license": "420904",
        #"doorplate": "doorplate",
        "company": "ff",
        "name": "Max Dog",
        "phone": "1234567890",
        "cell_phone": "cell_phone",
        "phone_extension": "",
        "email": "email",
        "platform_shop": "SAIA",
        #"is_order_cod": 0,
        #"order_cod_price": 99,
        #"order_cod_currency": "RMB",
        #"order_age_limit": 2,
        #"is_signature": 0,
        #"is_insurance": 0,
        #"insurance_value": 0,
        #"channel_code": "demo",
        #"packageCenterCode": "center_code",
        #"packageCenterName": "集包地中心名称",
        #"QrCode": "123ABC",
        #"shortAddress": "100-200-30-400",
        #"seller_id": "ebay-test001",
        #"buyer_id": "ebay-test002",
        #"only_logistics": 0,
        #"assign_date": "2021-01-01",
        #"assign_time": "02",
        "items": [
            {
                "product_sku": "FTS23235",
                #"reference_no": "149H6286",
                #"product_name_en": "Product Name",
                #"product_name": "Product Name",
                #"product_declared_value": 5.0,
                "quantity": 1,
                #"ref_tnx": "1495099983020",
                #"ref_item_id": "302588235574",
                #"ref_buyer_id": "esramo_a62szxok",
                #"already_taxed": "I_TAXED",
                #"child_order_id": "child_order_id",
                #"batch_info": [
                #    {
                #        "inventory_code": "RVJRY-220308-0008_EA140509201610_444_20220308151424",
                #        "sku_quantity": "1"
                #    }
                #],
                #"batch_number": ["abc", "def", "ghi"]
            },
            {
                "product_sku": "ZHX-26-26-022-3A",
                #"reference_no": "149H6286",
                #"product_name_en": "Product Name",
                #"product_name": "Product Name",
                #"product_declared_value": 5.0,
                "quantity": 1,
                #"ref_tnx": "1495099983020",
                #"ref_item_id": "302588235574",
                #"ref_buyer_id": "esramo_a62szxok",
                #"already_taxed": "I_TAXED",
                #"child_order_id": "child_order_id",
                #"batch_info": [
                #    {
                #        "inventory_code": "RVJRY-220308-0008_EA140509201610_444_20220308151424",
                #        "sku_quantity": "1"
                #    }
                #],
                #"batch_number": ["abc", "def", "ghi"]
            }
        ],
        #"report": [
        #    {
        #        "product_sku": "EA140509201610",
        #        "product_title": "键盘",
        #        "product_title_en": "keyboard",
        #        "product_quantity": 1,
        #        "product_declared_value": 5,
        #        "product_weight": 3
        #    }
        #],
        "tracking_no": "12345678900000",
        #"label": {
        #    "file_type": "png",
        #    "file_data": "hVJPjUP4+yHjvKErt5PuFfvRhd...",
        #    "file_size": "100x100",
        #    "file_name": "name"
        #},
        #"attach": [
        #    {"file_type": "zip", "attach_id": "4276"},
        #    {"file_type": "pdf", "attach_id": "4277"}
        #],
        #"other_documents": [
        #    {"attach_id": "133"}
        #],
        #"is_pack_box": 0,
        #"is_release_cargo": 0,
        #"is_vip": 0,
        #"order_kind": "BC",
        #"order_payer_name": "小明",
        #"order_id_number": "1325323112323",
        #"order_payer_phone": "13120775656",
        #"order_country_code_origin": "美国",
        #"order_sale_amount": "0",
        #"order_sale_currency": "RMB",
        #"is_platform_ebay": "0",
        #"ebay_item_id": "130056",
        #"ebay_transaction_id": "202009041652-0001",
        #"tax_payment_method": "CFR_OR_CPT",
        #"customs_company_name": "我是清关公司",
        #"customs_address": "我是清关地址",
        #"customs_contact_name": "我是清关联系人",
        #"customs_email": "15026521351@gmail.com",
        #"customs_tax_code": "156156156",
        #"customs_phone": "15625231521",
        #"customs_city": "武汉市",
        #"customs_state": "湖北省",
        #"customs_country_code": "CN",
        #"customs_postcode": "434400",
        #"customs_doorplate": "601",
        #"consignee_tax_number": "682515426999999",
        #"consignee_eori": "我是收件人EORI号",
        #"order_battery_type": "UN3481",
        #"vat_tax_code": "16515612231",
        #"distribution_information": "distribution information",
        #"consignee_tax_type": 1,
        #"api_source": "mabangerp",
        #"verify": 1,
        #"forceVerify": 0,
        #"lp_code": "AEOWH0000345968",
        #"is_merge": 0,
        #"merge_order_count": 0,
        #"insurance_type": 1,
        #"insurance_type_goods_value": 1,
        #"is_ju_order": 0,
        #"is_allow_open": 1,
        #"is_prime": 0,
        #"transaction_no": "",
        #"async": 0,
        #"premium_service": ""
    }

def build_soap_envelope(params: dict, app_token: str, app_key: str, service: str) -> str:
    """把 JSON 參數放進 SOAP Envelope。"""
    params_json = json.dumps(params, ensure_ascii=False, separators=(",", ":"))
    envelope = f'''<?xml version="1.0" encoding="UTF-8"?>
<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ns1="http://www.example.org/Ec/">
  <SOAP-ENV:Body>
    <ns1:callService>
      <paramsJson>
{params_json}
      </paramsJson>
      <appToken>{app_token}</appToken>
      <appKey>{app_key}</appKey>
      <service>{service}</service>
    </ns1:callService>
  </SOAP-ENV:Body>
</SOAP-ENV:Envelope>'''
    return envelope

def requests_session_with_retry() -> requests.Session:
    """建立帶重試機制的 requests Session。"""
    retry = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("POST", "GET"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s = requests.Session()
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

def call_soap(endpoint: str, envelope_xml: str) -> requests.Response:
    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": SERVICE,  # 有些服務需要，若報錯可移除或改為實際值
    }
    session = requests_session_with_retry()
    resp = session.post(endpoint, data=envelope_xml.encode("utf-8"), headers=headers, timeout=30)
    return resp

def try_parse_fault(xml_text: str):
    """嘗試解析 SOAP Fault，若無 Fault 則回傳 None。"""
    try:
        ns = {
            "soap": "http://schemas.xmlsoap.org/soap/envelope/"
        }
        root = ET.fromstring(xml_text)
        fault = root.find(".//soap:Fault", ns)
        if fault is not None:
            faultcode = fault.findtext("faultcode") or ""
            faultstring = fault.findtext("faultstring") or ""
            return {"faultcode": faultcode, "faultstring": faultstring}
        return None
    except ET.ParseError:
        return None
# ===== Added: lightweight exports to be imported by app-ok.py =====
def send_create_order(endpoint: str, app_token: str, app_key: str, params: dict, service: str = SERVICE) -> requests.Response:
    """
    Compose SOAP envelope from params and POST to the given endpoint.
    Returns the raw requests.Response (caller can inspect .status_code and .text).
    """
    envelope = build_soap_envelope(params, app_token, app_key, service)
    return call_soap(endpoint, envelope)
# ===== End Added =====


def main():
    params = build_params_dict()
    envelope = build_soap_envelope(params, APP_TOKEN, APP_KEY, SERVICE)

    print("====== SOAP Request (truncated to 2,000 chars for display) ======")
    truncated = envelope if len(envelope) <= 2000 else envelope[:2000] + "...(truncated)"
    print(truncated)

    resp = call_soap(ENDPOINT_URL, envelope)
    print("\n====== HTTP Response ======")
    print("Status code:", resp.status_code)
    print("Headers:", dict(resp.headers))
    print("\nResponse text (first 10,000 chars):")
    text = resp.text
    print(text[:10000])

    fault = try_parse_fault(text)
    if fault:
        print("\n====== SOAP Fault Detected ======")
        print(f"faultcode: {fault['faultcode']}")
        print(f"faultstring: {fault['faultstring']}")
    else:
        print("\nNo SOAP Fault element detected. (If service有自定義的回傳節點，請依實際結構解析)")

if __name__ == "__main__":
    main()
