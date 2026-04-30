"""
Qoo10 QAPI 클라이언트 (정식 모듈).

검증된 호출 흐름:
  - 인증: CertificationAPI.CreateCertificationKey → SAK 발급
  - 신규 주문: ShippingBasic.GetShippingInfo_v2 (ShippingStat=2 = 배송요청)
  - 송장 등록: ShippingBasic.SetSendingInfo (OrderNo + ShippingCorp + TrackingNo)

자격증명: config.json (qoo10_api_key / qoo10_user_id / qoo10_password)
또는 환경변수 QOO10_API_KEY / QOO10_USER_ID / QOO10_PASSWORD.
"""
import os
import sys
import json
import datetime
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Tuple

import requests

# DB 헬퍼 (지연 import — DB 미사용 환경에서도 import 자체는 동작하도록)
_DB_AVAILABLE = None


def _try_import_pg():
    global _DB_AVAILABLE
    if _DB_AVAILABLE is not None:
        return _DB_AVAILABLE
    try:
        _here = os.path.dirname(os.path.abspath(__file__))
        _base = os.path.dirname(_here)
        if os.path.join(_base, 'db') not in sys.path:
            sys.path.insert(0, os.path.join(_base, 'db'))
        import pg as _pg  # type: ignore
        _DB_AVAILABLE = _pg
    except Exception:
        _DB_AVAILABLE = False
    return _DB_AVAILABLE

BASE_URL = "https://api.qoo10.jp/GMKT.INC.Front.QAPIService/ebayjapan.qapi"
CERT_URL = f"{BASE_URL}/CertificationAPI.CreateCertificationKey"

# ShippingStat 값 매핑 (2026-04-30 검증)
SHIPPING_STAT_REQUEST = "2"   # On request — 배송요청 (KSE 출고요청 전 신규 주문)
SHIPPING_STAT_DELIVERY = "4"  # On delivery — 배송중 (송장 등록 후 자동 전이)

# 배송회사 코드 (Qoo10 측 인식)
SHIPPING_CORP_SAGAWA = "Sagawa"


def _config_path() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(os.path.dirname(here), 'config.json')


CREDS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS qoo10_credentials (
    id INTEGER PRIMARY KEY,
    api_key TEXT,
    user_id TEXT,
    password TEXT,
    expires_at DATE,
    updated_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul')
);
"""


def _ensure_creds_table():
    pg = _try_import_pg()
    if not pg:
        return False
    try:
        conn = pg.connect()
        with conn.cursor() as cur:
            cur.execute(CREDS_TABLE_DDL)
        conn.commit()
        conn.close()
        return True
    except Exception:
        return False


def load_credentials_from_db() -> Dict:
    """DB에서 단일 자격증명 행 로드. 없으면 빈 dict 반환."""
    pg = _try_import_pg()
    if not pg:
        return {}
    try:
        _ensure_creds_table()
        conn = pg.connect(autocommit=True)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT api_key, user_id, password, expires_at, updated_at
                FROM qoo10_credentials WHERE id = 1
            """)
            row = cur.fetchone()
        conn.close()
        if not row:
            return {}
        return {
            'api_key': row[0], 'user_id': row[1], 'password': row[2],
            'expires_at': row[3], 'updated_at': row[4],
        }
    except Exception:
        return {}


def save_credentials_to_db(api_key: Optional[str] = None,
                           user_id: Optional[str] = None,
                           password: Optional[str] = None,
                           expires_at: Optional[datetime.date] = None) -> bool:
    """DB에 자격증명 upsert. 빈 값(None/'')은 기존 값 유지.
    True/False 반환.
    """
    pg = _try_import_pg()
    if not pg:
        return False
    _ensure_creds_table()
    existing = load_credentials_from_db()
    new_api = api_key if api_key else existing.get('api_key')
    new_uid = user_id if user_id else existing.get('user_id')
    new_pw = password if password else existing.get('password')
    new_exp = expires_at if expires_at is not None else existing.get('expires_at')
    try:
        conn = pg.connect()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO qoo10_credentials (id, api_key, user_id, password, expires_at, updated_at)
                VALUES (1, %s, %s, %s, %s, (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul'))
                ON CONFLICT (id) DO UPDATE SET
                    api_key = EXCLUDED.api_key,
                    user_id = EXCLUDED.user_id,
                    password = EXCLUDED.password,
                    expires_at = EXCLUDED.expires_at,
                    updated_at = (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul')
            """, (new_api, new_uid, new_pw, new_exp))
        conn.commit()
        conn.close()
        return True
    except Exception:
        return False


def get_credentials_status() -> Dict:
    """UI 표시용 상태 dict.
    반환: {'configured': bool, 'expires_at': date|None, 'updated_at': str|None,
           'days_remaining': int|None, 'level': 'green'/'yellow'/'red'/'expired'/None}
    """
    db = load_credentials_from_db()
    creds = load_credentials()
    configured = all([creds.get('api_key'), creds.get('user_id'), creds.get('password')])
    exp = db.get('expires_at')
    days = None
    level = None
    if exp:
        # exp가 datetime.date 또는 str일 수 있음
        if isinstance(exp, str):
            try:
                exp = datetime.datetime.strptime(exp[:10], '%Y-%m-%d').date()
            except ValueError:
                exp = None
        if exp:
            days = (exp - datetime.date.today()).days
            if days < 0:
                level = 'expired'
            elif days <= 30:
                level = 'red'
            elif days <= 60:
                level = 'yellow'
            else:
                level = 'green'
    return {
        'configured': configured,
        'expires_at': exp,
        'updated_at': db.get('updated_at'),
        'days_remaining': days,
        'level': level,
    }


def load_credentials() -> Dict[str, str]:
    """자격증명 로드 우선순위: 환경변수 > Streamlit secrets > DB > config.json.
    - 운영 배포: 환경변수 또는 Streamlit secrets에 직접 주입
    - 사용자 입력: DB (사이드바 UI에서 저장)
    - 로컬 개발: config.json
    """
    creds = {'api_key': None, 'user_id': None, 'password': None}

    # 1) config.json (로컬 개발 환경)
    cfg = _config_path()
    if os.path.exists(cfg):
        try:
            with open(cfg, 'r', encoding='utf-8') as f:
                data = json.load(f)
            creds['api_key'] = data.get('qoo10_api_key')
            creds['user_id'] = data.get('qoo10_user_id')
            creds['password'] = data.get('qoo10_password')
        except (OSError, json.JSONDecodeError):
            pass

    # 2) DB (사용자가 사이드바 UI에서 저장한 값)
    db_creds = load_credentials_from_db()
    if db_creds.get('api_key'):
        creds['api_key'] = db_creds['api_key']
    if db_creds.get('user_id'):
        creds['user_id'] = db_creds['user_id']
    if db_creds.get('password'):
        creds['password'] = db_creds['password']

    # 3) Streamlit secrets (Cloud 환경 또는 .streamlit/secrets.toml)
    try:
        import streamlit as _st  # type: ignore
        try:
            sec = _st.secrets
            for key, dest in (('QOO10_API_KEY', 'api_key'),
                              ('QOO10_USER_ID', 'user_id'),
                              ('QOO10_PASSWORD', 'password')):
                v = None
                try:
                    v = sec.get(key) if hasattr(sec, 'get') else sec[key]
                except (KeyError, FileNotFoundError, Exception):
                    v = None
                if v:
                    creds[dest] = v
        except (FileNotFoundError, Exception):
            pass
    except ImportError:
        pass

    # 4) 환경변수 (최우선 — 명시적 override 의도로 간주)
    creds['api_key'] = os.environ.get('QOO10_API_KEY') or creds['api_key']
    creds['user_id'] = os.environ.get('QOO10_USER_ID') or creds['user_id']
    creds['password'] = os.environ.get('QOO10_PASSWORD') or creds['password']
    return creds


def has_credentials() -> bool:
    c = load_credentials()
    return all((c['api_key'], c['user_id'], c['password']))


def get_sak(api_key: Optional[str] = None,
            user_id: Optional[str] = None,
            password: Optional[str] = None) -> str:
    """인증키(SAK) 발급. 인자 미지정 시 config.json/환경변수에서 로드.
    실패 시 RuntimeError. 성공 시 SAK 문자열 반환.
    """
    if api_key is None or user_id is None or password is None:
        c = load_credentials()
        api_key = api_key or c['api_key']
        user_id = user_id or c['user_id']
        password = password or c['password']
    if not all((api_key, user_id, password)):
        raise RuntimeError("Qoo10 API 자격증명이 없습니다 (api_key / user_id / password)")

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "GiosisCertificationKey": api_key,
        "QAPIVersion": "1.0",
    }
    params = {"returnType": "text/xml", "user_id": user_id, "pwd": password}
    r = requests.post(CERT_URL, headers=headers, data=params, timeout=30)
    try:
        root = ET.fromstring(r.text)
    except ET.ParseError as e:
        raise RuntimeError(f"SAK 응답 파싱 실패: {e}; body={r.text[:300]}")
    rcode = root.findtext('.//ResultCode')
    rmsg = root.findtext('.//ResultMsg') or ''
    if rcode != '0':
        raise RuntimeError(f"SAK 발급 실패 (ResultCode={rcode}, {rmsg})")
    sak = (root.findtext('.//ResultObject') or '').strip()
    if not sak:
        raise RuntimeError(f"SAK 응답에 ResultObject 없음: {r.text[:300]}")
    return sak


def fetch_orders(sak: str,
                 start_date: str,
                 end_date: str,
                 shipping_stat: str = SHIPPING_STAT_REQUEST) -> List[Dict]:
    """주문 조회. start_date/end_date는 YYYYMMDD.
    응답 ResultObject 리스트 그대로 반환 (orderNo, packNo, itemTitle 등 포함).
    """
    params = {
        "v": "1.0",
        "returnType": "json",
        "ShippingStat": shipping_stat,
        "method": "ShippingBasic.GetShippingInfo_v2",
        "key": sak,
        "search_Sdate": start_date,
        "search_Edate": end_date,
    }
    r = requests.get(BASE_URL, params=params, timeout=60)
    r.raise_for_status()
    try:
        data = json.loads(r.text)
    except json.JSONDecodeError:
        return []
    return data.get('ResultObject') or []


def fetch_order_by_no(sak: str, order_no: str,
                      start_date: str, end_date: str) -> Tuple[Optional[str], Optional[Dict]]:
    """모든 ShippingStat에서 특정 OrderNo를 검색. (stat, item) 반환.
    상태 검증용 (등록 전후 비교).
    """
    for stat in ("1", "2", "3", "4", "5"):
        for item in fetch_orders(sak, start_date, end_date, stat):
            if str(item.get('orderNo')) == str(order_no):
                return stat, item
    return None, None


def register_waybill(sak: str,
                     order_no: str,
                     tracking_no: str,
                     shipping_corp: str = SHIPPING_CORP_SAGAWA) -> Dict:
    """단일 주문 송장번호 등록.
    성공 시: {'ok': True, 'order_no': ..., 'tracking_no': ..., 'msg': 'SUCCESS'}
    실패 시: {'ok': False, 'order_no': ..., 'tracking_no': ..., 'code': ..., 'msg': ...}
    """
    params = {
        "v": "1.0",
        "returnType": "json",
        "method": "ShippingBasic.SetSendingInfo",
        "key": sak,
        "OrderNo": str(order_no),
        "ShippingCorp": shipping_corp,
        "TrackingNo": str(tracking_no),
    }
    r = requests.get(BASE_URL, params=params, timeout=30)
    body = r.text
    try:
        data = json.loads(body)
        rcode = data.get('ResultCode')
        rmsg = data.get('ResultMsg', '')
    except json.JSONDecodeError:
        return {
            'ok': False, 'order_no': order_no, 'tracking_no': tracking_no,
            'code': r.status_code, 'msg': body[:200],
        }
    return {
        'ok': rcode == 0 or rcode == '0',
        'order_no': order_no,
        'tracking_no': tracking_no,
        'code': rcode,
        'msg': rmsg,
    }


def register_waybills_batch(sak: str,
                            mappings: List[Tuple[str, str]],
                            shipping_corp: str = SHIPPING_CORP_SAGAWA) -> List[Dict]:
    """여러 (order_no, tracking_no) 일괄 등록.
    각 호출 결과 dict 리스트 반환.
    """
    results = []
    for order_no, tracking_no in mappings:
        results.append(register_waybill(sak, order_no, tracking_no, shipping_corp))
    return results


def api_response_to_qsm_dict(api_order: Dict) -> Dict:
    """
    API 응답 1행 → QSM detail.csv DictReader 호환 키 형태.
    generator.generate_outbound_rows 가 기대하는 키만 채움.

    주의:
      - 주소: shippingAddr (Addr1+Addr2 합본, API 응답에 공백 1칸 포함될 수 있음)
              → clean_special_chars가 다운스트림에서 적용되므로 그대로 전달.
      - 전화: receiverMobile / receiverTel (둘 다 +81 prefix 포함됨)
              → 사용자 검증 결과 KSE OMS 통과 확인됨, 변환 없이 그대로 전달.
    """
    return {
        '배송상태': api_order.get('shippingStatus', ''),
        '주문번호': str(api_order.get('orderNo', '')),
        '장바구니번호': str(api_order.get('packNo', '')),
        '상품명': api_order.get('itemTitle', '') or '',
        '옵션정보': api_order.get('option', '') or '',
        '수량': str(api_order.get('orderQty', 1) or 1),
        '수취인명': api_order.get('receiver', '') or '',
        '수취인전화번호': api_order.get('receiverTel', '') or '',
        '수취인핸드폰번호': api_order.get('receiverMobile', '') or '',
        '주소': api_order.get('shippingAddr', '') or '',
        '우편번호': api_order.get('zipCode', '') or '',
        '주문일': api_order.get('orderDate', '') or '',
        '판매자상품코드': api_order.get('sellerItemCode', '') or '',
        '상품코드': str(api_order.get('itemCode', '') or ''),
        '택배사': api_order.get('DeliveryCompany', '') or '',
    }


_DETAIL_HEADERS = [
    '배송상태', '주문번호', '장바구니번호', '택배사', '송장번호',
    '발송일', '주문일', '입금일', '배달희망일', '발송예정일',
    '배송완료일', '배송방식', '상품코드', '상품명', '수량',
    '옵션정보', '판매자옵션코드', '사은품', '수취인명', '수취인명(음성표기)',
    '수취인전화번호', '수취인핸드폰번호', '주소', '우편번호', '국가',
    '배송비결제', '주문국가', '통화', '구매자결제금', '판매가',
    '할인액 ', '총주문액', '총공급원가', '구매자명', '구매자명(발음표기)',
    '배송요청사항', '구매자전화번호', '구매자핸드폰번호', '판매자상품코드', 'JAN코드',
    '규격번호', '(선물)보내는사람', '수화물 보관함 서비스', '외부광고', '소재',
    '선물하기주문',
]
_BRIEF_HEADERS = [
    '배송상태', '주문번호', '장바구니번호', '택배사', '송장번호',
    '발송일', '발송예정일', '상품명', '수량', '옵션정보',
    '판매자옵션코드', '수취인명', '판매자상품코드', '외부광고',
    '주문국가', '선물하기주문',
]

# CSV 헤더 → API 응답 키 매핑 (없으면 빈 문자열)
_HEADER_TO_API = {
    '배송상태': lambda o: '배송요청',
    '주문번호': lambda o: o.get('orderNo'),
    '장바구니번호': lambda o: o.get('packNo'),
    '택배사': lambda o: o.get('DeliveryCompany'),
    '송장번호': lambda o: o.get('TrackingNo') or '',
    '발송일': lambda o: o.get('ShippingDate') or '',
    '주문일': lambda o: o.get('orderDate'),
    '입금일': lambda o: o.get('PaymentDate'),
    '배달희망일': lambda o: o.get('hopeDate') or '',
    '발송예정일': lambda o: o.get('EstShippingDate') or '',
    '배송완료일': lambda o: o.get('DeliveredDate') or '',
    '배송방식': lambda o: 'API取込',
    '상품코드': lambda o: o.get('itemCode'),
    '상품명': lambda o: o.get('itemTitle'),
    '수량': lambda o: o.get('orderQty'),
    '옵션정보': lambda o: o.get('option') or '',
    '판매자옵션코드': lambda o: o.get('optionCode') or '',
    '수취인명': lambda o: o.get('receiver'),
    '수취인명(음성표기)': lambda o: o.get('receiver_gata') or '',
    '수취인전화번호': lambda o: o.get('receiverTel') or '',
    '수취인핸드폰번호': lambda o: o.get('receiverMobile') or '',
    '주소': lambda o: o.get('shippingAddr') or '',
    '우편번호': lambda o: o.get('zipCode') or '',
    '국가': lambda o: o.get('shippingCountry') or 'JP',
    '배송비결제': lambda o: o.get('shippingRateType') or '',
    '주문국가': lambda o: o.get('PaymentNation') or 'JP',
    '통화': lambda o: o.get('Currency') or 'JPY',
    '구매자결제금': lambda o: o.get('SettlePrice') or '',
    '판매가': lambda o: o.get('orderPrice') or '',
    '총주문액': lambda o: o.get('total') or '',
    '구매자명': lambda o: o.get('buyer') or '',
    '구매자명(발음표기)': lambda o: o.get('buyer_gata') or '',
    '배송요청사항': lambda o: o.get('ShippingMsg') or '',
    '구매자전화번호': lambda o: o.get('buyerTel') or '',
    '구매자핸드폰번호': lambda o: o.get('buyerMobile') or '',
    '판매자상품코드': lambda o: o.get('sellerItemCode') or '',
    '(선물)보내는사람': lambda o: o.get('senderName') or '',
    '선물하기주문': lambda o: o.get('Gift') or 'N',
}


def _build_csv_bytes(api_orders: List[Dict], headers: List[str]) -> bytes:
    """API 응답 → UTF-8 BOM CSV bytes (모든 필드 따옴표)."""
    import csv as _csv
    import io as _io
    out = _io.StringIO()
    w = _csv.writer(out, quoting=_csv.QUOTE_ALL, lineterminator='\r\n')
    w.writerow(headers)
    for o in api_orders:
        row = []
        for h in headers:
            fn = _HEADER_TO_API.get(h)
            v = fn(o) if fn else ''
            row.append('' if v is None else str(v))
        w.writerow(row)
    return b'\xef\xbb\xbf' + out.getvalue().encode('utf-8')


def build_detail_csv_bytes(api_orders: List[Dict]) -> bytes:
    """API 응답 → DeliveryManagement_detail*.csv 호환 bytes."""
    return _build_csv_bytes(api_orders, _DETAIL_HEADERS)


def build_brief_csv_bytes(api_orders: List[Dict]) -> bytes:
    """API 응답 → DeliveryManagement_brief*.csv 호환 bytes."""
    return _build_csv_bytes(api_orders, _BRIEF_HEADERS)


def fetch_orders_as_qsm_dicts(sak: Optional[str] = None,
                              start_date: Optional[str] = None,
                              end_date: Optional[str] = None,
                              shipping_stat: str = SHIPPING_STAT_REQUEST,
                              days: int = 3) -> Tuple[List[Dict], List[Dict]]:
    """
    편의 함수: SAK 발급 → 주문 조회 → QSM 호환 dict 리스트로 변환.
    반환: (qsm_compatible_dicts, raw_api_objects)
    """
    if sak is None:
        sak = get_sak()
    today = datetime.date.today()
    if end_date is None:
        end_date = today.strftime('%Y%m%d')
    if start_date is None:
        start_date = (today - datetime.timedelta(days=days)).strftime('%Y%m%d')
    raw = fetch_orders(sak, start_date, end_date, shipping_stat)
    return [api_response_to_qsm_dict(o) for o in raw], raw
