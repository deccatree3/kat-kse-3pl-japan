"""
재고 소진 임박 SKU Slack 알림 스케줄러.

실행: python alerts/notifier.py
스케줄링: Windows 작업 스케줄러 (매일 1회 권장)
"""
import os
import sys
import io
import json
import datetime
import urllib.request
from collections import defaultdict

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
STATE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "state.json")

# db/pg.py 모듈 import
sys.path.insert(0, os.path.join(BASE_DIR, "db"))
import pg as _pg


def load_config():
    """로컬 설정 (env 변수가 있으면 덮어쓰기)"""
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    else:
        cfg = {"enabled": False, "webhook_url": "", "threshold_days": 30}

    # 환경변수 오버라이드 (GitHub Actions용)
    if os.environ.get("SLACK_WEBHOOK_URL"):
        cfg["webhook_url"] = os.environ["SLACK_WEBHOOK_URL"]
    if os.environ.get("SLACK_ENABLED") in ("1", "true", "True"):
        cfg["enabled"] = True
    if os.environ.get("SLACK_THRESHOLD_DAYS"):
        cfg["threshold_days"] = int(os.environ["SLACK_THRESHOLD_DAYS"])

    return cfg


def load_state():
    if not os.path.exists(STATE_PATH):
        return {"last_alerted_skus": []}
    with open(STATE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_state(state):
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def compute_forecast():
    """DB에서 재고/출고 읽어 SKU별 잔여일수 계산"""
    conn = _pg.connect(autocommit=True)
    cur = conn.cursor()

    cur.execute("SELECT value FROM stock_load_meta WHERE key='latest_snapshot'")
    row = cur.fetchone()
    if not row:
        conn.close()
        return []
    snap_date = row[0]

    cur.execute("""
        SELECT sku_code, sku_name, total_qty, available_qty
        FROM stock_snapshots WHERE snapshot_date = %s
    """, (snap_date,))
    stock = [
        {'code': r[0], 'name': r[1], 'total': r[2], 'available': r[3]}
        for r in cur.fetchall()
    ]
    if not stock:
        conn.close()
        return []

    cur.execute("SELECT MIN(ship_date), MAX(ship_date) FROM shipments WHERE ship_date IS NOT NULL")
    date_min, date_max = cur.fetchone()
    if not date_min or not date_max:
        conn.close()
        return []

    cur.execute("SELECT sku_code, SUM(qty) FROM shipments GROUP BY sku_code")
    shipments = {code: qty for code, qty in cur.fetchall()}
    conn.close()

    dmin = datetime.datetime.strptime(date_min, '%Y%m%d').date()
    dmax = datetime.datetime.strptime(date_max, '%Y%m%d').date()
    period_days = (dmax - dmin).days + 1
    today = datetime.date.today()

    result = []
    for item in stock:
        shipped = shipments.get(item['code'], 0)
        daily_avg = shipped / period_days if period_days > 0 else 0
        if item['available'] == 0:
            days = 0
            dep_date = None
        elif daily_avg == 0:
            days = None
            dep_date = None
        else:
            days = item['available'] / daily_avg
            dep_date = today + datetime.timedelta(days=int(days))
        result.append({
            **item,
            'shipped': shipped,
            'daily_avg': round(daily_avg, 2),
            'remaining_days': days,
            'depletion_date': dep_date.strftime('%Y-%m-%d') if dep_date else None,
        })
    return result


def build_message(skus, threshold):
    today = datetime.date.today().strftime('%Y-%m-%d')

    def bucket(r):
        if r['remaining_days'] is None or r['remaining_days'] > threshold:
            return None
        if r['available'] == 0:
            return '⚫ 품절'
        if r['remaining_days'] <= 14:
            return '🔴 긴급'
        if r['remaining_days'] <= 30:
            return '🟠 주의'
        return '🟡 관찰'

    grouped = defaultdict(list)
    for r in skus:
        b = bucket(r)
        if b:
            grouped[b].append(r)

    if not grouped:
        return None

    lines = [f"*⚠️ 재고 소진 임박 SKU* _{today}_"]
    order = ['⚫ 품절', '🔴 긴급', '🟠 주의', '🟡 관찰']
    for b in order:
        if b not in grouped:
            continue
        lines.append(f"\n*{b}*")
        for r in sorted(grouped[b], key=lambda x: x['remaining_days'] or 0):
            name = r['name'] or r['code']
            if r['available'] == 0:
                lines.append(f"  • {name} — 품절")
            else:
                days = int(r['remaining_days'])
                lines.append(f"  • {name} — {days}일 남음 (가용 {r['available']}, 소진 {r['depletion_date']})")
    return "\n".join(lines)


def send_slack(webhook_url, text):
    payload = json.dumps({"text": text}).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return resp.status == 200


def main():
    cfg = load_config()
    state = load_state()

    if not cfg.get("enabled"):
        print("[SKIP] 알림 비활성화")
        return

    webhook = cfg.get("webhook_url", "").strip()
    if not webhook:
        print("[SKIP] Webhook URL 미설정")
        return

    threshold = int(cfg.get("threshold_days", 30))

    forecast = compute_forecast()
    if not forecast:
        print("[SKIP] 예측 데이터 없음")
        return

    current_alert = [
        r for r in forecast
        if r['remaining_days'] is not None and r['remaining_days'] <= threshold
    ]
    current_skus = set(r['code'] for r in current_alert)
    last_skus = set(state.get("last_alerted_skus", []))
    new_skus = current_skus - last_skus

    if not new_skus:
        print(f"[OK] 임계 진입 SKU 없음 (현재 ≤{threshold}일: {len(current_skus)}개)")
    else:
        new_items = [r for r in current_alert if r['code'] in new_skus]
        msg = build_message(new_items, threshold)
        if msg:
            try:
                send_slack(webhook, msg)
                print(f"[SENT] {len(new_skus)}개 SKU 신규 알림")
            except Exception as e:
                print(f"[ERROR] Slack 전송 실패: {e}")
                return

    # 상태 저장
    state["last_alerted_skus"] = sorted(current_skus)
    save_state(state)


if __name__ == "__main__":
    main()
