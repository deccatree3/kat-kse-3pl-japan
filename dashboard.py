"""
KAT-KSE 3PL Japan 물류비 대시보드
실행: streamlit run dashboard.py
"""
import os
import glob
import json
import datetime
import urllib.request
from collections import defaultdict
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import openpyxl

from db import pg

APP_CFG_PATH = os.path.join(os.path.dirname(__file__), "config.json")


def load_app_config():
    default = {"raw_dir": os.path.join(os.path.dirname(__file__), "raw")}
    if not os.path.exists(APP_CFG_PATH):
        return default
    try:
        with open(APP_CFG_PATH, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        return {**default, **cfg}
    except Exception:
        return default


def save_app_config(cfg):
    with open(APP_CFG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)


RAW_DIR = load_app_config()["raw_dir"]


def load_alert_config():
    """DB에서 alert_config 로드"""
    try:
        df = pg.query_df("SELECT enabled, webhook_url, threshold_days FROM alert_config WHERE id=1")
        if df.empty:
            return {"enabled": False, "webhook_url": "", "threshold_days": 30}
        r = df.iloc[0]
        return {
            "enabled": bool(r["enabled"]),
            "webhook_url": r["webhook_url"] or "",
            "threshold_days": int(r["threshold_days"]),
        }
    except Exception:
        return {"enabled": False, "webhook_url": "", "threshold_days": 30}


def save_alert_config(cfg):
    """DB에 alert_config 저장"""
    conn = pg.connect()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO alert_config (id, enabled, webhook_url, threshold_days, updated_at)
            VALUES (1, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (id) DO UPDATE SET
                enabled = EXCLUDED.enabled,
                webhook_url = EXCLUDED.webhook_url,
                threshold_days = EXCLUDED.threshold_days,
                updated_at = CURRENT_TIMESTAMP
        """, (
            bool(cfg.get("enabled", False)),
            (cfg.get("webhook_url") or "").strip(),
            int(cfg.get("threshold_days", 30)),
        ))
    conn.commit()
    conn.close()


def send_slack_test(webhook_url):
    payload = json.dumps({
        "text": f"✅ 3PL 재고 알림 테스트 메시지 ({datetime.datetime.now().strftime('%Y-%m-%d %H:%M')})"
    }).encode("utf-8")
    req = urllib.request.Request(
        webhook_url, data=payload,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return resp.status == 200


@st.cache_resource
def get_conn():
    return pg.connect(autocommit=True)


def load_data(query, params=None):
    # SQLite의 ? 파라미터를 Postgres %s로 자동 변환
    query = query.replace("?", "%s")
    conn = get_conn()
    try:
        return pg.query_df(query, params or (), conn=conn)
    except Exception:
        # 연결 끊김 시 재연결
        get_conn.clear()
        conn = get_conn()
        return pg.query_df(query, params or (), conn=conn)


def fmt_month(ym):
    return f"{ym[:4]}년 {int(ym[4:]):02d}월"


@st.cache_data(ttl=60)
def compute_stock_forecast():
    """DB에서 재고/출고 데이터 읽어 SKU별 잔여일수 계산"""
    try:
        latest = pg.query_df("SELECT value FROM stock_load_meta WHERE key='latest_snapshot'")
        snap_date = latest.iloc[0, 0] if not latest.empty else None
    except Exception:
        snap_date = None

    if not snap_date:
        return None, None, None, None

    stock_df = pg.query_df("""
        SELECT sku_code AS "상품코드", sku_name AS "상품명",
               total_qty AS "총재고", available_qty AS "가용재고"
        FROM stock_snapshots WHERE snapshot_date = %s
    """, [snap_date])

    if stock_df.empty:
        return None, None, None, None

    ship_range = pg.query_df("""
        SELECT MIN(ship_date) AS dmin, MAX(ship_date) AS dmax
        FROM shipments WHERE ship_date IS NOT NULL
    """)

    dmin_s = ship_range.iloc[0]['dmin']
    dmax_s = ship_range.iloc[0]['dmax']

    if not dmin_s or not dmax_s:
        stock_df['총출고'] = 0
        stock_df['일평균'] = 0.0
        stock_df['잔여일수'] = float('inf')
        stock_df['소진예상일'] = '판단불가'
        stock_df['상태'] = '⚪ 판단불가'
        return stock_df, snap_date, None, None

    ship_agg = pg.query_df("""
        SELECT sku_code, SUM(qty) AS total
        FROM shipments GROUP BY sku_code
    """)
    shipments = dict(zip(ship_agg['sku_code'], ship_agg['total']))

    dmin = datetime.datetime.strptime(dmin_s, '%Y%m%d').date()
    dmax = datetime.datetime.strptime(dmax_s, '%Y%m%d').date()
    period_days = (dmax - dmin).days + 1
    today = datetime.date.today()

    stock_df['총출고'] = stock_df['상품코드'].map(lambda c: shipments.get(c, 0))
    stock_df['일평균'] = stock_df['총출고'] / period_days

    def compute_row(r):
        if r['가용재고'] == 0:
            return (0, '품절', '⚫ 품절')
        if r['일평균'] == 0:
            return (float('inf'), '출고이력 없음', '⚪ 출고없음')
        days = r['가용재고'] / r['일평균']
        dep_date = today + datetime.timedelta(days=int(days))
        if days <= 14:
            status = '🔴 긴급'
        elif days <= 30:
            status = '🟠 주의'
        elif days <= 60:
            status = '🟡 관찰'
        else:
            status = '🟢 안전'
        return (round(days, 1), dep_date.strftime('%Y-%m-%d'), status)

    vals = stock_df.apply(compute_row, axis=1)
    stock_df['잔여일수'] = [v[0] for v in vals]
    stock_df['소진예상일'] = [v[1] for v in vals]
    stock_df['상태'] = [v[2] for v in vals]

    return stock_df, snap_date, (dmin, dmax, period_days), today


def get_stock_load_meta():
    try:
        df = pg.query_df("SELECT key, value FROM stock_load_meta")
        return dict(zip(df['key'], df['value']))
    except Exception:
        return {}


@st.cache_data(ttl=60)
def compute_kat_side(year_month: str) -> dict:
    """shipments 테이블에서 KATCHERS 측 대조 값을 자동 계산 (월별 필터)"""
    pattern = f"{year_month}%"
    try:
        b2c = pg.query_df("""
            SELECT COUNT(DISTINCT waybill) AS n, COALESCE(SUM(qty), 0) AS pcs
            FROM shipments WHERE ship_type='B2C' AND ship_date LIKE %s
        """, [pattern]).iloc[0]

        b2b = pg.query_df("""
            SELECT COUNT(DISTINCT waybill) AS n
            FROM shipments WHERE ship_type='B2B' AND ship_date LIKE %s
        """, [pattern]).iloc[0]
    except Exception:
        return {}

    b2c_count = int(b2c['n'])
    b2c_pcs = int(b2c['pcs'])
    # B2B는 FBA 특성상 ORDER_LIST 행 수와 청구 건수가 다르므로 자동화 제외
    return {
        'picking_pcs': b2c_pcs,
        'b2c_handling': b2c_count,
        'cushion': b2c_count,
    }


# ─── Page Config ───
st.set_page_config(
    page_title="3PL 물류비 대시보드",
    page_icon="📦",
    layout="wide",
)

st.title("📦 일본 3PL 물류비 대시보드")
st.caption("KATCHERS × 国際エキスプレス (KOKUSAI EXPRESS)")

# ─── Sidebar: 메뉴 선택 ───
menu = st.sidebar.radio(
    "메뉴",
    ["📋 물류비 검토", "📦 재고 소진 예측", "📤 출고요청서 (Qoo10)"],
    label_visibility="collapsed",
)
st.sidebar.markdown("---")

# ─── Sidebar: 파일 경로 설정 ───
with st.sidebar.expander("⚙️ 파일 경로 설정", expanded=False):
    st.caption(f"📁 저장 위치: `{RAW_DIR}`")

    st.markdown("**ORDER_LIST / 재고현황 업로드**")
    uploaded = st.file_uploader(
        "업로드",
        type=['xlsx'],
        accept_multiple_files=True,
        label_visibility="collapsed",
        help="ORDER_LIST_B2C, ORDER_LIST_B2B, 재고현황 파일 (누적/최신)"
    )
    if uploaded and st.button("업로드 & DB 갱신", width="stretch"):
        os.makedirs(RAW_DIR, exist_ok=True)
        saved = []
        for f in uploaded:
            dst = os.path.join(RAW_DIR, f.name)
            with open(dst, 'wb') as out:
                out.write(f.getbuffer())
            saved.append(f.name)
        from db.stock_loader import rebuild_all
        with st.spinner("DB 적재 중..."):
            r = rebuild_all()
        st.cache_data.clear()
        st.success(
            f"{len(saved)}개 파일 업로드 완료\n\n" +
            "\n".join(f"• {n}" for n in saved) +
            f"\n\n출고 {r['shipment_rows']}건, 스냅샷 {r['latest_snapshot']}"
        )
        st.rerun()

    st.markdown("---")
    st.markdown("**KSE 월별 파일 업로드**")
    st.caption("詳細.xlsx, 請求書.pdf, 確認書.pdf → `raw/YYYYMM/`")
    col_ym1, col_ym2 = st.columns(2)
    with col_ym1:
        ym_year = st.number_input("년", min_value=2024, max_value=2030, value=datetime.date.today().year, step=1)
    with col_ym2:
        ym_month = st.number_input("월", min_value=1, max_value=12, value=datetime.date.today().month, step=1)
    monthly_ym = f"{int(ym_year):04d}{int(ym_month):02d}"

    uploaded_m = st.file_uploader(
        "월별 업로드", type=['xlsx', 'pdf'], accept_multiple_files=True,
        label_visibility="collapsed", key="monthly_upload"
    )
    if uploaded_m and st.button(f"{monthly_ym} 폴더에 저장", width="stretch"):
        month_dir = os.path.join(RAW_DIR, monthly_ym)
        os.makedirs(month_dir, exist_ok=True)
        saved = []
        for f in uploaded_m:
            dst = os.path.join(month_dir, f.name)
            with open(dst, 'wb') as out:
                out.write(f.getbuffer())
            saved.append(f.name)
        st.success(f"{len(saved)}개 파일을 `{monthly_ym}/` 에 저장:\n\n" +
                   "\n".join(f"• {n}" for n in saved))

st.sidebar.markdown("---")

# ═══════════════════════════════════════════════
# MENU: 재고 소진 예측
# ═══════════════════════════════════════════════
if menu == "📦 재고 소진 예측":
    st.subheader("📦 SKU별 재고 소진 예측")

    # ─── 데이터 상태 / 새로고침 ───
    meta = get_stock_load_meta()
    last_loaded = meta.get('last_loaded_at', '—')

    mc1, mc2 = st.columns([4, 1])
    with mc1:
        st.caption(f"📁 DB 적재 시각: `{last_loaded}` (raw 파일이 갱신되면 새로고침 필요)")
    with mc2:
        if st.button("🔄 새로고침", width="stretch", help="raw 파일을 다시 읽어 DB에 적재"):
            from db.stock_loader import rebuild_all
            with st.spinner("raw 파일 재적재 중..."):
                r = rebuild_all()
            st.cache_data.clear()
            st.success(f"완료: 출고 {r['shipment_rows']}건, 스냅샷 {r['latest_snapshot']}")
            st.rerun()

    # ─── Slack 알림 설정 ───
    with st.expander("🔔 Slack 알림 설정", expanded=False):
        cfg = load_alert_config()

        col_a, col_b = st.columns([1, 3])
        with col_a:
            alert_on = st.toggle("알림 활성화", value=cfg.get("enabled", False))
        with col_b:
            threshold = st.slider(
                "알림 임계값 (일)", min_value=7, max_value=90,
                value=cfg.get("threshold_days", 30), step=1
            )

        webhook = st.text_input(
            "Slack Webhook URL",
            value=cfg.get("webhook_url", ""),
            type="password",
            placeholder="https://hooks.slack.com/services/...",
            help="Slack 워크스페이스에서 Incoming Webhook URL 발급"
        )

        b1, b2, b3 = st.columns([1, 1, 3])
        with b1:
            if st.button("설정 저장", width="stretch"):
                cfg["enabled"] = alert_on
                cfg["webhook_url"] = webhook.strip()
                cfg["threshold_days"] = int(threshold)
                save_alert_config(cfg)
                st.success("저장됨")
        with b2:
            if st.button("테스트 메시지", width="stretch", disabled=not webhook):
                try:
                    if send_slack_test(webhook):
                        st.success("전송 성공")
                    else:
                        st.error("응답 오류")
                except Exception as e:
                    st.error(f"전송 실패: {e}")

        st.caption(
            "GitHub Actions가 매일 KST 09:00에 실행하여 임계값 이하 **신규 진입** SKU를 Slack으로 발송합니다. "
            "위 설정은 저장 즉시 DB에 반영되어 스케줄러가 곧바로 사용합니다."
        )

    st.markdown("---")

    result = compute_stock_forecast()
    if result[0] is None:
        st.warning("재고현황 파일을 찾을 수 없습니다. `raw/재고현황 내역_*.xlsx` 파일을 확인하세요.")
    else:
        df, snap_date, period, today = result

        if period is None:
            st.info("출고 이력이 없어 일평균 계산 불가. ORDER_LIST 파일을 확인하세요.")
        else:
            dmin, dmax, period_days = period
            st.caption(
                f"재고 스냅샷: `{snap_date}` · "
                f"출고 분석 기간: `{dmin} ~ {dmax}` ({period_days}일) · "
                f"기준일: `{today.strftime('%Y-%m-%d')}`"
            )

            status_counts = df['상태'].value_counts().to_dict()
            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("총 SKU", f"{len(df)}")
            c2.metric("🔴 긴급 (≤14일)", f"{status_counts.get('🔴 긴급', 0)}")
            c3.metric("🟠 주의 (≤30일)", f"{status_counts.get('🟠 주의', 0)}")
            c4.metric("🟡 관찰 (≤60일)", f"{status_counts.get('🟡 관찰', 0)}")
            c5.metric("🟢 안전 (>60일)", f"{status_counts.get('🟢 안전', 0)}")

            st.markdown("---")

            df_sorted = df.sort_values('잔여일수', ascending=True).copy()
            df_sorted['일평균'] = df_sorted['일평균'].round(2)
            df_sorted['잔여일수'] = df_sorted['잔여일수'].apply(
                lambda x: '∞' if x == float('inf') else f"{x:.1f}"
            )
            display_df = df_sorted[[
                '상태', '상품코드', '상품명', '총재고', '가용재고',
                '총출고', '일평균', '잔여일수', '소진예상일'
            ]]
            st.dataframe(
                display_df, width="stretch", hide_index=True,
                column_config={
                    '총재고': st.column_config.NumberColumn(format="%d"),
                    '가용재고': st.column_config.NumberColumn(format="%d"),
                    '총출고': st.column_config.NumberColumn(format="%d"),
                }
            )

            st.markdown("---")

            chart_df = df[df['일평균'] > 0].copy()
            if not chart_df.empty:
                chart_df = chart_df.sort_values('잔여일수', ascending=True)
                chart_df['표시'] = chart_df['상품명'].apply(
                    lambda x: x[:30] + '...' if x and len(x) > 30 else x
                )

                def color(s):
                    if s == '🔴 긴급': return '#dc3545'
                    if s == '🟠 주의': return '#fd7e14'
                    if s == '🟡 관찰': return '#ffc107'
                    if s == '🟢 안전': return '#28a745'
                    return '#6c757d'

                chart_df['color'] = chart_df['상태'].apply(color)

                fig = go.Figure(go.Bar(
                    x=chart_df['잔여일수'],
                    y=chart_df['표시'],
                    orientation='h',
                    marker=dict(color=chart_df['color']),
                    text=chart_df['잔여일수'].round(0).astype(int).astype(str) + '일',
                    textposition='outside',
                ))
                fig.update_layout(
                    title='SKU별 재고 잔여일수',
                    xaxis_title='잔여일수',
                    yaxis_title=None,
                    height=max(300, len(chart_df) * 50),
                    showlegend=False,
                )
                st.plotly_chart(fig, width="stretch")

            st.markdown("---")
            st.caption(
                "ℹ️ 계산식: 일평균출고 = 총출고 ÷ 분석기간 일수 · "
                "잔여일수 = 가용재고 ÷ 일평균출고 · "
                "소진예상일 = 기준일 + 잔여일수"
            )

    st.markdown("---")
    st.caption(f"KAT-KSE 3PL Japan · Updated: {pd.Timestamp.now().strftime('%Y-%m-%d')}")
    st.stop()


# ═══════════════════════════════════════════════
# MENU: 출고요청서 (Qoo10 → KSE OMS)
# ═══════════════════════════════════════════════
if menu == "📤 출고요청서 (Qoo10)":
    from qoo10 import generator as qgen

    st.subheader("📤 Qoo10 출고요청서 / 송장번호 업로드")

    tab_gen, tab_waybill, tab_mapping = st.tabs([
        "① 출고요청서 생성", "② QSM 송장 업로드", "🔧 상품 매핑"
    ])

    # ─── 탭1: 출고요청서 생성 ───
    with tab_gen:
        st.markdown("QSM에서 다운받은 **detail.csv**를 업로드하면 KSE OMS 업로드용 `Outbound_ship_conf.xlsx`를 생성합니다.")
        det_file = st.file_uploader(
            "QSM detail.csv 업로드", type=['csv'], key="qoo10_detail"
        )
        if det_file:
            try:
                rows = qgen.parse_qsm_csv(det_file.getvalue())
                st.info(f"QSM 주문 {len(rows)}건 인식")

                mappings = qgen.load_mappings()
                out_rows, errors, addr_changes = qgen.generate_outbound_rows(rows, mappings)
                audit = qgen.compute_audit(rows, out_rows, mappings)

                st.markdown("#### 📊 검수 지표 (OMS 업로드 결과와 대조)")
                st.caption("이 수치를 기록해두고 OMS 업로드 후 응답과 비교하세요.")

                mc1, mc2, mc3, mc4 = st.columns(4)
                def _chip(ok: bool) -> str:
                    return "✅" if ok else "⚠️"

                mc1.metric(
                    "총 상품 수량",
                    f"{audit['total_item_qty']} {_chip(audit['check_total_match_count'])}",
                    help="매핑 수량 합 (SKU 단위). 업로드 개수와 일치해야 정상.",
                )
                mc2.metric(
                    "주문 업로드 개수",
                    f"{audit['upload_row_count']} {_chip(audit['check_rows_match'])}",
                    help="KSE OMS 업로드될 row 개수",
                )
                mc3.metric(
                    "송장번호 개수",
                    f"{audit['unique_carts']} {_chip(audit['check_carts_match'])}",
                    help="KSE OMS 주문(출고) 요청 개수 = 고유 장바구니번호",
                )
                mc4.metric(
                    "주문번호 개수",
                    f"{audit['unique_orders']} {_chip(audit['check_orders_covered'])}",
                    help="QSM에 송장번호 업로드할 주문번호 개수",
                )

                st.caption(
                    f"🚚 실제 출고 PCS (予定数量 합계): **{audit['total_picking_pcs']}** · "
                    f"에러 **{len(errors)}건** · 주소 정제 **{len(addr_changes)}건**"
                )

                if errors:
                    st.warning("매핑되지 않은 주문이 있습니다. 먼저 **🔧 상품 매핑** 탭에서 추가하세요.")
                    err_df = pd.DataFrame(errors)
                    st.dataframe(err_df, width="stretch", hide_index=True)

                # 주소 정제 검토 (필요시 사용자 최종 판단)
                addr_approved = True  # 주소 변경 없으면 자동 통과
                final_addr_map = {}
                if addr_changes:
                    st.markdown("---")
                    st.markdown("#### ⚠️ 주소 정제 검토 (사람의 최종 판단 필요)")
                    st.caption(
                        "자동 특수문자 제거 로직이 완벽하지 않아 **원본 주소와 정제 주소를 함께 표시**합니다. "
                        "각 건마다 주소를 직접 확인하고, 필요시 **최종주소 컬럼을 수정**한 뒤 **승인** 체크를 켜세요. "
                        "모두 승인되어야 출고요청서를 다운로드할 수 있습니다."
                    )

                    base = pd.DataFrame(addr_changes).copy()
                    base['최종주소'] = base['정제주소']
                    base['승인'] = False

                    edited = st.data_editor(
                        base,
                        column_config={
                            '장바구니번호': st.column_config.TextColumn(disabled=True, width="small"),
                            '주문번호': st.column_config.TextColumn(disabled=True, width="small"),
                            '원본주소': st.column_config.TextColumn(disabled=True, width="medium"),
                            '정제주소': st.column_config.TextColumn(disabled=True, width="medium"),
                            '사유': st.column_config.TextColumn(disabled=True, width="medium",
                                help="원본에서 제거/치환된 문자와 이유"),
                            '최종주소': st.column_config.TextColumn(required=True, width="medium",
                                help="부적합하면 이 컬럼을 편집. 기본값=정제주소."),
                            '승인': st.column_config.CheckboxColumn(required=True),
                        },
                        hide_index=True, width="stretch",
                        column_order=('장바구니번호', '주문번호', '원본주소', '정제주소',
                                      '사유', '최종주소', '승인'),
                        key="addr_review",
                    )

                    approved_count = int(edited['승인'].sum())
                    total_to_approve = len(edited)
                    addr_approved = (approved_count == total_to_approve)

                    if addr_approved:
                        st.success(f"주소 검토 완료 ({total_to_approve}건 모두 승인됨)")
                    else:
                        st.warning(f"승인 대기: {total_to_approve - approved_count}건 남음 (전체 {total_to_approve}건)")

                    # 장바구니번호 → 사용자 확정 주소 매핑
                    for _, r in edited.iterrows():
                        if r['승인']:
                            final_addr_map[str(r['장바구니번호'])] = str(r['최종주소']).strip()

                st.markdown("---")

                # 사용자 승인 주소로 업데이트
                if final_addr_map:
                    for row in out_rows:
                        cart = str(row.get('注文番号', ''))
                        if cart in final_addr_map:
                            row['基本住所'] = final_addr_map[cart]
                            row['注文先基本住所'] = final_addr_map[cart]

                if out_rows:
                    df_out = pd.DataFrame(out_rows)
                    st.markdown("**미리보기**")
                    st.dataframe(
                        df_out[['倉庫コード', '商品コード', '予定数量', '注文番号',
                                '仕入先名/受取人名', '郵便番号コード', '基本住所']],
                        width="stretch", hide_index=True,
                    )

                    if not addr_approved:
                        st.error(
                            "⚠️ 위 주소 검토 표에서 모든 건을 승인해야 다운로드할 수 있습니다. "
                            "최종주소 컬럼을 확인/수정 후 각 행의 **승인** 체크박스를 켜세요."
                        )
                    else:
                        xlsx_bytes = qgen.build_outbound_xlsx(out_rows)
                        today_str = datetime.date.today().strftime('%Y%m%d')
                        st.download_button(
                            f"📥 Outbound_ship_conf_btoc_{today_str}.xlsx 다운로드",
                            data=xlsx_bytes,
                            file_name=f"Outbound_ship_conf_btoc_{today_str}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            width="stretch",
                            type="primary",
                        )
            except Exception as e:
                st.error(f"처리 중 오류: {e}")

    # ─── 탭2: QSM 송장번호 업로드 양식 ───
    with tab_waybill:
        st.markdown("QSM **brief.csv**와 송장번호 매핑을 합쳐 QSM 업로드용 CSV를 생성합니다.")

        brief_file = st.file_uploader(
            "QSM brief.csv 업로드", type=['csv'], key="qoo10_brief"
        )

        st.markdown("**장바구니번호 → 송장번호 매핑**")
        wb_method = st.radio(
            "입력 방식",
            ["KSE OMS 다운로드 파일 업로드", "DB(shipments)에서 자동 조회", "수동 입력 (표)"],
            horizontal=True, label_visibility="collapsed",
            help="권장: KSE OMS에서 내려받은 '주문(출고&입고) 내역' xlsx 파일 업로드"
        )

        waybill_map = {}
        if brief_file:
            brief_rows = qgen.parse_qsm_csv(brief_file.getvalue())
            cart_nos = [r.get('장바구니번호', '') for r in brief_rows]

            if wb_method == "KSE OMS 다운로드 파일 업로드":
                oms_file = st.file_uploader(
                    "KSE OMS 주문(출고&입고) 내역 xlsx 업로드",
                    type=['xlsx'], key="oms_waybill_xlsx",
                    help="주문 번호(장바구니번호) ↔ 운송장 번호 추출"
                )
                if oms_file:
                    try:
                        oms_map = qgen.parse_kse_oms_waybill(oms_file.getvalue())
                        st.info(f"KSE OMS 파일에서 {len(oms_map)}건 송장번호 추출")
                        for c in cart_nos:
                            if c in oms_map:
                                waybill_map[c] = oms_map[c]
                        st.success(f"brief 기준 매칭: {len(waybill_map)}/{len(cart_nos)}건")
                        if len(waybill_map) < len(cart_nos):
                            missing = [c for c in cart_nos if c not in waybill_map]
                            st.warning(f"미매칭 (KSE 출고 미완료 or 취소 가능성): {', '.join(missing)}")
                    except Exception as e:
                        st.error(f"파일 파싱 실패: {e}")

            elif wb_method == "DB(shipments)에서 자동 조회":
                if cart_nos:
                    placeholders = ','.join(['%s'] * len(cart_nos))
                    df = pg.query_df(f"""
                        SELECT order_no, MAX(waybill) AS waybill
                        FROM shipments
                        WHERE order_no IN ({placeholders}) AND waybill NOT LIKE 'NOWB_%%'
                        GROUP BY order_no
                    """, cart_nos)
                    for _, r in df.iterrows():
                        waybill_map[str(r['order_no'])] = str(r['waybill'])
                    st.info(f"DB에서 {len(waybill_map)}/{len(cart_nos)}건 매칭됨")
                    if len(waybill_map) < len(cart_nos):
                        missing = [c for c in cart_nos if c not in waybill_map]
                        st.warning(f"미매칭: {', '.join(missing)}")

            else:  # 수동 입력
                df_wb = pd.DataFrame({
                    '장바구니번호': cart_nos,
                    '수취인명': [r.get('수취인명', '') for r in brief_rows],
                    '송장번호': [''] * len(brief_rows),
                })
                edited = st.data_editor(
                    df_wb, width="stretch", hide_index=True,
                    disabled=['장바구니번호', '수취인명'],
                )
                for _, r in edited.iterrows():
                    if r['송장번호']:
                        waybill_map[r['장바구니번호']] = str(r['송장번호']).strip()

        if brief_file and waybill_map:
            if st.button("송장번호 채워진 CSV 생성", width="stretch", type="primary"):
                csv_bytes, missing = qgen.build_qsm_waybill_csv(brief_file.getvalue(), waybill_map)
                today_str = datetime.date.today().strftime('%Y%m%d')
                st.download_button(
                    f"📥 QSM_waybill_{today_str}.csv 다운로드",
                    data=csv_bytes,
                    file_name=f"QSM_waybill_{today_str}.csv",
                    mime="text/csv",
                    width="stretch",
                )
                if missing:
                    st.warning(f"송장번호 미입력 {len(missing)}건: {', '.join(missing)}")

    # ─── 탭3: 상품 매핑 관리 ───
    with tab_mapping:
        st.markdown("Qoo10 상품/옵션 조합 ↔ KSE SKU 매핑 관리. 새 상품 옵션이 나오면 여기서 추가하세요.")

        maps_df = pg.query_df("""
            SELECT qoo10_name, qoo10_option, item_codes, sku_codes, quantities, enabled
            FROM qoo10_product_mapping ORDER BY enabled DESC, qoo10_name, qoo10_option
        """)
        st.caption(f"총 {len(maps_df)}개 매핑 (활성 {int(maps_df['enabled'].sum())}개)")
        st.dataframe(maps_df, width="stretch", hide_index=True)

        with st.expander("➕ 새 매핑 추가"):
            col1, col2 = st.columns(2)
            with col1:
                new_name = st.text_area("Qoo10 상품명", height=80)
                new_option = st.text_input("Qoo10 옵션정보 (옵션 없으면 빈칸)")
            with col2:
                new_skus = st.text_input("KSE SKU 코드 (쉼표 구분)", placeholder="KC_8809885876128,KC_8809885876555")
                new_qtys = st.text_input("수량 (쉼표 구분)", value="1")
                new_enabled = st.checkbox("활성화", value=True)

            if st.button("매핑 저장", type="primary"):
                if not new_name or not new_skus:
                    st.error("상품명과 SKU는 필수입니다.")
                else:
                    skus_list = [s.strip() for s in new_skus.split(',')]
                    qtys_list = [q.strip() for q in new_qtys.split(',')]
                    if len(qtys_list) == 1 and len(skus_list) > 1:
                        qtys_list = ['1'] * len(skus_list)
                    item_codes = ','.join(skus_list)  # 단순화: SKU코드를 품목코드로도 사용
                    conn = pg.connect()
                    with conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO qoo10_product_mapping
                            (qoo10_name, qoo10_option, item_codes, sku_codes, quantities, enabled)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (qoo10_name, qoo10_option) DO UPDATE SET
                                item_codes = EXCLUDED.item_codes,
                                sku_codes = EXCLUDED.sku_codes,
                                quantities = EXCLUDED.quantities,
                                enabled = EXCLUDED.enabled,
                                updated_at = CURRENT_TIMESTAMP
                        """, (new_name.strip(), new_option.strip(), item_codes,
                              ','.join(skus_list), ','.join(qtys_list), new_enabled))
                    conn.commit()
                    conn.close()
                    st.success("저장됨. 새로고침하세요.")

    st.stop()


# ═══════════════════════════════════════════════
# MENU: 물류비 검토
# ═══════════════════════════════════════════════
months = load_data("SELECT DISTINCT year_month FROM invoices ORDER BY year_month DESC")
if months.empty:
    st.error("DB에 데이터가 없습니다.")
    st.stop()

selected_month = st.sidebar.selectbox(
    "조회 월", months["year_month"].tolist(), format_func=fmt_month
)

# ─── 탭 구성 ───
tab_review, tab_mom, tab_overview, tab_findings, tab_prices = st.tabs(
    ["📋 물류비 검토", "📈 전월 비교", "📊 월간 요약", "🔍 검토 결과", "💰 단가표"]
)


# ═══════════════════════════════════════════════
# TAB 1: 물류비 검토 (3-Way 대조)
# ═══════════════════════════════════════════════
with tab_review:
    st.subheader(f"물류비 검토 [{fmt_month(selected_month)}]")
    st.caption("KSE 청구서(PDF) vs KSE 엑셀(詳細) vs KATCHERS(OMS) 3-Way 대조")

    line_all = load_data("""
        SELECT li.line_no, li.category, li.description, li.unit_price, li.unit,
               li.billed_qty, li.billed_amount,
               li.kse_excel_qty, li.kse_excel_amount,
               li.kat_qty, li.kat_amount,
               li.verdict, li.note,
               inv.invoice_type
        FROM line_items li
        JOIN invoices inv ON li.invoice_id = inv.id
        WHERE inv.year_month = ?
        ORDER BY inv.invoice_type, li.line_no
    """, [selected_month])

    # shipments 테이블 기반 KATCHERS 자동 계산값으로 해당 카테고리 덮어쓰기
    kat_auto = compute_kat_side(selected_month)
    if kat_auto:
        for cat, qty in kat_auto.items():
            mask = line_all['category'] == cat
            if mask.any():
                unit_price = line_all.loc[mask, 'unit_price'].iloc[0]
                line_all.loc[mask, 'kat_qty'] = qty
                line_all.loc[mask, 'kat_amount'] = qty * unit_price if qty > 0 else None
                if qty == 0:
                    line_all.loc[mask, 'kat_qty'] = None

        # 판정 재계산: PDF ≠ KATCHERS면 '불일치'
        for idx, row in line_all.iterrows():
            if row['category'] not in kat_auto:
                continue
            if pd.isna(row['kat_qty']):
                continue
            if row['billed_qty'] != row['kat_qty']:
                line_all.at[idx, 'verdict'] = '불일치'
            elif row['kse_excel_qty'] is not None and not pd.isna(row['kse_excel_qty']) \
                    and row['billed_qty'] == row['kse_excel_qty'] == row['kat_qty']:
                line_all.at[idx, 'verdict'] = 'OK'

    if not line_all.empty:
        monthly = line_all[line_all['invoice_type'] == 'monthly'].copy()

        if not monthly.empty:
            st.markdown("#### 정기 물류비")

            # 3-way 대조 테이블
            display = monthly[[
                'line_no', 'description', 'unit_price', 'unit',
                'billed_qty', 'billed_amount',
                'kse_excel_qty', 'kse_excel_amount',
                'kat_qty', 'kat_amount',
                'verdict'
            ]].copy()
            display.columns = [
                'No', '작업내용', '단가', '단위',
                'PDF 개수', 'PDF 금액',
                '엑셀 개수', '엑셀 금액',
                'KATCHERS 개수', 'KATCHERS 금액',
                '판정'
            ]

            # Format numbers
            for col in ['PDF 개수', '엑셀 개수', 'KATCHERS 개수']:
                display[col] = display[col].apply(lambda x: f"{int(x):,}" if pd.notna(x) and x != 0 else "-")
            for col in ['PDF 금액', '엑셀 금액', 'KATCHERS 금액']:
                display[col] = display[col].apply(lambda x: f"¥{int(x):,}" if pd.notna(x) and x != 0 else "-")
            display['단가'] = display['단가'].apply(lambda x: f"¥{int(x):,}")

            def highlight_verdict(val):
                if val == '불일치':
                    return 'background-color: #ffcccc; color: #cc0000; font-weight: bold'
                elif val == '확인필요':
                    return 'background-color: #fff3cd; color: #856404; font-weight: bold'
                elif val == 'OK':
                    return 'background-color: #d4edda; color: #155724'
                return ''

            styled = display.style.map(highlight_verdict, subset=['판정'])
            st.dataframe(styled, width="stretch", hide_index=True, height=950)

            # 소계 비교
            st.markdown("---")
            pdf_subtotal = monthly['billed_amount'].sum()
            excel_subtotal = monthly['kse_excel_amount'].dropna().sum()
            # For KATCHERS, only sum where we have data; for items w/o KATCHERS data, use PDF
            kat_items_with_data = monthly[monthly['kat_amount'].notna()]
            kat_items_without = monthly[monthly['kat_amount'].isna()]
            kat_subtotal = kat_items_with_data['kat_amount'].sum() + kat_items_without['billed_amount'].sum()

            col1, col2, col3 = st.columns(3)
            with col1:
                st.markdown("**KSE 청구서 (PDF)**")
                st.metric("소계", f"¥{int(pdf_subtotal):,}")
                st.metric("소비세(10%)", f"¥{int(pdf_subtotal * 0.1):,}")
                st.metric("합계", f"¥{int(pdf_subtotal * 1.1):,}")
            with col2:
                st.markdown("**KSE 엑셀 (詳細)**")
                st.metric("소계", f"¥{int(excel_subtotal):,}",
                         delta=f"¥{int(excel_subtotal - pdf_subtotal):,}" if excel_subtotal != pdf_subtotal else None,
                         delta_color="inverse")
                st.metric("소비세(10%)", f"¥{int(excel_subtotal * 0.1):,}")
                st.metric("합계", f"¥{int(excel_subtotal * 1.1):,}")
            with col3:
                st.markdown("**KATCHERS (OMS)**")
                st.caption("KATCHERS 데이터가 있는 항목만 대체, 나머지는 PDF 기준")
                st.metric("소계", f"¥{int(kat_subtotal):,}",
                         delta=f"¥{int(kat_subtotal - pdf_subtotal):,}" if kat_subtotal != pdf_subtotal else None,
                         delta_color="inverse")

            # PDF vs 엑셀 일치 여부 요약
            st.markdown("---")
            st.markdown("#### KSE PDF vs 엑셀 일치 여부")
            match_items = monthly[monthly['kse_excel_qty'].notna()].copy()
            if not match_items.empty:
                match_items['match'] = match_items.apply(
                    lambda r: '일치' if r['billed_qty'] == r['kse_excel_qty'] else '불일치', axis=1
                )
                matched = len(match_items[match_items['match'] == '일치'])
                mismatched = len(match_items[match_items['match'] == '불일치'])
                no_data = len(monthly[monthly['kse_excel_qty'].isna()])

                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("일치", f"{matched}건", delta_color="normal")
                with col2:
                    st.metric("불일치", f"{mismatched}건",
                             delta_color="inverse" if mismatched > 0 else "normal")
                with col3:
                    st.metric("엑셀 근거 없음", f"{no_data}건")

                if mismatched > 0:
                    mismatch_df = match_items[match_items['match'] == '불일치'][
                        ['line_no', 'description', 'billed_qty', 'kse_excel_qty', 'note']
                    ].copy()
                    mismatch_df.columns = ['No', '항목', 'PDF 수량', '엑셀 수량', '비고']
                    st.dataframe(mismatch_df, width="stretch", hide_index=True)

        # 별도 청구
        extra = line_all[line_all['invoice_type'] == 'extra'].copy()
        if not extra.empty:
            st.markdown("---")
            st.markdown("#### 별도 청구")
            extra_display = extra[['description', 'unit_price', 'unit',
                                   'billed_qty', 'billed_amount', 'verdict', 'note']].copy()
            extra_display.columns = ['항목', '단가', '단위', '개수', '금액', '판정', '비고']
            extra_display['단가'] = extra_display['단가'].apply(lambda x: f"¥{int(x):,}")
            extra_display['금액'] = extra_display['금액'].apply(lambda x: f"¥{int(x):,}")
            st.dataframe(extra_display, width="stretch", hide_index=True)

            inv_extra = load_data(
                "SELECT subtotal, tax_standard, total, note FROM invoices WHERE year_month=? AND invoice_type='extra'",
                [selected_month]
            )
            if not inv_extra.empty:
                for _, r in inv_extra.iterrows():
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("소계", f"¥{int(r['subtotal']):,}")
                    with col2:
                        st.metric("소비세", f"¥{int(r['tax_standard']):,}")
                    with col3:
                        st.metric("합계", f"¥{int(r['total']):,}")


# ═══════════════════════════════════════════════
# TAB 2: 전월 비교
# ═══════════════════════════════════════════════
with tab_mom:
    st.subheader(f"전월 비교 [{fmt_month(selected_month)}]")

    all_months = load_data("SELECT DISTINCT year_month FROM invoices ORDER BY year_month")['year_month'].tolist()
    current_idx = all_months.index(selected_month) if selected_month in all_months else -1

    if current_idx <= 0:
        st.info("전월 데이터가 없습니다. 2개월 이상의 데이터가 적재되면 전월 비교가 표시됩니다.")

        # 현재 월 데이터만 표시
        invoices = load_data("SELECT * FROM invoices WHERE year_month = ?", [selected_month])
        metrics = load_data("SELECT * FROM monthly_metrics WHERE year_month = ?", [selected_month])

        if not invoices.empty:
            st.markdown("#### 현재 월 데이터")

            col1, col2 = st.columns(2)
            with col1:
                monthly_total = invoices[invoices['invoice_type'] == 'monthly']['total'].sum()
                extra_total = invoices[invoices['invoice_type'] == 'extra']['total'].sum()
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=['정기 물류비', '별도 청구', '합계'],
                    y=[monthly_total, extra_total, monthly_total + extra_total],
                    text=[f"¥{int(v):,}" for v in [monthly_total, extra_total, monthly_total + extra_total]],
                    textposition='outside',
                    marker_color=['#4e79a7', '#f28e2b', '#59a14f']
                ))
                fig.update_layout(title='청구 금액', yaxis_title='금액 (JPY)')
                st.plotly_chart(fig, width="stretch")

            with col2:
                if not metrics.empty:
                    m = metrics.iloc[0]
                    fig = go.Figure()
                    fig.add_trace(go.Bar(
                        x=['B2C 출하', 'B2B 출하', '피킹 PCS', '입고 CTN'],
                        y=[m['b2c_shipments'], m['b2b_shipments'], m['total_picking_pcs'], m['inbound_ctn']],
                        text=[f"{int(v):,}" for v in [m['b2c_shipments'], m['b2b_shipments'], m['total_picking_pcs'], m['inbound_ctn']]],
                        textposition='outside',
                        marker_color='#4e79a7'
                    ))
                    fig.update_layout(title='운영 지표', yaxis_title='수량')
                    st.plotly_chart(fig, width="stretch")

            # 항목별 금액 (현재 월만)
            st.markdown("---")
            st.markdown("#### 항목별 금액")
            items = load_data("""
                SELECT li.line_no as "No", li.description as "작업내용",
                       li.unit_price as "단가", li.unit as "단위",
                       li.billed_qty as "개수", li.billed_amount as "금액"
                FROM line_items li
                JOIN invoices inv ON li.invoice_id = inv.id
                WHERE inv.year_month = ? AND inv.invoice_type = 'monthly' AND li.billed_amount > 0
                ORDER BY li.billed_amount DESC
            """, [selected_month])
            if not items.empty:
                fig = px.bar(items, x='금액', y='작업내용', orientation='h',
                           title='항목별 청구 금액',
                           color='금액', color_continuous_scale='Blues')
                fig.update_layout(height=500, yaxis_title=None, showlegend=False)
                fig.update_traces(
                    text=[f"¥{int(v):,}" for v in items['금액']],
                    textposition='outside'
                )
                st.plotly_chart(fig, width="stretch")

    else:
        prev_month = all_months[current_idx - 1]
        st.caption(f"{fmt_month(prev_month)} → {fmt_month(selected_month)}")

        # 청구 금액 비교
        inv_cur = load_data("SELECT * FROM invoices WHERE year_month = ?", [selected_month])
        inv_prev = load_data("SELECT * FROM invoices WHERE year_month = ?", [prev_month])
        met_cur = load_data("SELECT * FROM monthly_metrics WHERE year_month = ?", [selected_month])
        met_prev = load_data("SELECT * FROM monthly_metrics WHERE year_month = ?", [prev_month])

        cur_total = inv_cur['total'].sum()
        prev_total = inv_prev['total'].sum()
        cur_monthly = inv_cur[inv_cur['invoice_type'] == 'monthly']['total'].sum()
        prev_monthly = inv_prev[inv_prev['invoice_type'] == 'monthly']['total'].sum()

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("총 청구액", f"¥{int(cur_total):,}",
                     delta=f"¥{int(cur_total - prev_total):,}",
                     delta_color="inverse")
        with col2:
            st.metric("정기 물류비", f"¥{int(cur_monthly):,}",
                     delta=f"¥{int(cur_monthly - prev_monthly):,}",
                     delta_color="inverse")
        with col3:
            if not met_cur.empty and not met_prev.empty:
                cur_avg = met_cur.iloc[0]['avg_cost_per_order']
                prev_avg = met_prev.iloc[0]['avg_cost_per_order']
                st.metric("건당 평균", f"¥{int(cur_avg):,}",
                         delta=f"¥{int(cur_avg - prev_avg):,}",
                         delta_color="inverse")

        st.markdown("---")

        # 운영 지표 비교
        if not met_cur.empty and not met_prev.empty:
            mc = met_cur.iloc[0]
            mp = met_prev.iloc[0]

            st.markdown("#### 운영 지표 비교")
            col1, col2, col3, col4, col5 = st.columns(5)
            with col1:
                st.metric("B2C 출하", f"{int(mc['b2c_shipments']):,}건",
                         delta=f"{int(mc['b2c_shipments'] - mp['b2c_shipments']):,}건")
            with col2:
                st.metric("피킹 PCS", f"{int(mc['total_picking_pcs']):,}",
                         delta=f"{int(mc['total_picking_pcs'] - mp['total_picking_pcs']):,}")
            with col3:
                st.metric("입고 CTN", f"{int(mc['inbound_ctn']):,}",
                         delta=f"{int(mc['inbound_ctn'] - mp['inbound_ctn']):,}")
            with col4:
                st.metric("보관 PLT", f"{int(mc['storage_plt'])}",
                         delta=f"{int(mc['storage_plt'] - mp['storage_plt'])}")
            with col5:
                st.metric("배송료 비중", f"{mc['shipping_cost_ratio']:.1f}%",
                         delta=f"{mc['shipping_cost_ratio'] - mp['shipping_cost_ratio']:.1f}%p",
                         delta_color="inverse")

        # 항목별 전월 비교 테이블
        st.markdown("---")
        st.markdown("#### 항목별 전월 비교")
        items_cur = load_data("""
            SELECT li.line_no, li.description, li.unit_price, li.unit,
                   li.billed_qty, li.billed_amount
            FROM line_items li JOIN invoices inv ON li.invoice_id = inv.id
            WHERE inv.year_month = ? AND inv.invoice_type = 'monthly'
            ORDER BY li.line_no
        """, [selected_month])
        items_prev = load_data("""
            SELECT li.line_no, li.description, li.unit_price, li.unit,
                   li.billed_qty, li.billed_amount
            FROM line_items li JOIN invoices inv ON li.invoice_id = inv.id
            WHERE inv.year_month = ? AND inv.invoice_type = 'monthly'
            ORDER BY li.line_no
        """, [prev_month])

        if not items_cur.empty and not items_prev.empty:
            merged = items_cur.merge(items_prev, on=['line_no', 'description', 'unit_price', 'unit'],
                                     suffixes=('_cur', '_prev'), how='outer')
            merged = merged.fillna(0)
            merged['qty_diff'] = merged['billed_qty_cur'] - merged['billed_qty_prev']
            merged['amt_diff'] = merged['billed_amount_cur'] - merged['billed_amount_prev']

            display_mom = merged[
                (merged['billed_amount_cur'] > 0) | (merged['billed_amount_prev'] > 0)
            ].copy()
            display_mom = display_mom[[
                'line_no', 'description', 'unit_price', 'unit',
                'billed_qty_prev', 'billed_amount_prev',
                'billed_qty_cur', 'billed_amount_cur',
                'qty_diff', 'amt_diff'
            ]].copy()
            display_mom.columns = [
                'No', '작업내용', '단가', '단위',
                f'{fmt_month(prev_month)} 개수', f'{fmt_month(prev_month)} 금액',
                f'{fmt_month(selected_month)} 개수', f'{fmt_month(selected_month)} 금액',
                '개수 증감', '금액 증감'
            ]

            def highlight_diff(val):
                if isinstance(val, (int, float)):
                    if val > 0:
                        return 'color: #cc0000'
                    elif val < 0:
                        return 'color: #155724'
                return ''

            styled_mom = display_mom.style.map(highlight_diff, subset=['개수 증감', '금액 증감'])
            st.dataframe(styled_mom, width="stretch", hide_index=True, height=700)

            # 차트
            col1, col2 = st.columns(2)
            with col1:
                chart_data = display_mom[display_mom[f'{fmt_month(selected_month)} 금액'] > 0].copy()
                fig = go.Figure()
                fig.add_trace(go.Bar(name=fmt_month(prev_month),
                                    x=chart_data['작업내용'],
                                    y=chart_data[f'{fmt_month(prev_month)} 금액'],
                                    marker_color='#aec7e8'))
                fig.add_trace(go.Bar(name=fmt_month(selected_month),
                                    x=chart_data['작업내용'],
                                    y=chart_data[f'{fmt_month(selected_month)} 금액'],
                                    marker_color='#4e79a7'))
                fig.update_layout(title='항목별 금액 비교', barmode='group',
                                 xaxis_tickangle=-45, height=500)
                st.plotly_chart(fig, width="stretch")

            with col2:
                diff_data = display_mom[display_mom['금액 증감'] != 0].sort_values('금액 증감')
                if not diff_data.empty:
                    colors = ['#cc0000' if v > 0 else '#155724' for v in diff_data['금액 증감']]
                    fig = go.Figure()
                    fig.add_trace(go.Bar(
                        x=diff_data['금액 증감'], y=diff_data['작업내용'],
                        orientation='h', marker_color=colors,
                        text=[f"¥{int(v):,}" for v in diff_data['금액 증감']],
                        textposition='outside'
                    ))
                    fig.update_layout(title='전월 대비 증감', height=500, yaxis_title=None)
                    st.plotly_chart(fig, width="stretch")


# ═══════════════════════════════════════════════
# TAB 3: 월간 요약
# ═══════════════════════════════════════════════
with tab_overview:
    invoices = load_data("SELECT * FROM invoices WHERE year_month = ?", [selected_month])
    metrics = load_data("SELECT * FROM monthly_metrics WHERE year_month = ?", [selected_month])

    total_amount = invoices["total"].sum()
    monthly_inv = invoices[invoices["invoice_type"] == "monthly"]
    extra_inv = invoices[invoices["invoice_type"] == "extra"]

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("총 청구액", f"¥{total_amount:,}")
    with col2:
        st.metric("정기 물류비", f"¥{monthly_inv['total'].sum():,}" if not monthly_inv.empty else "¥0")
    with col3:
        st.metric("별도 청구", f"¥{extra_inv['total'].sum():,}" if not extra_inv.empty else "¥0")
    with col4:
        findings_count = load_data(
            "SELECT COUNT(*) as cnt FROM review_findings WHERE year_month = ? AND severity IN ('error','warning')",
            [selected_month]
        )["cnt"].iloc[0]
        st.metric("확인 필요", f"{findings_count}건")

    st.markdown("---")

    if not metrics.empty:
        m = metrics.iloc[0]
        st.subheader("운영 지표")
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.metric("B2C 출하", f"{int(m['b2c_shipments']):,}건")
        with col2:
            st.metric("B2B 출하", f"{int(m['b2b_shipments'])}건")
        with col3:
            st.metric("피킹 수량", f"{int(m['total_picking_pcs']):,} PCS")
        with col4:
            st.metric("건당 평균 물류비", f"¥{int(m['avg_cost_per_order']):,}")
        with col5:
            st.metric("배송료 비중", f"{m['shipping_cost_ratio']:.1f}%")

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("입고 CTN", f"{int(m['inbound_ctn']):,}")
        with col2:
            st.metric("입고 PLT", f"{int(m['inbound_plt'])}")
        with col3:
            st.metric("보관 PLT", f"{int(m['storage_plt'])}")
        with col4:
            st.metric("오키나와/낙도", f"{int(m['okinawa_shipments'])}건 (¥{int(m['okinawa_relay_fee']):,})")

    # 비용 구성비
    st.markdown("---")
    st.subheader("비용 구성비")

    line_items = load_data("""
        SELECT li.category, li.description, li.billed_amount
        FROM line_items li JOIN invoices inv ON li.invoice_id = inv.id
        WHERE inv.year_month = ? AND inv.invoice_type = 'monthly' AND li.billed_amount > 0
        ORDER BY li.billed_amount DESC
    """, [selected_month])

    if not line_items.empty:
        cost_groups = {
            '배송료': ['ship_60', 'ship_80', 'ship_100', 'ship_120_140', 'ship_160',
                      'ship_60_oki', 'ship_80_oki', 'ship_100_oki', 'okinawa_relay'],
            '출하수수료': ['b2c_handling', 'b2b_handling'],
            '포장자재': ['box_60', 'box_80', 'box_120', 'box_140', 'cushion'],
            '보관료': ['storage'],
            '입고비': ['inbound_plt', 'inbound_ctn', 'inbound_pcs'],
            '피킹': ['picking_pcs', 'picking_ctn'],
            '기타': ['set_work', 'labeling', 'repalletize', 'truck_load'],
        }
        group_totals = []
        for group_name, cats in cost_groups.items():
            total = line_items[line_items['category'].isin(cats)]['billed_amount'].sum()
            if total > 0:
                group_totals.append({'카테고리': group_name, '금액': total})

        df_groups = pd.DataFrame(group_totals)
        if not df_groups.empty:
            col1, col2 = st.columns(2)
            with col1:
                fig = px.pie(df_groups, values='금액', names='카테고리',
                           title='비용 카테고리별 구성비',
                           color_discrete_sequence=px.colors.qualitative.Set2)
                fig.update_traces(textinfo='percent+label', textposition='inside')
                st.plotly_chart(fig, width="stretch")
            with col2:
                fig = px.bar(df_groups.sort_values('금액', ascending=True),
                           x='금액', y='카테고리', orientation='h',
                           title='비용 카테고리별 금액', color='금액',
                           color_continuous_scale='Blues')
                fig.update_layout(showlegend=False, yaxis_title=None)
                fig.update_traces(
                    text=[f'¥{v:,}' for v in df_groups.sort_values('금액', ascending=True)['금액']],
                    textposition='outside'
                )
                st.plotly_chart(fig, width="stretch")

    # 청구서 목록
    st.markdown("---")
    st.subheader("청구서 목록")
    inv_display = invoices[['invoice_type', 'invoice_no', 'invoice_date', 'due_date',
                            'subtotal', 'tax_standard', 'total', 'note']].copy()
    inv_display.columns = ['유형', '청구NO', '청구일', '지급기한', '소계', '소비세', '합계', '비고']
    inv_display['유형'] = inv_display['유형'].map({'monthly': '월 정기', 'extra': '별도 청구'})
    st.dataframe(inv_display, width="stretch", hide_index=True)


# ═══════════════════════════════════════════════
# TAB 4: 검토 결과
# ═══════════════════════════════════════════════
with tab_findings:
    st.subheader("검토 결과")

    findings = load_data("""
        SELECT severity, category, title, description,
               billed_value, actual_value, amount_diff, status
        FROM review_findings WHERE year_month = ?
        ORDER BY CASE severity WHEN 'error' THEN 1 WHEN 'warning' THEN 2 ELSE 3 END
    """, [selected_month])

    if findings.empty:
        st.success("확인 필요 사항이 없습니다.")
    else:
        errors = len(findings[findings['severity'] == 'error'])
        warnings = len(findings[findings['severity'] == 'warning'])
        infos = len(findings[findings['severity'] == 'info'])

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("오류", f"{errors}건") if errors == 0 else st.error(f"오류: {errors}건")
        with col2:
            st.metric("경고", f"{warnings}건") if warnings == 0 else st.warning(f"경고: {warnings}건")
        with col3:
            st.info(f"참고: {infos}건")

        st.markdown("---")

        for _, row in findings.iterrows():
            icon = {'error': '🔴', 'warning': '🟡', 'info': '🔵'}.get(row['severity'], '⚪')
            badge = {'open': '미확인', 'confirmed': '확인완료', 'resolved': '해결'}.get(row['status'], '')

            with st.expander(f"{icon} [{badge}] {row['title']}", expanded=(row['severity'] != 'info')):
                st.markdown(row['description'])
                col1, col2, col3 = st.columns(3)
                with col1:
                    if row['billed_value']:
                        st.markdown(f"**청구 값**: {row['billed_value']}")
                with col2:
                    if row['actual_value']:
                        st.markdown(f"**실제 값**: {row['actual_value']}")
                with col3:
                    if row['amount_diff'] and row['amount_diff'] != 0:
                        diff = int(row['amount_diff'])
                        color = "red" if diff > 0 else "green"
                        label = "과다" if diff > 0 else "미청구"
                        st.markdown(f"**금액 차이**: :{color}[¥{abs(diff):,} ({label})]")

        st.markdown("---")
        total_over = findings[findings['amount_diff'] > 0]['amount_diff'].sum()
        total_under = findings[findings['amount_diff'] < 0]['amount_diff'].sum()
        col1, col2 = st.columns(2)
        with col1:
            st.metric("과다 청구 (세전)", f"¥{int(total_over):,}",
                     delta=f"¥{int(total_over * 1.1):,} (세후)", delta_color="inverse")
        with col2:
            st.metric("미청구/유리 (세전)", f"¥{int(abs(total_under)):,}",
                     delta=f"¥{int(abs(total_under) * 1.1):,} (세후)", delta_color="normal")


# ═══════════════════════════════════════════════
# TAB 5: 단가표
# ═══════════════════════════════════════════════
with tab_prices:
    st.subheader("현행 단가표")

    prices = load_data("""
        SELECT description as "항목", unit_price as "단가(JPY)", unit as "단위",
               effective_from as "적용시작", effective_to as "적용종료"
        FROM unit_prices WHERE effective_to IS NULL ORDER BY id
    """)

    if not prices.empty:
        prices['적용종료'] = prices['적용종료'].fillna('현행')
        prices['적용시작'] = prices['적용시작'].apply(
            lambda x: f"{x[:4]}년 {int(x[4:]):02d}월" if x else ''
        )
        st.dataframe(prices, width="stretch", hide_index=True)

        st.markdown("---")
        fig = px.bar(prices.sort_values('단가(JPY)', ascending=True),
                    x='단가(JPY)', y='항목', orientation='h',
                    color='단가(JPY)', color_continuous_scale='Viridis',
                    title='항목별 단가 비교')
        fig.update_layout(height=600, yaxis_title=None, showlegend=False)
        st.plotly_chart(fig, width="stretch")


# ─── Footer ───
st.markdown("---")
st.caption("KAT-KSE 3PL Japan | 국제익스프레스 물류비 관리 시스템")
