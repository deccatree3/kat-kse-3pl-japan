"""
KAT-KSE 3PL Japan - DB 초기화 스크립트
SQLite DB 스키마 생성 및 초기 데이터 적재
"""
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "logistics.db")


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def create_tables(conn):
    conn.executescript("""
    -- 월별 청구서 (인보이스 단위)
    CREATE TABLE IF NOT EXISTS invoices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        year_month TEXT NOT NULL,          -- 'YYYYMM'
        invoice_type TEXT NOT NULL,        -- 'monthly' | 'extra'
        invoice_no TEXT,                   -- 청구NO
        invoice_date TEXT,                 -- 청구일
        due_date TEXT,                     -- 지급기한
        subtotal INTEGER NOT NULL,         -- 소계 (세전)
        tax_rate_reduced REAL DEFAULT 0.08,
        tax_reduced INTEGER DEFAULT 0,     -- 경감세율 소비세
        tax_rate_standard REAL DEFAULT 0.10,
        tax_standard INTEGER DEFAULT 0,    -- 표준 소비세
        total INTEGER NOT NULL,            -- 합계 (세후)
        note TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_invoices_ym_type
        ON invoices(year_month, invoice_type, invoice_no);

    -- 항목별 상세 (청구 라인)
    CREATE TABLE IF NOT EXISTS line_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        invoice_id INTEGER NOT NULL REFERENCES invoices(id),
        line_no INTEGER,                   -- 항목 번호
        category TEXT NOT NULL,            -- 카테고리 코드
        description TEXT NOT NULL,         -- 항목명
        unit_price REAL NOT NULL,          -- 단가
        unit TEXT NOT NULL,                -- 단위 (PLT, CTN, PCS, 건, 매, 개, SET, 식)
        billed_qty REAL DEFAULT 0,         -- 청구 수량
        billed_amount INTEGER DEFAULT 0,   -- 청구 금액
        verified_qty REAL,                 -- 검증 수량
        verdict TEXT DEFAULT 'OK',         -- OK | 불일치 | 확인필요
        note TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_line_items_invoice
        ON line_items(invoice_id);

    -- 단가 이력 (단가 변동 추적)
    CREATE TABLE IF NOT EXISTS unit_prices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        category TEXT NOT NULL,
        description TEXT NOT NULL,
        unit_price REAL NOT NULL,
        unit TEXT NOT NULL,
        effective_from TEXT NOT NULL,      -- 적용 시작 YYYYMM
        effective_to TEXT,                 -- 적용 종료 YYYYMM (NULL=현행)
        note TEXT
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_unit_prices_cat_from
        ON unit_prices(category, effective_from);

    -- 월별 운영 지표
    CREATE TABLE IF NOT EXISTS monthly_metrics (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        year_month TEXT NOT NULL UNIQUE,
        b2c_shipments INTEGER DEFAULT 0,
        b2b_shipments INTEGER DEFAULT 0,
        total_picking_pcs INTEGER DEFAULT 0,
        avg_picking_per_order REAL DEFAULT 0,
        inbound_ctn INTEGER DEFAULT 0,
        inbound_plt INTEGER DEFAULT 0,
        storage_plt INTEGER DEFAULT 0,
        okinawa_shipments INTEGER DEFAULT 0,
        okinawa_relay_fee INTEGER DEFAULT 0,
        avg_cost_per_order REAL DEFAULT 0,  -- 건당 평균 물류비
        shipping_cost_ratio REAL DEFAULT 0  -- 배송료 비중 (%)
    );

    -- 검토 결과 (불일치/확인필요 사항)
    CREATE TABLE IF NOT EXISTS review_findings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        year_month TEXT NOT NULL,
        severity TEXT NOT NULL,            -- 'error' | 'warning' | 'info'
        category TEXT NOT NULL,
        title TEXT NOT NULL,
        description TEXT,
        billed_value TEXT,
        actual_value TEXT,
        amount_diff INTEGER DEFAULT 0,     -- 금액 차이 (양수=과다, 음수=과소)
        status TEXT DEFAULT 'open',        -- 'open' | 'confirmed' | 'resolved'
        resolved_note TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_findings_ym
        ON review_findings(year_month);
    """)
    conn.commit()


def insert_202603_data(conn):
    """2026년 3월분 데이터 적재"""
    cur = conn.cursor()

    # --- Invoice 1: 월 정기 물류비 ---
    cur.execute("""
        INSERT OR IGNORE INTO invoices
        (year_month, invoice_type, invoice_no, invoice_date, due_date,
         subtotal, tax_reduced, tax_standard, total, note)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, ('202603', 'monthly', None, '2026-03-31', '2026-04-30',
          398485, 0, 39849, 438334, '3월분 정기 물류비'))
    inv1_id = cur.lastrowid

    # --- Invoice 2: KOTRA 별도 청구 ---
    cur.execute("""
        INSERT OR IGNORE INTO invoices
        (year_month, invoice_type, invoice_no, invoice_date, due_date,
         subtotal, tax_reduced, tax_standard, total, note)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, ('202603', 'extra', 'JITP2605106-0000', '2026-03-31', '2026-04-30',
          123759, 0, 12376, 136135, 'KOTRA 정산금 부족분'))
    inv2_id = cur.lastrowid

    # --- Line items for monthly invoice ---
    items = [
        (1, 'inbound_plt', '입고,재고등록(PLT)', 980, 'PLT', 0, 0, 4, '확인필요', 'Excel상 4PLT 입고이나 미청구(유리)'),
        (2, 'inbound_ctn', '입고,재고등록(CTN)', 95, 'CTN', 132, 12540, 132, 'OK', '17+34+80+1=132'),
        (3, 'inbound_pcs', '입고,재고등록(PCS)', 10, 'PCS', 0, 0, 0, 'OK', None),
        (4, 'storage', '보관료', 2100, 'PLT', 7, 14700, None, '확인필요', '월간 PLT수 별도검증 필요'),
        (5, 'picking_pcs', '피킹요금_PCS', 9, 'PCS', 694, 6246, 694, 'OK', None),
        (6, 'picking_ctn', '피킹요금_CTN', 95, 'CTN', 4, 380, 4, 'OK', 'B2B FBA 4CTN'),
        (7, 'b2c_handling', 'B2C 출하작업수수료', 85, '건', 584, 49640, 583, '불일치', '+1건 과다(85엔)'),
        (8, 'b2b_handling', 'B2B 출하작업수수료', 125, '건', 1, 125, 1, 'OK', 'FBA15G7N1QC2'),
        (9, 'cushion', '완충자재/Material Surcharge', 30, '매', 584, 17520, 583, '불일치', '+1매 과다(30엔)'),
        (10, 'set_work', '세트 작업 비용', 300, 'SET', 0, 0, 0, 'OK', None),
        (11, 'labeling', '라벨링 작업 비용', 20, '매', 4, 80, 4, 'OK', None),
        (12, 'repalletize', 'Repalletizing 비용', 1200, 'PLT', 0, 0, 0, 'OK', None),
        (13, 'truck_load', '출하작업비용_PLT(트럭적재)', 1200, 'PLT', 0, 0, 0, 'OK', None),
        (14, 'box_60', '40~60size 포장박스', 46, '개', 579, 26634, 579, 'OK', None),
        (15, 'box_80', '80size 포장박스', 75, '개', 4, 300, 4, 'OK', None),
        (16, 'box_120', '100~120size 포장박스', 185, '개', 0, 0, 0, 'OK', None),
        (17, 'box_140', '140size 포장박스', 410, '개', 0, 0, 0, 'OK', None),
        (18, 'ship_60', 'B2C 배송료(~60사이즈)', 450, '건', 579, 260550, 579, 'OK', None),
        (19, 'ship_80', 'B2C 배송료(80사이즈)', 455, '건', 4, 1820, 4, 'OK', None),
        (20, 'ship_100', 'B2C 배송료(100사이즈)', 640, '건', 0, 0, 0, 'OK', None),
        (21, 'ship_120_140', 'B2C 배송료(120,140사이즈)', 1180, '건', 0, 0, 0, 'OK', None),
        (22, 'ship_160', 'B2C 배송료(160사이즈)', 1250, '건', 0, 0, 0, 'OK', None),
        (23, 'okinawa_relay', '오키나와 및 낙도 중계료', 7950, '식', 1, 7950, 7950, 'OK', '개별 중계료 합산 일치'),
        (24, 'ship_60_oki', 'B2C 배송료(~60사이즈)_沖縄', 1330, '건', 0, 0, 0, 'OK', None),
        (25, 'ship_80_oki', 'B2C 배송료(80사이즈)_沖縄', 1880, '건', 0, 0, 0, 'OK', None),
        (26, 'ship_100_oki', 'B2C 배송료(100사이즈)_沖縄', 2430, '건', 0, 0, 0, 'OK', None),
    ]
    for item in items:
        cur.execute("""
            INSERT INTO line_items
            (invoice_id, line_no, category, description, unit_price, unit,
             billed_qty, billed_amount, verified_qty, verdict, note)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (inv1_id, *item))

    # --- KOTRA invoice line item ---
    cur.execute("""
        INSERT INTO line_items
        (invoice_id, line_no, category, description, unit_price, unit,
         billed_qty, billed_amount, verified_qty, verdict, note)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (inv2_id, 1, 'kotra_settle', '출하작업수수료(KOTRA 정산금 부족분)',
          123759, '一式', 1, 123759, None, '확인필요', '이전 정산 관련 별도 확인 필요'))

    # --- Unit prices ---
    unit_prices = [
        ('inbound_plt', '입고,재고등록(PLT)', 980, 'PLT'),
        ('inbound_ctn', '입고,재고등록(CTN)', 95, 'CTN'),
        ('inbound_pcs', '입고,재고등록(PCS)', 10, 'PCS'),
        ('storage', '보관료', 2100, 'PLT'),
        ('picking_pcs', '피킹요금_PCS', 9, 'PCS'),
        ('picking_ctn', '피킹요금_CTN', 95, 'CTN'),
        ('b2c_handling', 'B2C 출하작업수수료', 85, '건'),
        ('b2b_handling', 'B2B 출하작업수수료', 125, '건'),
        ('cushion', '완충자재/Material Surcharge', 30, '매'),
        ('set_work', '세트 작업 비용', 300, 'SET'),
        ('labeling', '라벨링 작업 비용', 20, '매'),
        ('repalletize', 'Repalletizing 비용', 1200, 'PLT'),
        ('truck_load', '출하작업비용_PLT', 1200, 'PLT'),
        ('box_60', '40~60size 포장박스', 46, '개'),
        ('box_80', '80size 포장박스', 75, '개'),
        ('box_120', '100~120size 포장박스', 185, '개'),
        ('box_140', '140size 포장박스', 410, '개'),
        ('ship_60', 'B2C 배송료(~60사이즈)', 450, '건'),
        ('ship_80', 'B2C 배송료(80사이즈)', 455, '건'),
        ('ship_100', 'B2C 배송료(100사이즈)', 640, '건'),
        ('ship_120_140', 'B2C 배송료(120,140사이즈)', 1180, '건'),
        ('ship_160', 'B2C 배송료(160사이즈)', 1250, '건'),
        ('okinawa_relay', '오키나와 및 낙도 중계료', 7950, '식'),
        ('ship_60_oki', 'B2C 배송료(~60사이즈)_沖縄', 1330, '건'),
        ('ship_80_oki', 'B2C 배송료(80사이즈)_沖縄', 1880, '건'),
        ('ship_100_oki', 'B2C 배송료(100사이즈)_沖縄', 2430, '건'),
    ]
    for cat, desc, price, unit in unit_prices:
        cur.execute("""
            INSERT OR IGNORE INTO unit_prices
            (category, description, unit_price, unit, effective_from)
            VALUES (?, ?, ?, ?, ?)
        """, (cat, desc, price, unit, '202603'))

    # --- Monthly metrics ---
    cur.execute("""
        INSERT OR IGNORE INTO monthly_metrics
        (year_month, b2c_shipments, b2b_shipments, total_picking_pcs,
         avg_picking_per_order, inbound_ctn, inbound_plt, storage_plt,
         okinawa_shipments, okinawa_relay_fee, avg_cost_per_order, shipping_cost_ratio)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, ('202603', 583, 1, 694, 1.19, 132, 4, 7, 6, 7950, 683, 67.8))

    # --- Review findings ---
    findings = [
        ('warning', 'b2c_handling', 'B2C 출하건수 불일치',
         '청구서 584건 vs Excel집계 583건 (+1건 과다)', '584', '583', 85, 'open', None),
        ('warning', 'cushion', '완충자재 수량 불일치',
         '청구서 584매 vs Excel집계 583건 (+1매 과다)', '584', '583', 30, 'open', None),
        ('info', 'inbound_plt', '입고 PLT 미청구',
         'Excel상 4PLT 입고이나 청구서에 미청구 (당사 유리, ¥3,920 미청구)', '0', '4', -3920, 'open', None),
        ('warning', 'storage', '보관료 PLT수 검증 불가',
         '7PLT × ¥2,100 = ¥14,700, 월간 보관 PLT수 데이터 없음', '7', None, 0, 'open', None),
        ('warning', 'kotra_settle', 'KOTRA 정산금 부족분 근거 미확인',
         '¥123,759 별도 청구, 근거자료 확인 필요', '123,759', None, 0, 'open', None),
    ]
    for f in findings:
        cur.execute("""
            INSERT INTO review_findings
            (year_month, severity, category, title, description,
             billed_value, actual_value, amount_diff, status, resolved_note)
            VALUES ('202603', ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, f)

    conn.commit()
    print("202603 data inserted successfully.")


if __name__ == "__main__":
    conn = get_conn()
    create_tables(conn)
    insert_202603_data(conn)
    conn.close()
    print(f"DB created at: {DB_PATH}")
