#!/usr/bin/env python3
"""
거래처 원장 자동화 웹앱
실행: python3 app.py
브라우저: http://localhost:5001
"""

import os, uuid, threading, zipfile, io, time, json, tempfile, shutil, csv, re
from flask import (Flask, render_template, request, jsonify,
                   send_from_directory, Response, stream_with_context)
import pandas as pd

import generate_ledger as gl

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100 MB

UPLOAD_BASE = os.path.join(tempfile.gettempdir(), "doson_ledger")
SESSIONS: dict = {}
_LOCK = threading.Lock()
ALLOWED_EXTS = {'.xlsx', '.xls', '.csv'}

# 헤더 식별용 핵심 컬럼 (정규화된 이름)
HEADER_SIGNATURES = {'매출거래처명', '품목명', '거래일자', '매출공급가액', '매출합계금액'}


# ─────────────────────────────────────────────────────────────────────────────
# TSV 파싱 (견고한 버전)
# ─────────────────────────────────────────────────────────────────────────────
def normalize_col(s):
    return re.sub(r'[\s\n\r]+', '', str(s))


def find_header_idx(rows):
    """헤더 행 인덱스 자동 탐색 (제목행 / 빈 행이 앞에 있어도 동작)"""
    for i, row in enumerate(rows):
        normalized = {normalize_col(c) for c in row}
        if normalized & HEADER_SIGNATURES:
            return i
    return 0  # fallback


def parse_tsv(text):
    """
    Excel 복사 데이터(TSV)를 DataFrame으로 변환.
    1차: csv.reader (따옴표 처리 포함)
    2차: 따옴표 무시 모드 (비고란에 따옴표가 있어 파싱 오류 시)
    3차: 단순 split 폴백
    """
    text = text.strip()

    def _build_df(rows):
        if not rows or len(rows) < 2:
            return None
        hi = find_header_idx(rows)
        headers = rows[hi]
        data = rows[hi + 1:]
        if not data:
            return None
        # 컬럼 수 통일 (헤더 기준)
        ncols = len(headers)
        normalized_data = []
        for r in data:
            if len(r) < ncols:
                r = r + [''] * (ncols - len(r))
            normalized_data.append(r[:ncols])
        df = pd.DataFrame(normalized_data, columns=headers)
        df = df.fillna('').astype(str)
        return df

    # ── 1차: 표준 csv.reader (멀티라인 따옴표 셀 지원) ──
    try:
        reader = csv.reader(io.StringIO(text), delimiter='\t')
        rows = [r for r in reader if any(c.strip() for c in r)]
        df = _build_df(rows)
        # 충분히 파싱됐는지 확인 (텍스트 줄 수 대비 30% 이상)
        raw_lines = [l for l in text.split('\n') if l.strip()]
        if df is not None and len(df) >= max(1, len(raw_lines) * 0.25):
            return df, len(df)
    except Exception:
        pass

    # ── 2차: 따옴표 무시 모드 (비고에 따옴표가 있는 경우) ──
    try:
        reader = csv.reader(io.StringIO(text), delimiter='\t',
                            quoting=csv.QUOTE_NONE, escapechar='\\')
        rows = [r for r in reader if any(c.strip() for c in r)]
        df = _build_df(rows)
        if df is not None and len(df) > 0:
            return df, len(df)
    except Exception:
        pass

    # ── 3차: 단순 split 폴백 ──
    lines = [l for l in text.split('\n') if l.strip()]
    rows = [l.split('\t') for l in lines]
    df = _build_df(rows)
    if df is not None:
        return df, len(df)

    return None, 0


# ─────────────────────────────────────────────────────────────────────────────
# 세션 헬퍼
# ─────────────────────────────────────────────────────────────────────────────
def session_dir(sid):
    return os.path.join(UPLOAD_BASE, sid)

def output_dir(sid):
    return os.path.join(session_dir(sid), 'output')


# ─────────────────────────────────────────────────────────────────────────────
# 자동 정리 (2시간 이상 된 세션 삭제)
# ─────────────────────────────────────────────────────────────────────────────
def _cleanup():
    while True:
        time.sleep(1800)
        try:
            now = time.time()
            if not os.path.isdir(UPLOAD_BASE):
                continue
            for sid in os.listdir(UPLOAD_BASE):
                path = os.path.join(UPLOAD_BASE, sid)
                if now - os.path.getmtime(path) > 7200:
                    shutil.rmtree(path, ignore_errors=True)
                    with _LOCK:
                        SESSIONS.pop(sid, None)
        except Exception:
            pass

threading.Thread(target=_cleanup, daemon=True).start()


# ─────────────────────────────────────────────────────────────────────────────
# 공통 처리 로직
# ─────────────────────────────────────────────────────────────────────────────
def _run_processing(sid, df):
    def update(**kw):
        with _LOCK:
            SESSIONS[sid].update(kw)
    try:
        if not os.path.isfile(gl.TEMPLATE_PATH):
            update(error=f"템플릿 파일을 찾을 수 없습니다:\n{gl.TEMPLATE_PATH}")
            return

        col_map = gl.detect_columns(df)
        customer_col = col_map.get('customer')
        if not customer_col:
            cols = list(df.columns[:10])
            update(error=f"'매출 거래처명' 컬럼을 찾을 수 없습니다.\n감지된 컬럼: {cols}")
            return

        year, month = gl.detect_year_month(df, col_map)

        # 빈 거래처명 제거 후 그룹핑
        # reset_index: groupby 후 각 그룹의 행 인덱스가 불연속해지는 것 방지
        df_clean = df[df[customer_col].str.strip() != ''].copy().reset_index(drop=True)
        groups = list(df_clean.groupby(customer_col, sort=True))
        update(total=len(groups), year=year, month=month,
               parsed_rows=len(df), parsed_customers=len(groups))

        out = output_dir(sid)
        os.makedirs(out, exist_ok=True)
        errors = []

        for i, (customer, grp) in enumerate(groups, 1):
            customer = str(customer).strip()
            if not customer:
                continue
            update(done=i, current=customer)
            try:
                gl.create_ledger(customer, grp.reset_index(drop=True),
                                 year, month, col_map, out)
            except Exception as e:
                errors.append(f"{customer}: {e}")

        files = sorted(os.listdir(out))
        update(finished=True, files=files, errors=errors)

    except Exception as e:
        import traceback
        update(error=f"{e}\n{traceback.format_exc()}")


# ─────────────────────────────────────────────────────────────────────────────
# 라우트
# ─────────────────────────────────────────────────────────────────────────────
@app.route('/')
def index():
    return render_template('index.html')


@app.route('/paste', methods=['POST'])
def paste():
    body = request.get_json(silent=True) or {}
    text = (body.get('text') or '').strip()
    if not text:
        return jsonify(error="데이터가 없습니다. 엑셀에서 복사 후 붙여넣어 주세요."), 400

    df, nrows = parse_tsv(text)
    if df is None or nrows == 0:
        return jsonify(error="데이터를 인식할 수 없습니다. 엑셀에서 헤더 포함 전체를 복사해 주세요."), 400

    sid = str(uuid.uuid4())
    os.makedirs(output_dir(sid), exist_ok=True)
    with _LOCK:
        SESSIONS[sid] = dict(total=0, done=0, current='', finished=False,
                              files=[], errors=[], error=None,
                              year=None, month=None,
                              parsed_rows=nrows, parsed_customers=0)

    threading.Thread(target=_run_processing, args=(sid, df), daemon=True).start()
    return jsonify(session_id=sid, parsed_rows=nrows)


@app.route('/upload', methods=['POST'])
def upload():
    f = request.files.get('file')
    if not f or not f.filename:
        return jsonify(error="파일을 선택해 주세요."), 400
    ext = os.path.splitext(f.filename)[1].lower()
    if ext not in ALLOWED_EXTS:
        return jsonify(error=f"지원하지 않는 형식입니다. ({', '.join(ALLOWED_EXTS)})"), 400

    sid = str(uuid.uuid4())
    sdir = session_dir(sid)
    os.makedirs(sdir, exist_ok=True)
    input_path = os.path.join(sdir, 'input' + ext)
    f.save(input_path)

    with _LOCK:
        SESSIONS[sid] = dict(total=0, done=0, current='', finished=False,
                              files=[], errors=[], error=None,
                              year=None, month=None, parsed_rows=0, parsed_customers=0)

    def _process_file(sid, path):
        try:
            df = gl.load_source(path)
            _run_processing(sid, df)
        except Exception as e:
            with _LOCK:
                SESSIONS[sid]['error'] = str(e)

    threading.Thread(target=_process_file, args=(sid, input_path), daemon=True).start()
    return jsonify(session_id=sid)


@app.route('/progress/<sid>')
def progress(sid):
    if sid not in SESSIONS:
        return Response('data: {"error": "세션을 찾을 수 없습니다."}\n\n',
                        mimetype='text/event-stream')

    def stream():
        while True:
            with _LOCK:
                state = dict(SESSIONS.get(sid, {}))
            yield f"data: {json.dumps(state, ensure_ascii=False)}\n\n"
            if state.get('finished') or state.get('error'):
                break
            time.sleep(0.3)

    return Response(stream_with_context(stream()),
                    mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})


@app.route('/download/<sid>/<path:filename>')
def download_file(sid, filename):
    out = output_dir(sid)
    if not os.path.isdir(out):
        return "세션을 찾을 수 없습니다.", 404
    return send_from_directory(out, filename, as_attachment=True)


@app.route('/download-zip/<sid>')
def download_zip(sid):
    out = output_dir(sid)
    if not os.path.isdir(out):
        return "세션을 찾을 수 없습니다.", 404
    state = SESSIONS.get(sid, {})
    label = (f"{state.get('year','')}년{state.get('month','')}월_원장"
             if state.get('year') else "원장_전체")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        for fname in os.listdir(out):
            zf.write(os.path.join(out, fname), fname)
    buf.seek(0)
    return Response(buf, mimetype='application/zip',
                    headers={'Content-Disposition': f'attachment; filename="{label}.zip"'})


if __name__ == '__main__':
    os.makedirs(UPLOAD_BASE, exist_ok=True)
    print("=" * 50)
    print("  거래처 원장 자동화 웹앱")
    print("  브라우저에서 http://localhost:5001 열기")
    print("=" * 50)
    app.run(debug=False, host='0.0.0.0', port=5001)
