#!/usr/bin/env python3
"""
거래처 원장 자동화 웹앱
실행: python3 app.py
브라우저: http://localhost:5001
"""

import os, uuid, zipfile, io, json, tempfile, shutil, csv, re, gc, threading, time
from flask import Flask, render_template, request, jsonify, send_from_directory, Response
import pandas as pd

import generate_ledger as gl

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100 MB

UPLOAD_BASE = os.path.join(tempfile.gettempdir(), "doson_ledger")
HEADER_SIGNATURES = {'매출거래처명', '품목명', '거래일자', '매출공급가액', '매출합계금액'}


# ─────────────────────────────────────────────────────────────────────────────
# TSV 파싱
# ─────────────────────────────────────────────────────────────────────────────
def normalize_col(s):
    return re.sub(r'[\s\n\r]+', '', str(s))


def find_header_idx(rows):
    for i, row in enumerate(rows):
        if {normalize_col(c) for c in row} & HEADER_SIGNATURES:
            return i
    return 0


def parse_tsv(text):
    text = text.strip()

    def _build_df(rows):
        if not rows or len(rows) < 2:
            return None
        hi = find_header_idx(rows)
        headers = rows[hi]
        data = rows[hi + 1:]
        if not data:
            return None
        ncols = len(headers)
        normalized = []
        for r in data:
            if len(r) < ncols:
                r = r + [''] * (ncols - len(r))
            normalized.append(r[:ncols])
        df = pd.DataFrame(normalized, columns=headers).fillna('').astype(str)
        return df

    raw_lines = [l for l in text.split('\n') if l.strip()]

    # 1차: 표준 csv.reader
    try:
        rows = [r for r in csv.reader(io.StringIO(text), delimiter='\t')
                if any(c.strip() for c in r)]
        df = _build_df(rows)
        if df is not None and len(df) >= max(1, len(raw_lines) * 0.25):
            return df, len(df)
    except Exception:
        pass

    # 2차: QUOTE_NONE
    try:
        rows = [r for r in csv.reader(io.StringIO(text), delimiter='\t',
                                       quoting=csv.QUOTE_NONE, escapechar='\\')
                if any(c.strip() for c in r)]
        df = _build_df(rows)
        if df is not None and len(df) > 0:
            return df, len(df)
    except Exception:
        pass

    # 3차: 단순 split
    rows = [l.split('\t') for l in raw_lines]
    df = _build_df(rows)
    return (df, len(df)) if df is not None else (None, 0)


# ─────────────────────────────────────────────────────────────────────────────
# 동기 처리 (세션·스레드·SSE 없음)
# ─────────────────────────────────────────────────────────────────────────────
def output_dir(sid):
    return os.path.join(UPLOAD_BASE, sid, 'output')


def process_all(df):
    """거래처별 원장을 순차 생성하고 결과를 반환. 오류 시 error 키 포함."""
    if not os.path.isfile(gl.TEMPLATE_PATH):
        return {'error': f'템플릿 파일을 찾을 수 없습니다:\n{gl.TEMPLATE_PATH}'}

    col_map = gl.detect_columns(df)
    customer_col = col_map.get('customer')
    if not customer_col:
        return {'error': f"'매출 거래처명' 컬럼을 찾을 수 없습니다.\n감지된 컬럼: {list(df.columns[:10])}"}

    year, month = gl.detect_year_month(df, col_map)

    df_clean = df[df[customer_col].str.strip() != ''].copy().reset_index(drop=True)
    groups = list(df_clean.groupby(customer_col, sort=True))

    sid = str(uuid.uuid4())
    out = output_dir(sid)
    os.makedirs(out, exist_ok=True)

    errors = []
    for customer, grp in groups:
        customer = str(customer).strip()
        if not customer:
            continue
        try:
            gl.create_ledger(customer, grp.reset_index(drop=True),
                             year, month, col_map, out)
        except Exception as e:
            errors.append(f'{customer}: {e}')
        gc.collect()   # 워크북 메모리 거래처마다 강제 회수

    files = sorted(os.listdir(out))
    return {
        'session_id': sid,
        'files': files,
        'year': year,
        'month': month,
        'errors': errors,
        'total': len(groups),
    }


# ─────────────────────────────────────────────────────────────────────────────
# 자동 정리 (2시간 이상 된 세션 삭제)
# ─────────────────────────────────────────────────────────────────────────────
def _cleanup():
    while True:
        time.sleep(1800)
        try:
            if not os.path.isdir(UPLOAD_BASE):
                continue
            now = time.time()
            for sid in os.listdir(UPLOAD_BASE):
                path = os.path.join(UPLOAD_BASE, sid)
                if now - os.path.getmtime(path) > 7200:
                    shutil.rmtree(path, ignore_errors=True)
        except Exception:
            pass

threading.Thread(target=_cleanup, daemon=True).start()


# ─────────────────────────────────────────────────────────────────────────────
# 라우트
# ─────────────────────────────────────────────────────────────────────────────
@app.route('/')
def index():
    return render_template('index.html')


@app.errorhandler(Exception)
def handle_exception(e):
    """Flask 내 모든 예외를 JSON으로 반환 — HTML 에러 페이지 방지"""
    import traceback
    return jsonify(error=f'{e}\n{traceback.format_exc()}'), 500


@app.route('/paste', methods=['POST'])
def paste():
    try:
        body = request.get_json(silent=True) or {}
        text = (body.get('text') or '').strip()
        if not text:
            return jsonify(error='데이터가 없습니다. 엑셀에서 복사 후 붙여넣어 주세요.'), 400

        df, nrows = parse_tsv(text)
        if df is None or nrows == 0:
            return jsonify(error='데이터를 인식할 수 없습니다. 엑셀에서 헤더 포함 전체를 복사해 주세요.'), 400

        result = process_all(df)   # ← 동기 처리 (완료될 때까지 대기)

        if 'error' in result:
            return jsonify(error=result['error']), 500

        return jsonify(
            session_id=result['session_id'],
            files=result['files'],
            year=result['year'],
            month=result['month'],
            parsed_rows=nrows,
            total=result['total'],
            errors=result['errors'],
        )
    except Exception as e:
        import traceback
        return jsonify(error=f'{e}\n{traceback.format_exc()}'), 500


@app.route('/download/<sid>/<path:filename>')
def download_file(sid, filename):
    out = output_dir(sid)
    if not os.path.isdir(out):
        return '파일을 찾을 수 없습니다.', 404
    return send_from_directory(out, filename, as_attachment=True)


@app.route('/download-zip/<sid>')
def download_zip(sid):
    out = output_dir(sid)
    if not os.path.isdir(out):
        return '파일을 찾을 수 없습니다.', 404

    # 연도·월은 파일명에서 추출 시도
    label = '원장_전체'
    files = os.listdir(out)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        for fname in sorted(files):
            zf.write(os.path.join(out, fname), fname)
    buf.seek(0)
    return Response(buf, mimetype='application/zip',
                    headers={'Content-Disposition': f'attachment; filename="{label}.zip"'})


if __name__ == '__main__':
    os.makedirs(UPLOAD_BASE, exist_ok=True)
    print('=' * 50)
    print('  거래처 원장 자동화 웹앱')
    print('  브라우저에서 http://localhost:5001 열기')
    print('=' * 50)
    app.run(debug=False, host='0.0.0.0', port=5001)
