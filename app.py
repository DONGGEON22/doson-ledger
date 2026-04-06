#!/usr/bin/env python3
"""
거래처 원장 자동화 웹앱
실행: python3 app.py
브라우저: http://localhost:5001
"""

import os, uuid, threading, zipfile, io, time, json, tempfile, shutil, csv, re, gc
from flask import (Flask, render_template, request, jsonify,
                   send_from_directory, send_file, Response, stream_with_context)
from werkzeug.exceptions import HTTPException, RequestEntityTooLarge
import pandas as pd

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024

UPLOAD_BASE = os.path.join(tempfile.gettempdir(), 'doson_ledger')
SESSIONS: dict = {}
_LOCK = threading.Lock()
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
        return pd.DataFrame(normalized, columns=headers).fillna('').astype(str)

    raw_lines = [l for l in text.split('\n') if l.strip()]

    try:
        rows = [r for r in csv.reader(io.StringIO(text), delimiter='\t')
                if any(c.strip() for c in r)]
        df = _build_df(rows)
        if df is not None and len(df) >= max(1, len(raw_lines) * 0.25):
            return df, len(df)
    except Exception:
        pass

    try:
        rows = [r for r in csv.reader(io.StringIO(text), delimiter='\t',
                                       quoting=csv.QUOTE_NONE, escapechar='\\')
                if any(c.strip() for c in r)]
        df = _build_df(rows)
        if df is not None and len(df) > 0:
            return df, len(df)
    except Exception:
        pass

    rows = [l.split('\t') for l in raw_lines]
    df = _build_df(rows)
    return (df, len(df)) if df is not None else (None, 0)


# ─────────────────────────────────────────────────────────────────────────────
# 세션 헬퍼
# ─────────────────────────────────────────────────────────────────────────────
def session_dir(sid):
    return os.path.join(UPLOAD_BASE, sid)

def output_dir(sid):
    return os.path.join(session_dir(sid), 'output')

def _state_path(sid):
    return os.path.join(session_dir(sid), 'state.json')

def _save_state(sid):
    """세션 상태를 디스크에 동기 저장 (워커 재시작 후 복구용)"""
    try:
        with _LOCK:
            state = dict(SESSIONS.get(sid, {}))
        os.makedirs(session_dir(sid), exist_ok=True)
        with open(_state_path(sid), 'w', encoding='utf-8') as f:
            json.dump(state, f, ensure_ascii=False)
    except Exception:
        pass

def _load_state(sid):
    try:
        with open(_state_path(sid), 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# 자동 정리
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
                    with _LOCK:
                        SESSIONS.pop(sid, None)
        except Exception:
            pass

threading.Thread(target=_cleanup, daemon=True).start()


# ─────────────────────────────────────────────────────────────────────────────
# 백그라운드 처리
# ─────────────────────────────────────────────────────────────────────────────
def _run_processing(sid, df):
    def update(**kw):
        with _LOCK:
            SESSIONS[sid].update(kw)
        _save_state(sid)

    try:
        # openpyxl 등 무거운 의존성은 워커 기동·첫 요청 부담을 줄이기 위해 지연 import
        import generate_ledger as gl

        if not os.path.isfile(gl.TEMPLATE_PATH):
            update(error=f'템플릿 파일을 찾을 수 없습니다:\n{gl.TEMPLATE_PATH}')
            return

        col_map = gl.detect_columns(df)
        customer_col = col_map.get('customer')
        if not customer_col:
            update(error=f"'매출 거래처명' 컬럼을 찾을 수 없습니다.\n감지된 컬럼: {list(df.columns[:10])}")
            return

        year, month = gl.detect_year_month(df, col_map)
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
                errors.append(f'{customer}: {e}')
            # 거래처마다 GC 실행 — openpyxl 워크북 메모리 즉시 회수
            gc.collect()

        files = sorted(os.listdir(out))
        update(finished=True, files=files, errors=errors)

    except Exception as e:
        import traceback
        update(error=f'{e}\n{traceback.format_exc()}')


# ─────────────────────────────────────────────────────────────────────────────
# 라우트
# ─────────────────────────────────────────────────────────────────────────────
@app.route('/')
def index():
    return render_template('index.html')


@app.route('/healthz')
def healthz():
    """로드밸런서·플랫폼 헬스체크용 (본문 최소)"""
    return Response('ok', mimetype='text/plain')


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

        sid = str(uuid.uuid4())
        os.makedirs(output_dir(sid), exist_ok=True)
        with _LOCK:
            SESSIONS[sid] = dict(total=0, done=0, current='', finished=False,
                                 files=[], errors=[], error=None,
                                 year=None, month=None,
                                 parsed_rows=nrows, parsed_customers=0)
        _save_state(sid)

        threading.Thread(target=_run_processing, args=(sid, df), daemon=True).start()
        return jsonify(session_id=sid, parsed_rows=nrows)
    except RequestEntityTooLarge:
        return jsonify(error='요청 용량이 너무 큽니다. (최대 100MB)'), 413
    except Exception as e:
        import traceback
        return jsonify(error=f'{e}\n{traceback.format_exc()}'), 500


@app.route('/progress/<sid>')
def progress(sid):
    def stream():
        while True:
            with _LOCK:
                state = dict(SESSIONS.get(sid, {}))

            # 메모리에 없으면 디스크에서 복구
            if not state:
                state = _load_state(sid) or {}
                if state:
                    with _LOCK:
                        SESSIONS[sid] = state

            if not state:
                yield 'data: {"error": "세션을 찾을 수 없습니다."}\n\n'
                return

            yield f'data: {json.dumps(state, ensure_ascii=False)}\n\n'

            if state.get('finished') or state.get('error'):
                return

            time.sleep(0.5)   # 0.5초마다 이벤트 → 프록시 타임아웃 방지

    return Response(stream_with_context(stream()),
                    mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})


@app.route('/status/<sid>')
def status(sid):
    with _LOCK:
        state = dict(SESSIONS.get(sid, {}))
    if not state:
        state = _load_state(sid)
    if not state:
        return jsonify(error='세션을 찾을 수 없습니다.'), 404
    if sid not in SESSIONS:
        with _LOCK:
            SESSIONS[sid] = state
    return jsonify(state)


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
    files = sorted(os.listdir(out))
    if not files:
        return '파일을 찾을 수 없습니다.', 404

    with _LOCK:
        state = dict(SESSIONS.get(sid, {}))
    if not state:
        state = _load_state(sid) or {}

    # ASCII 전용: 프록시/게이트웨이가 비ASCII Content-Disposition에서 502 나는 경우 방지
    y, m = state.get('year'), state.get('month')
    try:
        if y is not None and m is not None:
            download_name = f'ledger_{int(y)}_{int(m):02d}.zip'
        else:
            download_name = 'ledger.zip'
    except (TypeError, ValueError):
        download_name = 'ledger.zip'

    fd, tmp_path = tempfile.mkstemp(suffix='.zip', prefix='doson_ledger_')
    os.close(fd)
    try:
        # xlsx는 이미 압축됨 → STORED가 더 빠르고 CPU/시간 부담이 적음(Render 타임아웃 완화)
        with zipfile.ZipFile(tmp_path, 'w', zipfile.ZIP_STORED) as zf:
            for fname in files:
                zf.write(os.path.join(out, fname), fname)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise

    resp = send_file(
        tmp_path,
        mimetype='application/zip',
        as_attachment=True,
        download_name=download_name,
    )

    @resp.call_on_close
    def _unlink_tmp():
        try:
            os.unlink(tmp_path)
        except OSError:
            pass

    return resp


@app.errorhandler(HTTPException)
def handle_http_exception(e):
    return jsonify(error=e.description or str(e)), e.code


@app.errorhandler(Exception)
def handle_exception(e):
    import traceback
    try:
        msg = f'{e}\n{traceback.format_exc()}'
        if len(msg) > 12000:
            msg = msg[:12000] + '\n...[truncated]'
        return jsonify(error=msg), 500
    except Exception:
        return Response(
            '{"error":"서버 내부 오류가 발생했습니다."}',
            status=500,
            mimetype='application/json; charset=utf-8',
        )


if __name__ == '__main__':
    os.makedirs(UPLOAD_BASE, exist_ok=True)
    port = int(os.environ.get('PORT', '5001'))
    print('=' * 50)
    print('  거래처 원장 자동화 웹앱')
    print(f'  브라우저에서 http://localhost:{port} 열기')
    print('=' * 50)
    app.run(debug=False, host='0.0.0.0', port=port)
