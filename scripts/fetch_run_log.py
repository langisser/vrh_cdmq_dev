"""
fetch_run_log.py — ดึง cell outputs จาก Databricks run ผ่าน /api/2.0/jobs/runs/export
usage: python3 fetch_run_log.py <task_run_id>
"""
import sys, os, json, re, base64, subprocess, requests

CONFIG_FILE = '/home/khaw/ClaudeCode/vrh_cdmq_dev/.databrickscfg'
HOST = 'https://adb-7405612978007880.0.azuredatabricks.net'

def get_token():
    r = subprocess.run(
        ['az', 'account', 'get-access-token',
         '--resource', '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d',
         '--query', 'accessToken', '-o', 'tsv'],
        capture_output=True, text=True
    )
    return r.stdout.strip()

def fetch_run_log(task_run_id: str):
    token = get_token()
    headers = {'Authorization': f'Bearer {token}'}

    # export HTML
    resp = requests.get(
        f'{HOST}/api/2.0/jobs/runs/export',
        headers=headers,
        params={'run_id': task_run_id, 'views_to_export': 'CODE'},
        timeout=60
    )
    resp.raise_for_status()
    data = resp.json()

    # views เป็น list ของ {name, content, type}
    views = data.get('views', [])
    if not views:
        print('No views returned')
        return

    for view in views:
        html = view.get('content', '')
        name = view.get('name', '')
        print(f'\n{"="*60}')
        print(f'View: {name}')
        print('='*60)

        # หา __DATABRICKS_NOTEBOOK_MODEL ใน HTML
        m = re.search(r'__DATABRICKS_NOTEBOOK_MODEL\s*=\s*[\'"]([^\'"]+)[\'"]', html)
        if not m:
            # ลอง JSON embed แบบอื่น
            m = re.search(r'id="__NEXT_DATA__"[^>]*>(\{.*?\})</script>', html, re.DOTALL)
            if not m:
                print('Cannot find notebook model in HTML')
                continue
            payload = json.loads(m.group(1))
        else:
            raw = m.group(1)
            # decode base64 หรือ unescape
            from urllib.parse import unquote
            decoded = base64.b64decode(raw + '==').decode('utf-8')
            payload = json.loads(unquote(decoded))

        commands = payload.get('commands', [])
        print(f'Total cells: {len(commands)}')
        for i, cmd in enumerate(commands):
            title = cmd.get('commandTitle') or f'Cell {i+1}'
            results = cmd.get('results') or {}
            text = ''
            # results อาจเป็น dict (resultType/data) หรือ list ของ output chunks
            if isinstance(results, dict):
                # รูปแบบเก่า: {resultType, data} หรือ {type, data}
                inner = results.get('data') or results.get('text') or ''
                if isinstance(inner, list):
                    text = ''.join(r.get('data', '') if isinstance(r, dict) else str(r) for r in inner)
                else:
                    text = str(inner)
            elif isinstance(results, list):
                # รูปแบบใหม่: list ของ {type, data, name, ...}
                text = ''.join(r.get('data', '') if isinstance(r, dict) else str(r) for r in results)
            if text:
                print(f'\n--- {title} ---')
                print(str(text)[:1000])

if __name__ == '__main__':
    run_id = sys.argv[1] if len(sys.argv) > 1 else '21741913734568'
    fetch_run_log(run_id)
