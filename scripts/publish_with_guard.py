#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Optional

SCRIPT_DIR = Path(__file__).resolve().parent
WORKSPACE = Path('/Users/lucifinil_chen/.openclaw/workspace')
LEDGER_FILE = WORKSPACE / 'intel' / 'collaboration' / 'media' / 'wemedia' / 'xiaohongshu' / 'publish-ledger.json'
SCHEMA_FILE = WORKSPACE / 'shared-context' / 'XHS-PUBLISH-PACK-SCHEMA.md'


def load_ledger():
    if LEDGER_FILE.exists():
        try:
            return json.loads(LEDGER_FILE.read_text(encoding='utf-8'))
        except Exception:
            return []
    return []


def save_ledger(entries):
    LEDGER_FILE.parent.mkdir(parents=True, exist_ok=True)
    LEDGER_FILE.write_text(json.dumps(entries, ensure_ascii=False, indent=2), encoding='utf-8')


def append_ledger(entry):
    rows = load_ledger()
    rows.append(entry)
    save_ledger(rows)


def parse_pack(path: str):
    text = Path(path).read_text(encoding='utf-8')
    data = {}
    current = None
    buffer = []
    for raw in text.splitlines():
        line = raw.rstrip('\n')
        if line.startswith('平台：'):
            data['platform'] = line.split('：',1)[1].strip(); current=None; buffer=[]; continue
        if line.startswith('内容ID：'):
            data['content_id'] = line.split('：',1)[1].strip(); current=None; buffer=[]; continue
        if line.startswith('标题：'):
            data['title'] = line.split('：',1)[1].strip(); current=None; buffer=[]; continue
        if line.startswith('正文：'):
            current='body'; buffer=[]; continue
        if line.startswith('图片路径：'):
            if current == 'body': data['body']='\n'.join(buffer).strip()
            current='images'; buffer=[]; continue
        if line.startswith('标签：'):
            if current == 'body': data['body']='\n'.join(buffer).strip()
            if current == 'images':
                data['image_paths']=[l[2:].strip() for l in buffer if l.strip().startswith('- ')]
            data['tags']=[x.strip() for x in line.split('：',1)[1].split(',') if x.strip()]
            current=None; buffer=[]; continue
        if line.startswith('可见性：'):
            data['visibility']=line.split('：',1)[1].strip(); current=None; buffer=[]; continue
        if line.startswith('备注：'):
            data['notes']=line.split('：',1)[1].strip(); current=None; buffer=[]; continue
        if current in ('body','images'):
            buffer.append(line)
    if current == 'body': data['body']='\n'.join(buffer).strip()
    if current == 'images': data['image_paths']=[l[2:].strip() for l in buffer if l.strip().startswith('- ')]
    return data


def validate_pack(data):
    errors=[]
    if data.get('platform') != 'xiaohongshu': errors.append('platform must be xiaohongshu')
    for k in ('content_id','title','body','image_paths'):
        if not data.get(k): errors.append(f'missing {k}')
    for p in data.get('image_paths', []):
        rp = str(Path(p).expanduser().resolve())
        if not os.path.isabs(rp) or not Path(rp).exists():
            errors.append(f'image missing: {p}')
    return {'ok': not errors, 'errors': errors, 'data': data}


def check_duplicate(data):
    title = data.get('title')
    content_id = data.get('content_id')
    images = [str(Path(p).expanduser().resolve()) for p in data.get('image_paths', [])]
    ledger = load_ledger()
    matches=[]
    for item in ledger:
        reasons=[]
        if title and item.get('title') == title:
            reasons.append('title')
        if content_id and item.get('content_id') == content_id:
            reasons.append('content_id')
        old_images = item.get('image_paths') or []
        if images and old_images and old_images == images:
            reasons.append('image_paths')
        if reasons:
            matches.append({'entry': item, 'reasons': reasons})

    remote = verify_publish({'title': title}) if title else {'ok': False, 'title_found': False, 'matched_count': 0, 'matched_rows': []}
    remote_duplicate = bool(remote.get('title_found'))
    local_duplicate = len(matches) > 0
    return {
        'ok': True,
        'duplicate': bool(remote_duplicate or local_duplicate),
        'remote_duplicate': remote_duplicate,
        'remote_matched_count': remote.get('matched_count', 0),
        'remote_matches': remote.get('matched_rows', [])[:3],
        'local_duplicate': local_duplicate,
        'local_match_count': len(matches),
        'local_matches': matches[:5],
    }


def run_publish(data):
    body_file = Path(tempfile.mkdtemp(prefix='xhs_pack_')) / 'body.txt'
    body_file.write_text(data['body'], encoding='utf-8')
    image_args = [str(Path(p).expanduser().resolve()) for p in data['image_paths']]
    cmd = [
        sys.executable,
        str(SCRIPT_DIR / 'cdp_publish.py'),
        'publish',
        '--title', data['title'],
        '--content-file', str(body_file),
        '--images',
        *image_args,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    return {
        'ok': proc.returncode == 0,
        'returncode': proc.returncode,
        'stdout': proc.stdout,
        'stderr': proc.stderr,
        'command': cmd,
    }


def _extract_content_data_payload(text: str):
    marker = 'CONTENT_DATA_RESULT:'
    if marker not in text:
        return None
    tail = text.split(marker, 1)[1].strip()
    start = tail.find('{')
    end = tail.rfind('}')
    if start == -1 or end == -1 or end <= start:
        return None
    try:
        return json.loads(tail[start:end+1])
    except Exception:
        return None



def verify_publish(data):
    cmd = [
        sys.executable,
        str(SCRIPT_DIR / 'cdp_publish.py'),
        'content-data',
        '--page-num', '1',
        '--page-size', '10',
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    text = proc.stdout + proc.stderr
    payload = _extract_content_data_payload(text)
    title = data.get('title', '')
    rows = payload.get('rows', []) if isinstance(payload, dict) else []
    title_found = False
    matched_rows = []
    for row in rows:
        row_title = str(row.get('title') or '')
        if title and title in row_title:
            title_found = True
            matched_rows.append(row)
    return {
        'ok': proc.returncode == 0 and title_found,
        'title_found': title_found,
        'matched_count': len(matched_rows),
        'matched_rows': matched_rows[:3],
        'raw_excerpt': text[:4000],
    }


def main():
    ap = argparse.ArgumentParser(description='XHS publish wrapper with duplicate guard and ledger')
    ap.add_argument('--pack', required=True)
    ap.add_argument('--step', choices=['validate_pack','check_duplicate','verify_publish','full'], default='full')
    args = ap.parse_args()

    data = parse_pack(args.pack)
    val = validate_pack(data)
    if args.step == 'validate_pack':
        print(json.dumps(val, ensure_ascii=False, indent=2))
        sys.exit(0 if val['ok'] else 1)
    if not val['ok']:
        print(json.dumps(val, ensure_ascii=False, indent=2))
        sys.exit(1)

    dup = check_duplicate(data)
    if args.step == 'check_duplicate':
        print(json.dumps(dup, ensure_ascii=False, indent=2))
        sys.exit(0 if dup['ok'] else 1)
    if dup['duplicate']:
        print(json.dumps({'ok': False, 'error': 'duplicate detected', 'duplicate': dup}, ensure_ascii=False, indent=2))
        sys.exit(1)

    if args.step == 'verify_publish':
        out = verify_publish(data)
        print(json.dumps(out, ensure_ascii=False, indent=2))
        sys.exit(0 if out['ok'] else 1)

    pub = run_publish(data)
    if not pub['ok']:
        print(json.dumps({'ok': False, 'validate': val, 'duplicate': dup, 'publish': pub}, ensure_ascii=False, indent=2))
        sys.exit(1)

    ver = verify_publish(data)
    if ver['ok']:
        append_ledger({
            'content_id': data.get('content_id'),
            'title': data.get('title'),
            'image_paths': [str(Path(p).expanduser().resolve()) for p in data.get('image_paths', [])],
            'visibility': data.get('visibility', 'public'),
            'recorded_at': __import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'verify': {'title_found': ver.get('title_found')},
        })

    result = {
        'content_id': data.get('content_id'),
        'title': data.get('title'),
        'title_found': ver.get('title_found'),
        'review_state': 'published_or_visible' if ver.get('ok') else 'unknown',
        'ledger_recorded': bool(ver.get('ok')),
    }
    print(json.dumps({'ok': bool(pub['ok'] and ver['ok']), 'validate': val, 'duplicate': dup, 'publish': {'returncode': pub['returncode']}, 'verify': ver, 'result': result}, ensure_ascii=False, indent=2))
    sys.exit(0 if pub['ok'] and ver['ok'] else 1)


if __name__ == '__main__':
    main()
