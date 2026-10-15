"""Standalone data-only scheduled pipeline, safe to publish independently."""
import argparse
from datetime import datetime,timezone
import json
import os
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--audit',action='store_true')
    parser.add_argument('--publish-only',action='store_true')
    args=parser.parse_args()
    from price_lab.ingestion import resolve_current,refresh
    from price_lab.providers import Providers
    from price_lab.release_transport import pull_latest,publish_current
    root=ROOT/'data'/'market'
    if not args.publish_only:
        pull_latest(root)
        # Weekly complete historical revision audit; daily overlap repairs recent data.
        audit=args.audit or datetime.now(timezone.utc).weekday()==6
        out=refresh(Providers(raw_dir=root/'raw'),root=root,audit=audit)
        print(json.dumps({'stage':'refresh','status':out['status'],'reason':out.get('reason'),'attempt_id':out.get('attempt_id')}),flush=True)
        if out['status'] not in ('PUBLISHED','UNCHANGED'):raise SystemExit(1)
    current=resolve_current(root)
    if current is None:raise ValueError('Checkpoint bootstrap required')
    result=publish_current(root)
    print(json.dumps({'stage':'release',**result,'sequence':current['sequence'],'markets':current['markets']}),flush=True)

if __name__=='__main__':main()
