"""Actions-only entry point; no database, private modules or local paths."""
import argparse
import json
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--seconds',type=int,default=1200)
    parser.add_argument('--max-requests',type=int,default=50)
    parser.add_argument('--max-batches',type=int,default=100)
    args=parser.parse_args()
    if not 60<=args.seconds<=3600 or not 1<=args.max_batches<=100: parser.error('Bounded cloud acquisition required')
    from price_lab.market_reference_pipeline import bootstrap,run
    result=run(bootstrap(ROOT/'config/market-reference-bootstrap.json'),ROOT/'data/research/market-reference-packages',
               seconds=args.seconds,max_requests=args.max_requests,max_batches=args.max_batches)
    if 'duckdb' in sys.modules or 'price_lab.local_credentials' in sys.modules: raise ValueError('Cloud reference runtime imported private/database code')
    print(json.dumps(result),flush=True)


if __name__=='__main__': main()
