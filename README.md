# Durable daily price pipeline

Alpaca (US/crypto), KRX (Korea), KIS (Korean benchmark ETFs and corporate action evidence). This repository contains only the price ingestion runtime; investment strategies and the local research application are maintained separately.

The repository is public. API credentials belong only in Actions Secrets: `ALPACA_API_KEY`, `ALPACA_API_SECRET`, `KRX_API_KEY`, `KIS_APP_KEY`, `KIS_APP_SECRET`. `PRICE_DATA_KEY` is an independent random 32-byte, URL-safe base64 encryption key. Never commit values, raw responses or database files. Broker account identifiers are not needed.

## Operation

The default-branch workflow runs at 01:37 and 11:37 UTC (10:37 and 20:37 Korea), and can be started manually. GitHub scheduling may be delayed. Its runs are serialized and only trusted schedule/manual main-branch runs receive credentials. A Sunday run reconciles complete retained history; other runs reconcile the latest five sessions plus missing calendar dates. There is no rolling deletion at 750 sessions.

Each run restores the last authenticated encrypted Release checkpoint, collects completed exchange sessions, validates all required markets/actions/benchmarks, and creates a draft Release. It uploads the complete encrypted checkpoint before making that Release current. A failure leaves the last published Release intact. Prior checkpoints are retained; no billing plan, paid runner or storage upgrade is enabled by this project.

The file `checkpoint.pldata` uses streaming gzip + AES-256-GCM with authenticated version/sequence/lineage/watermark metadata. Decryption/authentication, database hash and database structure are checked before atomic publication. A local client can skip multiple remote generations if its current hash appears in authenticated ancestry; rollback and conflicting lineage are rejected. Protect and back up the decryption key separately. Losing both local key and Secret prevents recovery of encrypted data.

## Data contract

The DuckDB includes raw observations, adjustment evidence and a consumer `backtest_bars` view of split-adjusted OHLCV + turnover + VWAP. Raw OHLCV stays available. Adjusted turnover is value invariant under unit splits. US/crypto turnover is explicitly the provider VWAP times volume; Korean turnover is the provider's actual traded value. This is price return data, without total-return dividend reinvestment. The seed retains its original raw, quarantined and manual-override evidence tables.

KSD record dates are not treated as split execution dates. Relisting and reference-price evidence must confirm a unit change. Unresolved events block publication instead of guessing a factor. Provider event namespace/ID/revision prevent duplicate application and support corrections. Invalid batches and incomplete required benchmark sessions block publication.

The universe still has collection-time active/survivorship limitations. This pipeline does not manufacture historical delisted constituents, point-in-time capitalizations or missing security classifications. Provider subscriptions, permissions, availability and correction windows remain external dependencies. Check the Actions run log for health; the presence of a local checkpoint does not prove the latest scheduled run succeeded.

## Local runtime

Python 3.13: `python -m pip install -r requirements-ingestion.txt`, then `python -m pytest tests -q`.
After a trusted operator bootstraps the first encrypted Release, run `python scripts/price_pipeline.py` with the required environment secrets. The included script never requests brokerage account access or submits orders.
