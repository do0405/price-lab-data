# Durable daily price pipeline

Alpaca (US/crypto), KRX (Korea), KIS (Korean benchmark ETFs and corporate action evidence). This repository contains only the price ingestion runtime; investment strategies and the local research application are maintained separately.

The repository is public. API credentials belong only in Actions Secrets: `ALPACA_API_KEY`, `ALPACA_API_SECRET`, `KRX_API_KEY`, `KIS_APP_KEY`, `KIS_APP_SECRET`. `PRICE_DATA_KEY` is an independent random 32-byte, URL-safe base64 encryption key. Never commit values, raw responses or database files. Broker account identifiers are not needed.

## Operation

The default-branch workflow runs daily at 19:37 America/New_York (08:37/09:37 next day in Korea, following DST) and 20:37 Asia/Seoul, and can be started manually. Both slots are after the respective regular close, with time for provider publication/revisions. Weekends remain enabled for crypto. GitHub scheduling may be delayed. Its runs are serialized and only trusted schedule/manual main-branch runs receive credentials. A Sunday run reconciles complete retained history; other runs reconcile the latest five sessions plus missing calendar dates. There is no rolling deletion at 750 sessions.

Each run restores the last authenticated encrypted Release checkpoint, collects completed exchange sessions, validates all required markets/actions/benchmarks, and creates a draft Release. It uploads the complete encrypted checkpoint before making that Release current. A failure leaves the last published Release intact. Prior checkpoints are retained; no billing plan, paid runner or storage upgrade is enabled by this project.

The file `checkpoint.pldata` uses streaming gzip + AES-256-GCM with authenticated version/sequence/lineage/watermark metadata. Decryption/authentication, database hash and database structure are checked before atomic publication. A local client can skip multiple remote generations if its current hash appears in authenticated ancestry; rollback and conflicting lineage are rejected. Protect and back up the decryption key separately. Losing both local key and Secret prevents recovery of encrypted data.

GitHub limits each Release asset to less than 2GiB. Small checkpoints keep the original single-file format. At or above that boundary, the same encrypted container is split into parts of at most1GiB with `checkpoint.index.json`. A domain-separated HMAC authenticates the full checkpoint identity, ordered part sizes/hashes and full ciphertext hash before reassembly and GCM decryption. Missing, altered, duplicate or conflicting parts are rejected. The ciphertext bound is32GiB+32MiB and the existing decompressed database bound remains32GiB. Splitting and reassembly require temporary disk space in addition to the checkpoint; this does not enlarge GitHub runner disks or authorize paid storage.

## Data contract

The DuckDB includes raw observations, adjustment evidence and a consumer `backtest_bars` view of split-adjusted OHLCV + turnover + VWAP. Raw OHLCV stays available. Adjusted turnover is value invariant under unit splits. US/crypto turnover is explicitly the provider VWAP times volume; Korean turnover is the provider's actual traded value. This is price return data, without total-return dividend reinvestment. The seed retains its original raw, quarantined and manual-override evidence tables.

KSD record dates are not treated as split execution dates. Relisting and reference-price evidence must confirm a unit change. Unresolved split adjustment events block publication instead of guessing a factor. An explicit open-ended KSD halt with complete zero-volume raw-session evidence can remain pending with an unknown execution date; it is rechecked on every refresh, and a resumed traded bar without date/reference proof blocks publication. Alpaca corporate action requests explicitly cover forward/reverse splits. Other distributions, mergers and symbol-history splicing are outside this split-only data contract. Provider event namespace/ID/revision prevent duplicate application and support corrections. Invalid batches and incomplete required benchmark sessions block publication.

The universe still has collection-time active/survivorship limitations. This pipeline does not manufacture historical delisted constituents, point-in-time capitalizations or missing security classifications. Provider subscriptions, permissions, availability and correction windows remain external dependencies. Check the Actions run log for health; the presence of a local checkpoint does not prove the latest scheduled run succeeded.

KRX bulk collection uses a conservative next-calendar-day 08:15 Seoul eligibility policy, separately from exchange close. This is an operating cutoff, not a verified publication SLA. The Korean evening run does not claim same-day KRX confirmation. Previously collected sessions are retained; only the newest pending tail may wait, and historical holes or empty required responses still fail validation. A same-day provisional KIS full-universe feed is not included.

New dates and valid corrections are upserted by market/symbol/session into a copied working DB, then published as a new immutable full checkpoint. Old raw seed/evidence and previous checkpoints remain. Unchanged data does not create a new Release. Full snapshots increase storage and transfer volume with retained history and versions. An offline PC receives the newer checkpoint on its next explicit/startup pull; its local file is not updated while powered off.

## Local runtime

Python 3.13: `python -m pip install -r requirements-ingestion.txt`, then `python -m pytest tests -q`.
After a trusted operator bootstraps the first encrypted Release, run `python scripts/price_pipeline.py` with the required environment secrets. The included script never requests brokerage account access or submits orders.

A trusted successful run records minimal verified watermarks in .github/price-pipeline-status.json when the main-branch commit is at least25days old. This supplies a useful operating audit and repository activity before GitHub public-repository inactivity rules can disable schedules; the record contains no keys or database contents.
