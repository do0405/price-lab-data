# Encrypted daily price changes

The cloud is a stateless collector. It does not create, restore, cache, publish, or retain a DuckDB database. The research application's single canonical DuckDB, historical archives, split adjustments, security history, validation, and backtests remain local. No periodic full backup is scheduled.

## Schedule and access

The main-branch workflow collects at 19:37 America/New_York (08:37/09:37 next day in Korea, following DST) and 20:37 Asia/Seoul. Weekends remain enabled for crypto. GitHub may delay scheduled runs. A serialized check at minute 17 every six hours UTC verifies the latest encrypted package without collecting prices or opening a database.

Credentials belong only in existing Actions Secrets: ALPACA_API_KEY, ALPACA_API_SECRET, KRX_API_KEY, KIS_APP_KEY, KIS_APP_SECRET, PRICE_DATA_KEY. The data key is a random 32-byte URL-safe base64 key. No broker account identifier is needed. Trusted main-branch scheduled/manual runs alone receive secrets; checkout does not persist credentials.

The cloud runtime and its CI use requirements-delta.txt, which contains no database engine. Database-based application regression tests run locally. CI uses only DB-free codec, collection, and entrypoint contract tests.

## Collection and transfer

Alpaca supplies US/crypto bars, KRX supplies Korean daily bulk bars, and KIS supplies Korean benchmark bars and corporate-action observations. Each collection requests missing sessions plus the latest five completed sessions to discover corrections. A Sunday audit rechecks one bounded historical window of at most twenty sessions per market, advancing its control cursor. It never downloads a historical database. There is no rolling deletion at 750 sessions.

The first run verifies the HMAC on config/daily-bootstrap.json. This small non-DB control contains a preserved local anchor identity, observed symbol directory, actual completed-session labels, watermarks, and calendar evidence. Later runs obtain this state from the latest authenticated package. The control is not a price database or a historical investment universe.

Each immutable Release contains only daily.pldelta: canonical JSON records compressed with gzip and encrypted/authenticated with AES-256-GCM in a separate PLDELTA1 domain. Schema, stream, sequence, predecessor package hash, local bootstrap anchor, and collection time bind the records. Header size is bounded at 4KiB and uncompressed payload at 512MiB. Authentication completes before JSON parsing or decompression. Packages contain raw bars, asset observations, corporate actions, requested/completed sessions, action-query boundaries, and bounded official Korean capitalization/security-master responses. API keys and authorization headers are forbidden fields.

A package is made ready only after the encrypted asset is uploaded. A failed price/action/required-benchmark request does not advance collection state. Reference failures have a separate fixed status and do not erase successful price collection. Reference dates and retrieval times do not certify historical publication timing or complete security lifecycles.

Korean KRX daily collection keeps the conservative next-calendar-day 08:15 Seoul eligibility policy. The Korean evening slot does not claim same-day KRX confirmation. Official holiday evidence is distinct from provider publication delay. Missing responses do not imply market closure, delisting, or a zero recovery amount.

## Local application

The local app downloads missing packages in sequence, verifies ciphertext/content identity and continuity, and applies them to a copy of its freshest local parent. The existing raw-preserving ingestion and split-confirmation rules build the next immutable DuckDB. All parts are staged before corporate actions are evaluated; a later package can supply the evidence needed to resolve an earlier unconfirmed action. Applied-package receipts and the resulting data commit together only after validation. Tampering, gaps, unconfirmed actions, or database validation failures leave the prior app database and cursor intact. Exact replays are no-ops.

US/crypto turnover is provider VWAP times volume; Korean turnover is the provider's actual traded value. Split-adjusted turnover is value invariant. KSD record dates are not treated as execution dates, and price jumps do not manufacture split factors. Ordinary dividend reinvestment and complete historical security lifecycles are outside this split-only price contract.

A PC that was off catches up from retained encrypted packages when it next starts. Local-only unfinished backfills cannot be read while that PC is off. They are validated and merged locally on the next six-hour pass; complete candidate databases are never uploaded. Collection success and local application success are separate states.

Existing full-checkpoint Releases belong to the retired transport and are removed only after local preservation and migration validation. There is no automatic fallback to them. Do not prune daily packages automatically: a missing package breaks catch-up for offline clients. A new device needs a locally preserved baseline; the cloud contains no baseline database. Protect the encryption key independently.
