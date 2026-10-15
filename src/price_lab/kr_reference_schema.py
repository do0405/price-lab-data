"""Isolated KR reference contract. No price tables or active database are opened."""

SCHEMA_VERSION = 'kr-reference-v1'
AVAILABILITY_BASES = ('EFFECTIVE_DATE_RECONSTRUCTED', 'PUBLICATION_TIME_VERIFIED', 'UNKNOWN')
BOARDS = ('KOSPI', 'KOSDAQ')
REFERENCE_TABLES = (
    'kr_reference_schema', 'kr_source_manifest', 'kr_capitalization_observations',
    'kr_security_aliases', 'kr_listing_events', 'kr_settlements',
    'kr_reference_receipts', 'kr_reference_coverage',
)

_PROVENANCE = """
source_id VARCHAR NOT NULL, source_row_id VARCHAR NOT NULL, revision_id VARCHAR NOT NULL,
manifest_id VARCHAR NOT NULL, retrieved_at TIMESTAMPTZ NOT NULL,
source_published_at TIMESTAMPTZ, research_available_at TIMESTAMPTZ, research_policy_version VARCHAR,
availability_basis VARCHAR NOT NULL CHECK (availability_basis IN
('EFFECTIVE_DATE_RECONSTRUCTED','PUBLICATION_TIME_VERIFIED','UNKNOWN'))
"""


def create_schema(db):
    """Create only kr_* tables on a caller-owned DuckDB connection (idempotent)."""
    present={r[0] for r in db.execute('SHOW TABLES').fetchall()}
    if set(REFERENCE_TABLES).issubset(present):
        if db.execute('SELECT version FROM kr_reference_schema').fetchall()!=[(SCHEMA_VERSION,)]:
            raise ValueError('Unsupported KR reference schema version')
        return SCHEMA_VERSION
    db.execute('CREATE TABLE IF NOT EXISTS kr_reference_schema(version VARCHAR PRIMARY KEY)')
    versions = [r[0] for r in db.execute('SELECT version FROM kr_reference_schema').fetchall()]
    if versions and versions != [SCHEMA_VERSION]:
        raise ValueError('Unsupported KR reference schema version')
    db.execute('INSERT INTO kr_reference_schema VALUES (?) ON CONFLICT DO NOTHING', [SCHEMA_VERSION])
    db.execute("""CREATE TABLE IF NOT EXISTS kr_source_manifest (
        manifest_id VARCHAR PRIMARY KEY, source_id VARCHAR NOT NULL, source_url VARCHAR NOT NULL,
        raw_sha256 VARCHAR NOT NULL, raw_path VARCHAR, requested_date DATE, board VARCHAR,
        retrieved_at TIMESTAMPTZ NOT NULL, source_published_at TIMESTAMPTZ,
        availability_basis VARCHAR NOT NULL, record_count BIGINT NOT NULL,
        completeness VARCHAR NOT NULL, metadata_json VARCHAR NOT NULL)""")
    db.execute(f"""CREATE TABLE IF NOT EXISTS kr_capitalization_observations (
        {_PROVENANCE}, asof_date DATE NOT NULL,
        board VARCHAR NOT NULL CHECK(board IN ('KOSPI','KOSDAQ')),
        raw_symbol VARCHAR NOT NULL, raw_close DECIMAL(38,8), market_cap_krw DECIMAL(38,0),
        listed_shares BIGINT CHECK(listed_shares>=0), currency VARCHAR NOT NULL CHECK(currency='KRW'),
        quality_status VARCHAR NOT NULL, raw_fields_json VARCHAR NOT NULL, null_reasons_json VARCHAR NOT NULL,
        PRIMARY KEY(source_id,source_row_id,revision_id))""")
    db.execute(f"""CREATE TABLE IF NOT EXISTS kr_security_aliases (
        {_PROVENANCE}, alias_id VARCHAR NOT NULL, security_id VARCHAR NOT NULL, listing_id VARCHAR NOT NULL,
        isin VARCHAR NOT NULL, raw_symbol VARCHAR NOT NULL, name VARCHAR, board VARCHAR NOT NULL,
        security_type VARCHAR NOT NULL, listing_date DATE, effective_from DATE NOT NULL, effective_to DATE,
        identity_status VARCHAR NOT NULL, evidence_url VARCHAR, raw_fields_json VARCHAR NOT NULL,
        CHECK(effective_to IS NULL OR effective_to>effective_from), PRIMARY KEY(alias_id,revision_id))""")
    db.execute(f"""CREATE TABLE IF NOT EXISTS kr_listing_events (
        {_PROVENANCE}, event_id VARCHAR NOT NULL, security_id VARCHAR, listing_id VARCHAR,
        raw_symbol VARCHAR NOT NULL, board VARCHAR NOT NULL, event_type VARCHAR NOT NULL,
        announced_at TIMESTAMPTZ, effective_date DATE, last_trade_date DATE,
        status VARCHAR NOT NULL, supersedes_event_id VARCHAR, evidence_url VARCHAR NOT NULL,
        raw_fields_json VARCHAR NOT NULL, PRIMARY KEY(event_id,revision_id))""")
    db.execute(f"""CREATE TABLE IF NOT EXISTS kr_settlements (
        {_PROVENANCE}, settlement_id VARCHAR NOT NULL, event_id VARCHAR NOT NULL, security_id VARCHAR,
        kind VARCHAR NOT NULL, status VARCHAR NOT NULL, payment_date DATE,
        cash_per_raw_share DECIMAL(38,8), exchange_ratio DECIMAL(38,12), target_security_id VARCHAR,
        currency VARCHAR, raw_share_basis VARCHAR, effective_date DATE, evidence_url VARCHAR NOT NULL,
        raw_fields_json VARCHAR NOT NULL, PRIMARY KEY(settlement_id,revision_id))""")
    db.execute("""CREATE TABLE IF NOT EXISTS kr_reference_receipts (
        receipt_id VARCHAR PRIMARY KEY, committed_at TIMESTAMPTZ NOT NULL,
        schema_version VARCHAR NOT NULL, delta_sha256 VARCHAR NOT NULL, row_count BIGINT NOT NULL)""")
    db.execute("""CREATE TABLE IF NOT EXISTS kr_reference_coverage (
        scope VARCHAR NOT NULL, asof_date DATE NOT NULL, board VARCHAR NOT NULL, item VARCHAR NOT NULL,
        expected BIGINT, received BIGINT NOT NULL, unknown BIGINT NOT NULL, conflicts BIGINT NOT NULL,
        status VARCHAR NOT NULL, validation_version VARCHAR NOT NULL, computed_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY(scope,asof_date,board,item))""")
    return SCHEMA_VERSION
