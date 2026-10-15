import base64
import importlib.util
import json
from pathlib import Path
import pytest


def module():
    assert importlib.util.find_spec('price_lab.cloud_data'), 'authenticated checkpoint transport missing'
    from price_lab import cloud_data
    return cloud_data


def checkpoint(tmp_path):
    from test_ingestion import seed,FakeProvider
    from price_lab.ingestion import refresh,resolve_current
    root=tmp_path/'source';source=seed(tmp_path/'seed.duckdb')
    refresh(FakeProvider(),seed_path=source,supplement_path=None,root=root,markets=('us',),now='2024-01-05T22:00Z')
    return resolve_current(root)


def test_encrypted_checkpoint_roundtrip_and_wrong_key(tmp_path):
    c=module();current=checkpoint(tmp_path);key=base64.urlsafe_b64encode(b'x'*32).decode();out=tmp_path/'checkpoint.bin'
    public=c.encrypt_checkpoint(current['resolved_path'],current,out,key=key)
    assert b'DUCK' not in out.read_bytes()[:64]
    restored=c.decrypt_checkpoint(out,tmp_path/'restored',key=key)
    assert restored['sha256']==current['sha256'] and restored['sequence']==1
    assert 'resolved_path' not in public.get('manifest',{})
    with pytest.raises(ValueError):c.decrypt_checkpoint(out,tmp_path/'wrong',key=base64.urlsafe_b64encode(b'y'*32).decode())
    assert not (tmp_path/'wrong'/'current.json').exists()


def test_ciphertext_and_authenticated_metadata_tamper_fail(tmp_path):
    c=module();current=checkpoint(tmp_path);out=tmp_path/'checkpoint.bin';key=b'x'*32
    c.encrypt_checkpoint(current['resolved_path'],current,out,key=key)
    blob=bytearray(out.read_bytes());blob[-20]^=1;out.write_bytes(blob)
    with pytest.raises(ValueError):c.decrypt_checkpoint(out,tmp_path/'bad',key=key)
    assert not (tmp_path/'bad'/'current.json').exists()


def test_rollback_rejected_but_authenticated_multi_generation_lineage_allowed(tmp_path):
    c=module();current=checkpoint(tmp_path);key=b'x'*32
    one=tmp_path/'one.bin';c.encrypt_checkpoint(current['resolved_path'],current,one,key=key)
    local=tmp_path/'local';c.decrypt_checkpoint(one,local,key=key)
    third={**current,'sequence':3,'database':'checkpoints/000000000003.duckdb','parent_sha256':'a'*64,'ancestors':['a'*64,current['sha256']]}
    # A newer DB is needed: a content SHA must never be its own ancestor.
    import duckdb
    db_path=tmp_path/'third.duckdb';db_path.write_bytes(Path(current['resolved_path']).read_bytes())
    with duckdb.connect(str(db_path)) as db:db.execute("CREATE TABLE generation_three AS SELECT 3 n");db.execute('CHECKPOINT')
    from price_lab.ingestion import sha256_file
    third['sha256']=sha256_file(db_path)
    three=tmp_path/'three.bin';c.encrypt_checkpoint(db_path,third,three,key=key)
    assert c.decrypt_checkpoint(three,local,key=key)['sequence']==3
    with pytest.raises(ValueError,match='rollback'):c.decrypt_checkpoint(one,local,key=key)


def test_unrelated_lineage_and_weak_keys_rejected(tmp_path):
    c=module();current=checkpoint(tmp_path)
    with pytest.raises(ValueError,match='key'):c.encrypt_checkpoint(current['resolved_path'],current,tmp_path/'weak',key='short')


def test_plain_header_is_authenticated_and_restore_rejects_unrelated_lineage(tmp_path):
    c=module();current=checkpoint(tmp_path);key=b'x'*32;one=tmp_path/'one.bin'
    c.encrypt_checkpoint(current['resolved_path'],current,one,key=key)
    local=tmp_path/'local';c.decrypt_checkpoint(one,local,key=key)
    blob=one.read_bytes();assert b'"sequence":1' in blob
    one.write_bytes(blob.replace(b'"sequence":1',b'"sequence":9',1))
    with pytest.raises(ValueError,match='authentication'):c.decrypt_checkpoint(one,tmp_path/'tampered',key=key)
    import duckdb
    from price_lab.ingestion import sha256_file
    new=tmp_path/'new.duckdb';new.write_bytes(Path(current['resolved_path']).read_bytes())
    with duckdb.connect(str(new)) as db:db.execute('CREATE TABLE different AS SELECT 2 n');db.execute('CHECKPOINT')
    manifest={**current,'sequence':2,'database':'checkpoints/000000000002.duckdb','sha256':sha256_file(new),'parent_sha256':'b'*64,'ancestors':['b'*64]}
    c.encrypt_checkpoint(new,manifest,one,key=key)
    with pytest.raises(ValueError,match='lineage'):c.decrypt_checkpoint(one,local,key=key)
