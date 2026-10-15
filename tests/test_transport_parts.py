import copy
import hashlib
import hmac
import importlib.util
import json
import struct

import pytest


KEY=b'x'*32


def module():
    assert importlib.util.find_spec('price_lab.transport_parts'), 'authenticated multipart transport missing'
    from price_lab import transport_parts
    return transport_parts


def manifest():
    return dict(schema_version=1,key_version=1,sequence=2,database='checkpoints/000000000002.duckdb',
                sha256='b'*64,parent_sha256='a'*64,ancestors=['a'*64],includes_supplement=True,
                markets={'us':{'actual':'2024-01-05','target':'2024-01-05'}})


def container(tmp_path):
    from price_lab.cloud_data import MAGIC
    from price_lab.ingestion import canonical
    m=manifest();header=canonical(m).encode();path=tmp_path/'checkpoint.pldata'
    path.write_bytes(MAGIC+struct.pack('>I',len(header))+header+b'n'*12+b'ciphertext'*24+b't'*16)
    return path,m


def resign(value):
    from price_lab.ingestion import canonical
    value=copy.deepcopy(value);value.pop('hmac_sha256',None)
    derived=hmac.new(KEY,b'price-lab/transport-index/key/v1',hashlib.sha256).digest()
    value['hmac_sha256']=hmac.new(derived,b'price-lab/transport-index/payload/v1\0'+canonical(value).encode(),hashlib.sha256).hexdigest()
    return value


def test_streaming_split_assemble_and_authenticated_identity(tmp_path):
    t=module();source,m=container(tmp_path);directory=tmp_path/'parts'
    index=t.split_checkpoint(source,m,directory,key=KEY,part_size=97)
    assert len(index['parts'])>2 and all(0<p['size']<=97 for p in index['parts'])
    assert index['checkpoint']==m and 'resolved_path' not in index['checkpoint']
    assert index['ciphertext_sha256']==hashlib.sha256(source.read_bytes()).hexdigest()
    assert t.read_index(directory/t.INDEX_NAME,key=KEY,current={'sequence':1,'sha256':'a'*64})==index
    output=t.assemble_checkpoint(index,directory,tmp_path/'joined',key=KEY,current={'sequence':1,'sha256':'a'*64})
    assert output.read_bytes()==source.read_bytes()


@pytest.mark.parametrize('mutation',['manifest','order','missing','duplicate','name','count','total','size','digest'])
def test_tampered_index_is_rejected_before_assembly(tmp_path,mutation):
    t=module();source,m=container(tmp_path);index=t.split_checkpoint(source,m,tmp_path/'parts',key=KEY,part_size=97)
    bad=copy.deepcopy(index)
    if mutation=='manifest':bad['checkpoint']['sequence']=5
    elif mutation=='order':bad['parts'].reverse()
    elif mutation=='missing':bad['parts'].pop()
    elif mutation=='duplicate':bad['parts'][1]=copy.deepcopy(bad['parts'][0])
    elif mutation=='name':bad['parts'][0]['name']='../outside'
    elif mutation=='count':bad['parts']*=t.MAX_PARTS+1
    elif mutation=='total':bad['total_size']=t.MAX_TOTAL_BYTES+1
    elif mutation=='size':bad['parts'][0]['size']=t.PART_BYTES+1
    elif mutation=='digest':bad['ciphertext_sha256']='0'*64
    with pytest.raises(ValueError):t.assemble_checkpoint(bad,tmp_path/'parts',tmp_path/'joined',key=KEY)
    assert not (tmp_path/'joined').exists()


@pytest.mark.parametrize('mutation',['order','name','count','total','size'])
def test_even_authenticated_malformed_layout_is_rejected(tmp_path,mutation):
    t=module();source,m=container(tmp_path);bad=t.split_checkpoint(source,m,tmp_path/'parts',key=KEY,part_size=97)
    if mutation=='order':bad['parts'].reverse()
    elif mutation=='name':bad['parts'][0]['name']='C:outside'
    elif mutation=='count':bad['parts']*=t.MAX_PARTS+1
    elif mutation=='total':bad['total_size']=t.MAX_TOTAL_BYTES+1
    elif mutation=='size':bad['parts'][0]['size']=True
    with pytest.raises(ValueError):t.verify_index(resign(bad),key=KEY)


def test_wrong_key_duplicate_json_and_part_corruption_cannot_produce_output(tmp_path):
    t=module();source,m=container(tmp_path);directory=tmp_path/'parts';index=t.split_checkpoint(source,m,directory,key=KEY,part_size=97)
    with pytest.raises(ValueError):t.verify_index(index,key=b'y'*32)
    duplicate=tmp_path/'duplicate.json';duplicate.write_text('{"format":1,"format":2}',encoding='utf-8')
    with pytest.raises(ValueError):t.read_index(duplicate,key=KEY)
    first=directory/index['parts'][0]['name'];first.write_bytes(b'z'*first.stat().st_size)
    with pytest.raises(ValueError):t.assemble_checkpoint(index,directory,tmp_path/'joined',key=KEY)
    assert not (tmp_path/'joined').exists()


def test_missing_part_full_digest_and_container_identity_are_verified(tmp_path):
    t=module();source,m=container(tmp_path);directory=tmp_path/'parts';index=t.split_checkpoint(source,m,directory,key=KEY,part_size=97)
    first=directory/index['parts'][0]['name'];saved=first.read_bytes();first.unlink()
    with pytest.raises(ValueError):t.assemble_checkpoint(index,directory,tmp_path/'joined',key=KEY)
    first.write_bytes(saved)
    bad=resign({**index,'ciphertext_sha256':'0'*64})
    with pytest.raises(ValueError):t.assemble_checkpoint(bad,directory,tmp_path/'joined',key=KEY)
    bad=resign({**index,'checkpoint':{**m,'sha256':'c'*64}})
    with pytest.raises(ValueError):t.assemble_checkpoint(bad,directory,tmp_path/'joined',key=KEY)
    assert not (tmp_path/'joined').exists()


def test_authenticated_rollback_fork_and_skipped_generations(tmp_path):
    t=module();source,m=container(tmp_path);index=t.split_checkpoint(source,m,tmp_path/'parts',key=KEY,part_size=97)
    with pytest.raises(ValueError,match='rollback'):t.verify_index(index,key=KEY,current={'sequence':3,'sha256':'c'*64})
    with pytest.raises(ValueError,match='lineage'):t.verify_index(index,key=KEY,current={'sequence':1,'sha256':'c'*64})
    with pytest.raises(ValueError,match='conflict'):t.verify_index(index,key=KEY,current={'sequence':2,'sha256':'c'*64})
    ahead=resign({**index,'checkpoint':{**m,'sequence':4,'parent_sha256':'d'*64,'ancestors':['d'*64,'a'*64]}})
    assert t.verify_index(ahead,key=KEY,current={'sequence':1,'sha256':'a'*64})['checkpoint']['sequence']==4


def test_truncated_source_stops_instead_of_looping_and_cleans_partial_files(tmp_path,monkeypatch):
    import io
    from pathlib import Path
    t=module();source,m=container(tmp_path);blob=source.read_bytes();original=Path.open;reads=0
    class Truncated(io.BytesIO):
        ended=False
        def read(self,size=-1):
            value=super().read(size)
            if not value:
                if self.ended:raise AssertionError('repeated reads after truncated source EOF')
                self.ended=True
            return value
    def open_path(path,mode='r',*args,**kwargs):
        nonlocal reads
        if path==source and mode=='rb':
            reads+=1
            if reads==2:return Truncated(blob[:-1])
        return original(path,mode,*args,**kwargs)
    monkeypatch.setattr(Path,'open',open_path)
    with pytest.raises(ValueError,match='changed'):t.split_checkpoint(source,m,tmp_path/'parts',key=KEY,part_size=97)
    assert not list((tmp_path/'parts').iterdir())


def test_oversized_index_is_bounded_before_parsing(tmp_path,monkeypatch):
    t=module();monkeypatch.setattr(t,'MAX_INDEX_BYTES',64);path=tmp_path/'index.json';path.write_bytes(b'x'*65)
    with pytest.raises(ValueError,match='size limit'):t.read_index(path,key=KEY)
