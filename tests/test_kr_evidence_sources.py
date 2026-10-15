import ast
import importlib
from pathlib import Path

import pytest


def source():return importlib.import_module('price_lab.kr_evidence_sources')


def native_list(rows=None,total=None):
    rows=rows if rows is not None else [('1','Example','000001','2024-01-15','합병','')]
    header='<tr>'+''.join('<th>'+v+'</th>' for v in ('번호','회사명','종목코드','폐지일자','폐지사유','비고'))+'</tr>'
    body=''.join('<tr>'+''.join('<td>'+v+'</td>' for v in row)+'</tr>' for row in rows)
    return ('<html><table>'+header+body+'</table>'+('전체 '+str(total)+' 건' if total is not None else '')+'</html>').encode()


def test_pure_helpers_have_no_private_runtime_imports():
    for name in ('kr_evidence_sources.py','research_delta_collect.py'):
        tree=ast.parse((Path(__file__).resolve().parents[1]/'src'/'price_lab'/name).read_text(encoding='utf-8'))
        imports=[n.module or '' for n in ast.walk(tree) if isinstance(n,ast.ImportFrom)]
        imports += [a.name for n in ast.walk(tree) if isinstance(n,ast.Import) for a in n.names]
        assert not any(any(term in value for term in ('duckdb','ingestion','local_credentials','kr_reference_sources','kr_reference_schema')) for value in imports)


def test_native_kind_list_preserves_six_character_code_and_unknown_terms():
    s=source();task=s.kind_list_task('KOSPI','2010-01-01','2024-01-31')
    assert task['params']['method']=='searchDelCompanySub' and task['params']['forward']=='delcompany_down'
    rows=s.kind_list_rows(native_list(),task)
    assert rows[0]['event_id']=='KIND:KOSPI:000001:2024-01-15'
    assert rows[0]['raw_symbol']=='000001' and rows[0]['settlement_status']=='UNKNOWN'
    assert rows[0]['reason']=='합병'


@pytest.mark.parametrize('body',[
    native_list([('2','x','000001','2024-01-15','합병','')]),
    native_list([('1','x','00001','2024-01-15','합병','')]),
    native_list([('1','x','000001','2025-01-15','합병','')]),
    native_list([('2','x','000001','2024-01-15','합병',''),('1','x','000001','2024-01-15','다른사유','')]),
    native_list([],None),native_list(total=2),b'<html>blocked</html>',
])
def test_incomplete_conflicting_or_unknown_kind_list_never_completes(body):
    with pytest.raises(ValueError):source().kind_list_rows(body,source().kind_list_task('KOSPI','2010-01-01','2024-01-31'))


def test_source_task_allowlist_rejects_destination_auth_and_nonofficial_urls():
    s=source();valid=s.krx_task('KOSPI','master','2024-01-15');s.validate_task(valid)
    for changed in ({**valid,'headers':{'AUTH_KEY':'forbidden'}},{**valid,'url':'https://elsewhere.example/file'},
                    {**valid,'requested_date':'2024-01-16'},{**valid,'path':'x.duckdb'}):
        with pytest.raises(ValueError):s.validate_task(changed)
