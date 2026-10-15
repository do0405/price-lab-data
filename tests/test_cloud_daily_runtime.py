"""Production cloud entrypoint is testable without database libraries or files."""
import ast
import importlib.util
from pathlib import Path
import pytest


def pipeline_module():
    path=Path(__file__).resolve().parents[1]/'scripts/price_pipeline.py'
    spec=importlib.util.spec_from_file_location('cloud_daily_pipeline',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


def test_six_hour_check_reads_only_authenticated_latest_package(monkeypatch,tmp_path):
    from price_lab import daily_delta_collect as c,daily_delta_transport as t
    events=[]
    monkeypatch.setattr(t,'newest_package',lambda:events.append('discover') or {'id':1})
    monkeypatch.setattr(t,'load_package',lambda *a,**k:events.append('authenticate') or {'sequence':7})
    monkeypatch.setattr(t,'record_periodic_status',lambda *a,**k:{'recorded':False})
    def forbidden(*a,**k):raise AssertionError('Six-hour check collected or published data')
    monkeypatch.setattr(c,'collect_delta',forbidden);monkeypatch.setattr(t,'publish_package',forbidden)
    result=pipeline_module().run_pipeline(root=tmp_path,integration_only=True)
    assert events==['discover','authenticate'] and result['sequence']==7
    assert result['cloud_database'] is False and not list(tmp_path.rglob('*.duckdb'))


def test_cloud_daily_collects_then_publishes_without_restoring_any_database(monkeypatch,tmp_path):
    from price_lab import daily_delta_collect as c,daily_delta_transport as t
    events=[]
    monkeypatch.setattr(t,'newest_package',lambda:{'id':1})
    monkeypatch.setattr(t,'load_package',lambda *a,**k:{'sequence':6})
    def collect(provider,**kwargs):
        events.append('collect');assert kwargs['prior_payload']=={'sequence':6}
        return {'sequence':7,'parts':[]}
    monkeypatch.setattr(c,'collect_delta',collect)
    monkeypatch.setattr(t,'publish_package',lambda *a,**k:events.append('publish') or {'sequence':7})
    monkeypatch.setattr(t,'record_periodic_status',lambda *a,**k:{'recorded':False})
    result=pipeline_module().run_pipeline(root=tmp_path,providers=object(),reference_fetch=lambda *a:{})
    assert events==['collect','publish'] and result['cloud_database'] is False
    assert not list(tmp_path.rglob('*.duckdb'))


def test_retired_full_publication_cannot_be_requested(monkeypatch,tmp_path):
    from price_lab import daily_delta_transport as t
    monkeypatch.setattr(t,'newest_package',lambda:None)
    with pytest.raises(ValueError,match='retired'):
        pipeline_module().run_pipeline(root=tmp_path,publish_only=True)


def test_production_collector_imports_no_database_runtime():
    root=Path(__file__).resolve().parents[1]
    for relative in ('scripts/price_pipeline.py','src/price_lab/daily_delta_collect.py','src/price_lab/daily_delta_codec.py'):
        tree=ast.parse((root/relative).read_text(encoding='utf-8'))
        names=[n.module or '' for n in ast.walk(tree) if isinstance(n,ast.ImportFrom)]
        names += [a.name for n in ast.walk(tree) if isinstance(n,ast.Import) for a in n.names]
        assert not any(any(term in name for term in ('duckdb','ingestion','cloud_data','history_integration','history_transport','release_transport')) for name in names)
