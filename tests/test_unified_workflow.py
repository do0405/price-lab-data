import importlib.util
from pathlib import Path


def pipeline_module():
    path=Path(__file__).resolve().parents[1]/'scripts/price_pipeline.py'
    spec=importlib.util.spec_from_file_location('public_pipeline_contract',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


def test_daily_direct_child_is_published_before_candidate_merge(monkeypatch,tmp_path):
    from price_lab import ingestion,release_transport,providers,history_transport,kr_reference_refresh
    events=[]
    monkeypatch.setattr(release_transport,'pull_latest',lambda *a,**k:events.append('pull-A'))
    monkeypatch.setattr(ingestion,'refresh',lambda *a,**k:(events.append('refresh-B') or {'status':'PUBLISHED'}))
    monkeypatch.setattr(ingestion,'resolve_current',lambda *a,**k:{'sequence':2,'markets':{}})
    published=iter(('publish-B','publish-C'))
    monkeypatch.setattr(release_transport,'publish_current',lambda *a,**k:(events.append(next(published)) or {}))
    monkeypatch.setattr(release_transport,'record_periodic_status',lambda *a,**k:{})
    monkeypatch.setattr(kr_reference_refresh,'refresh_recent_reference',lambda *a,**k:(events.append('reference-C') or {'status':'PUBLISHED'}))
    monkeypatch.setattr(history_transport,'process_pending',lambda *a,**k:(events.append('merge-publish-D') or []))
    monkeypatch.setattr(providers,'Providers',lambda **kwargs:object())
    pipeline_module().run_pipeline(root=tmp_path)
    assert events==['pull-A','refresh-B','publish-B','reference-C','publish-C','merge-publish-D']


def test_six_hour_monitor_does_not_recollect_all_daily_prices(monkeypatch,tmp_path):
    from price_lab import ingestion,release_transport,history_transport,kr_reference_refresh
    monkeypatch.setattr(release_transport,'pull_latest',lambda *a,**k:{})
    monkeypatch.setattr(ingestion,'resolve_current',lambda *a,**k:{'sequence':2,'markets':{}})
    monkeypatch.setattr(release_transport,'publish_current',lambda *a,**k:{})
    monkeypatch.setattr(release_transport,'record_periodic_status',lambda *a,**k:{})
    monkeypatch.setattr(history_transport,'process_pending',lambda *a,**k:[])
    def unexpected(*args,**kwargs):raise AssertionError('integration monitor fetched daily prices')
    monkeypatch.setattr(ingestion,'refresh',unexpected)
    monkeypatch.setattr(kr_reference_refresh,'refresh_recent_reference',unexpected)
    pipeline_module().run_pipeline(root=tmp_path,integration_only=True)
