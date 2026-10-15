import copy
import json

import pytest

from price_lab import research_delta_transport as wire
from price_lab.research_delta_codec import package_digest
from test_research_delta_codec import payload
from test_research_delta_transport import release


def head(sequence, digest):
    return dict(sequence=sequence, package_sha256=digest)


def test_exact_canonical_head_ignores_newer_ready_orphan(monkeypatch):
    committed = release(2, 'b' * 64, 'a' * 64)
    paths = []
    def api(method, path, **kwargs):
        paths.append(path)
        assert path == '/releases/tags/' + committed['tag_name']
        return committed
    monkeypatch.setattr(wire, '_api', api)
    assert wire.canonical_newest(head(2, 'b' * 64)) == committed
    assert len(paths) == 1
    assert wire.canonical_newest(None) is None


def test_canonical_ancestry_excludes_orphan_branches(monkeypatch):
    first = release(1, 'a' * 64)
    second = release(2, 'b' * 64, 'a' * 64)
    third = release(3, 'c' * 64, 'b' * 64)
    rows = [release(4, 'f' * 64, 'e' * 64), release(3, 'e' * 64, 'd' * 64),
            third, release(2, 'd' * 64, 'a' * 64), second, first]
    monkeypatch.setattr(wire, '_api', lambda *a, **k: rows)
    assert wire.canonical_packages(head(3, 'c' * 64)) == [first, second, third]
    assert wire.canonical_packages(head(3, 'c' * 64), after_sequence=1,
        expected_cursor_hash='a' * 64) == [second, third]
    assert wire.canonical_packages(head(3, 'c' * 64), after_sequence=3,
        expected_cursor_hash='c' * 64) == []


@pytest.mark.parametrize('rows,tip,after,cursor', [
    ([release(1, 'a' * 64)], head(2, 'b' * 64), 0, None),
    ([release(3, 'c' * 64, 'b' * 64), release(1, 'a' * 64)], head(3, 'c' * 64), 0, None),
    ([release(2, 'b' * 64, 'e' * 64), release(1, 'a' * 64)], head(2, 'b' * 64), 1, 'a' * 64),
    ([release(1, 'a' * 64)], head(1, 'a' * 64), 2, 'b' * 64),
    ([release(1, 'a' * 64)], head(1, 'a' * 64), 1, 'b' * 64),
    ([], None, 1, 'a' * 64),
])
def test_canonical_rejects_missing_head_gaps_rollback_cursor_mismatch(monkeypatch, rows, tip, after, cursor):
    monkeypatch.setattr(wire, '_api', lambda *a, **k: rows)
    with pytest.raises(ValueError):
        wire.canonical_packages(tip, after_sequence=after, expected_cursor_hash=cursor)


def test_canonical_ancestry_searches_later_listing_pages(monkeypatch):
    one = release(1, 'a' * 64)
    two = release(2, 'b' * 64, 'a' * 64)
    calls = []
    def api(method, path, **kwargs):
        calls.append(path)
        return [two] + [{'tag_name': 'unrelated'}] * 99 if path.endswith('&page=1') else [one]
    monkeypatch.setattr(wire, '_api', api)
    assert wire.canonical_packages(head(2, 'b' * 64)) == [one, two]
    assert len(calls) == 2


def test_controlled_publish_preflight_uses_canonical_parent_only(tmp_path, monkeypatch):
    from price_lab import research_ownership as ownership
    from test_research_ownership import Clock, Contents, KEY
    first = payload()
    first['records'] = []
    first_digest = package_digest(first)
    next_payload = copy.deepcopy(first)
    next_payload.update(sequence=2, previous_package_sha256=first_digest, collected_at='2026-10-16T00:00:00Z')
    orphan = release(2, 'd' * 64, first_digest)
    committed = release(1, first_digest)
    store = Contents()
    clock = Clock()
    monkeypatch.setattr(ownership, '_api', store.api)
    old = ownership.ResearchOwner('cloud', owner_id='old', key=KEY, clock=clock)
    assert old.acquire(initial_head=head(1, first_digest), anchor_sha256='a' * 64)
    clock.advance(600)
    new = ownership.ResearchOwner('local', owner_id='new', key=KEY, clock=clock)
    assert new.acquire()
    releases = {committed['tag_name']: committed, orphan['tag_name']: orphan}
    draft = {}
    def api(method, path, **kwargs):
        if method == 'GET':
            if path.startswith('/releases/tags/'):
                tag = path.rsplit('/', 1)[1]
                if tag not in releases:
                    raise RuntimeError('HTTP 404')
                return releases[tag]
            raise AssertionError('Controlled publishing must not discover legacy ready tip')
        data = kwargs.get('payload')
        if path == '/releases':
            draft.update(data, id=42, assets=[], upload_url='https://uploads.github.com/new{?name}', html_url='https://github.com/new')
            return dict(draft)
        if method == 'POST':
            size = len(kwargs['stream'].read())
            tag = draft['tag_name']
            asset = dict(id=42, name=wire.ASSET, size=size,
                browser_download_url=f'https://github.com/{wire.REPOSITORY}/releases/download/{tag}/{wire.ASSET}')
            draft['assets'] = [asset]
            return asset
        draft.update(data)
        if draft.get('draft') is False:
            releases[draft['tag_name']] = dict(draft)
        return dict(draft)
    monkeypatch.setattr(wire, '_api', api)
    monkeypatch.setattr(wire, 'newest_package', lambda **k: (_ for _ in ()).throw(AssertionError('legacy tip')))
    result = wire.publish_package(next_payload, key=b'x' * 32, work_root=tmp_path,
        expected_head=new.head)
    assert result['sequence'] == 2
    assert result['package_sha256'] != 'd' * 64
    assert orphan['tag_name'] in releases
    assert new.commit_head(next_payload) == head(2, result['package_sha256'])
    with pytest.raises(ownership.OwnershipLost):
        old.commit_head(next_payload)
    monkeypatch.setattr(wire, '_api', lambda *a, **k: list(releases.values()))
    selected = wire.canonical_packages(ownership.read_control(key=KEY)['head'])
    assert [json.loads(r['body'])['package_sha256'] for r in selected] == [first_digest, result['package_sha256']]


def test_controlled_publish_rejects_stale_parent_even_for_existing_ready_package(monkeypatch):
    first = payload()
    digest = package_digest(first)
    ready = release(1, digest)
    ready['html_url'] = 'https://github.com/example'
    monkeypatch.setattr(wire, '_api', lambda *a, **k: ready)
    with pytest.raises(ValueError):
        wire.publish_package(first, expected_head=head(1, digest))


def test_explicit_null_controlled_head_allows_bootstrap_without_legacy_scan(monkeypatch):
    first = payload()
    ready = release(1, package_digest(first))
    ready['html_url'] = 'https://github.com/example'
    monkeypatch.setattr(wire, '_api', lambda *a, **k: ready)
    monkeypatch.setattr(wire, 'newest_package', lambda **k: (_ for _ in ()).throw(AssertionError('legacy tip')))
    assert wire.publish_package(first, expected_head=None)['unchanged']


def test_canonical_discovery_fails_closed_at_page_bound(monkeypatch):
    monkeypatch.setattr(wire, 'MAX_PAGES', 2)
    calls = []
    def api(method, path, **kwargs):
        calls.append(path)
        return [{'tag_name': 'unrelated'}] * 100
    monkeypatch.setattr(wire, '_api', api)
    with pytest.raises(ValueError, match='bound'):
        wire.canonical_packages(head(1, 'a' * 64))
    assert len(calls) == 2


@pytest.mark.parametrize('kind', ['missing', 'draft', 'mismatch'])
def test_canonical_exact_head_requires_ready_matching_metadata(monkeypatch, kind):
    row = release(1, 'a' * 64)
    if kind == 'draft':
        row['draft'] = True
    if kind == 'mismatch':
        row = release(1, 'b' * 64)
    def api(*args, **kwargs):
        if kind == 'missing':
            raise RuntimeError('HTTP 404')
        return row
    monkeypatch.setattr(wire, '_api', api)
    with pytest.raises(ValueError):
        wire.canonical_newest(head(1, 'a' * 64))
