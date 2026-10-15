import base64
import copy
from datetime import datetime, timedelta, timezone
import importlib

import pytest

from price_lab.research_delta_codec import _canonical, package_digest, sign_control
from test_research_delta_codec import payload


KEY = b'x' * 32
ANCHOR = 'a' * 64


class Clock:
    def __init__(self):
        self.now = datetime(2026, 10, 16, tzinfo=timezone.utc)

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += timedelta(seconds=seconds)


class Contents:
    def __init__(self):
        self.content = None
        self.sha = 0
        self.history = False
        self.puts = 0
        self.reads = 0
        self.uncertain = False
        self.conflicts = 0

    def api(self, method, path, **kwargs):
        if path.startswith('/commits?'):
            return [{'sha': 'initialized'}] if self.history else []
        if method == 'GET':
            self.reads += 1
            if self.content is None:
                raise RuntimeError('HTTP 404')
            return dict(sha=str(self.sha), content=self.content)
        assert method == 'PUT'
        update = kwargs['payload']
        self.puts += 1
        if self.conflicts:
            self.conflicts -= 1
            raise RuntimeError('HTTP 409')
        if update.get('sha') != (str(self.sha) if self.content else None):
            raise RuntimeError('HTTP 409')
        self.content = update['content']
        self.sha += 1
        self.history = True
        if self.uncertain:
            self.uncertain = False
            raise RuntimeError('transport failed after durable write')
        return {}

    def set_body(self, body):
        self.content = base64.b64encode(_canonical(sign_control(body, KEY))).decode()
        self.sha += 1
        self.history = True


@pytest.fixture
def protocol(monkeypatch):
    ownership = importlib.import_module('price_lab.research_ownership')
    store = Contents()
    monkeypatch.setattr(ownership, '_api', store.api)
    clock = Clock()
    def owner(role, identity):
        return ownership.ResearchOwner(role, owner_id=identity, key=KEY, clock=clock)
    return ownership, store, clock, owner


def test_local_preference_excludes_cloud_until_exact_expiry(protocol):
    mod, store, clock, owner = protocol
    local = owner('local', 'local-1')
    assert local.acquire(anchor_sha256=ANCHOR)
    local.prefer_local()
    local.release()
    cloud = owner('cloud', 'cloud-1')
    assert not cloud.acquire()
    clock.advance(180)
    assert cloud.acquire()
    assert cloud.control['epoch'] == 2
    assert cloud.head is None


def test_preference_refresh_preserves_live_cloud_lease_and_head(protocol):
    mod, store, clock, owner = protocol
    initial = dict(sequence=2, package_sha256='b' * 64)
    cloud = owner('cloud', 'cloud-1')
    assert cloud.acquire(initial_head=initial, anchor_sha256=ANCHOR)
    before = mod.read_control(key=KEY)
    local = owner('local', 'local-1')
    local.prefer_local()
    after = mod.read_control(key=KEY)
    assert after['lease'] == before['lease']
    assert after['head'] == initial
    assert cloud.should_yield()
    assert not local.acquire()
    clock.advance(181)
    assert not cloud.should_yield()


def test_live_owner_exclusion_takeover_and_epoch_fence(protocol):
    mod, store, clock, owner = protocol
    old = owner('cloud', 'old')
    new = owner('local', 'new')
    assert old.acquire(anchor_sha256=ANCHOR)
    assert not new.acquire()
    clock.advance(600)
    with pytest.raises(mod.OwnershipLost):
        old.guard()
    assert new.acquire()
    assert new.control['epoch'] == 2
    with pytest.raises(mod.OwnershipLost):
        old.guard(force=True)
    with pytest.raises(mod.OwnershipLost):
        old.commit_head(payload())
    old.release()
    assert mod.read_control(key=KEY)['lease']['owner_id'] == 'new'


def test_guard_checks_at_sixty_seconds_and_never_renews_expired_lease(protocol):
    mod, store, clock, owner = protocol
    local = owner('local', 'local')
    local.acquire(anchor_sha256=ANCHOR)
    reads = store.reads
    clock.advance(59)
    local.guard()
    assert store.reads == reads
    clock.advance(1)
    local.guard()
    assert store.reads > reads
    assert datetime.fromisoformat(local.control['lease']['expires_at']) == clock.now + timedelta(seconds=600)
    clock.advance(600)
    puts = store.puts
    with pytest.raises(mod.OwnershipLost):
        local.guard(force=True)
    assert store.puts == puts


def test_force_guard_does_not_use_cached_lease(protocol):
    mod, store, clock, owner = protocol
    local = owner('local', 'local')
    local.acquire(anchor_sha256=ANCHOR)
    body = mod.read_control(key=KEY)
    body['epoch'] += 1
    body['revision'] += 1
    body['lease'].update(owner_id='replacement', epoch=body['epoch'])
    store.set_body(body)
    with pytest.raises(mod.OwnershipLost):
        local.guard(force=True)


def test_cas_conflicts_and_uncertain_acquisition_are_confirmed(protocol):
    mod, store, clock, owner = protocol
    store.conflicts = 1
    store.uncertain = True
    local = owner('local', 'local')
    assert local.acquire(anchor_sha256=ANCHOR)
    assert mod.read_control(key=KEY)['epoch'] == 1
    assert store.puts == 2


def test_head_commit_is_contiguous_anchor_bound_and_uncertain_retry_idempotent(protocol):
    mod, store, clock, owner = protocol
    local = owner('local', 'local')
    local.acquire(anchor_sha256=ANCHOR)
    first = payload()
    store.uncertain = True
    expected = dict(sequence=1, package_sha256=package_digest(first))
    assert local.commit_head(first) == expected
    revision = mod.read_control(key=KEY)['revision']
    assert local.commit_head(first) == expected
    assert mod.read_control(key=KEY)['revision'] == revision
    second = copy.deepcopy(first)
    second.update(sequence=2, previous_package_sha256=expected['package_sha256'])
    bad = copy.deepcopy(second)
    bad['previous_package_sha256'] = 'e' * 64
    with pytest.raises(ValueError):
        local.commit_head(bad)
    bad = copy.deepcopy(second)
    bad['bootstrap_anchor_sha256'] = 'e' * 64
    with pytest.raises(ValueError):
        local.commit_head(bad)
    assert local.commit_head(second)['sequence'] == 2
    with pytest.raises(ValueError):
        local.commit_head(first)


def test_commit_rejects_cas_race_with_takeover(protocol, monkeypatch):
    mod, store, clock, owner = protocol
    local = owner('local', 'old')
    local.acquire(anchor_sha256=ANCHOR)
    original = store.api
    def race(method, path, **kwargs):
        if method == 'PUT':
            proposed = __import__('json').loads(base64.b64decode(kwargs['payload']['content']))
            if proposed['head'] is not None:
                body = mod.read_control(key=KEY)
                body['epoch'] += 1
                body['revision'] += 1
                body['lease'].update(owner_id='new', epoch=body['epoch'])
                store.set_body(body)
        return original(method, path, **kwargs)
    monkeypatch.setattr(mod, '_api', race)
    with pytest.raises(mod.OwnershipLost):
        local.commit_head(payload())
    assert mod.read_control(key=KEY)['head'] is None


def test_release_preserves_preference_and_head(protocol):
    mod, store, clock, owner = protocol
    local = owner('local', 'local')
    local.acquire(anchor_sha256=ANCHOR)
    local.prefer_local()
    head = local.commit_head(payload())
    before = mod.read_control(key=KEY)
    local.release()
    after = mod.read_control(key=KEY)
    assert after['lease'] is None
    assert after['local_until'] == before['local_until']
    assert after['head'] == head


def test_missing_after_history_fails_closed(protocol):
    mod, store, clock, owner = protocol
    assert mod.read_control(key=KEY) is None
    store.history = True
    with pytest.raises(ValueError, match='initialized'):
        mod.read_control(key=KEY)
    with pytest.raises(ValueError):
        owner('cloud', 'new').acquire(anchor_sha256=ANCHOR)


def test_initialization_requires_validated_explicit_anchor(protocol):
    mod, store, clock, owner = protocol
    with pytest.raises(ValueError):
        owner('local', 'x').acquire()
    with pytest.raises(ValueError):
        owner('local', 'x').acquire(initial_head={'sequence': 1, 'package_sha256': 'bad'}, anchor_sha256=ANCHOR)
    assert store.puts == 0


@pytest.mark.parametrize('change', [
    lambda b: b.update(extra=True),
    lambda b: b.update(revision=True),
    lambda b: b.update(epoch=-1),
    lambda b: b.update(anchor_sha256='bad'),
    lambda b: b.update(local_until='2026-10-16T00:00:00'),
    lambda b: b['head'].update(sequence=0),
    lambda b: b['head'].update(extra=1),
    lambda b: b['lease'].update(role='unknown'),
    lambda b: b['lease'].update(epoch=2),
    lambda b: b['lease'].update(owner_id='x' * 161),
    lambda b: b['lease'].update(expires_at='tomorrow'),
    lambda b: b['lease'].update(extra=1),
])
def test_signed_but_malformed_control_is_rejected(protocol, change):
    mod, store, clock, owner = protocol
    owner('local', 'x').acquire(initial_head=dict(sequence=1, package_sha256='b' * 64), anchor_sha256=ANCHOR)
    body = mod.read_control(key=KEY)
    change(body)
    store.set_body(body)
    with pytest.raises(ValueError):
        mod.read_control(key=KEY)


def test_tampered_and_oversized_documents_are_rejected(protocol):
    mod, store, clock, owner = protocol
    owner('local', 'x').acquire(anchor_sha256=ANCHOR)
    body = __import__('json').loads(base64.b64decode(store.content))
    body['epoch'] = 100
    store.content = base64.b64encode(_canonical(body)).decode()
    with pytest.raises(ValueError, match='authentication'):
        mod.read_control(key=KEY)
    store.content = base64.b64encode(b' ' * (64 * 1024 + 1)).decode()
    with pytest.raises(ValueError, match='size'):
        mod.read_control(key=KEY)


def test_observed_head_cannot_regress_or_change_at_same_sequence(protocol):
    mod, store, clock, owner = protocol
    local = owner('local', 'x')
    local.acquire(anchor_sha256=ANCHOR)
    local.commit_head(payload())
    body = mod.read_control(key=KEY)
    body['revision'] += 1
    body['head']['package_sha256'] = 'e' * 64
    store.set_body(body)
    with pytest.raises(ValueError, match='regression|conflict'):
        local.guard(force=True)
