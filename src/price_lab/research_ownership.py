"""Authenticated acquisition lease and canonical head; no database or source I/O.

Only this small control document is mutable. Ready encrypted releases become
canonical observations when a live owner commits their exact digest here.
"""
import base64
import copy
from datetime import datetime, timedelta, timezone
import os
import re
import uuid

from .daily_delta_transport import _api
from .research_delta_codec import (
    _canonical, _fields, _hash, _instant, _loads, package_digest,
    sign_control, verify_control,
)

PATH = '/contents/.github/research-owner.json'
HISTORY = '/commits?path=.github%2Fresearch-owner.json&per_page=1'
VERSION = 'research-ownership-v1'
MAX_CONTROL_BYTES = 64 * 1024
LEASE_SECONDS = 600
LOCAL_PREFERENCE_SECONDS = 180
GUARD_SECONDS = 60
CAS_RETRIES = 4
_LIMIT = 2 ** 63
_OWNER = re.compile(r'[A-Za-z0-9_-]{1,160}')


class OwnershipLost(RuntimeError):
    """The caller must stop source sends and discard its uncommitted batch."""


def _head(value):
    if value is None:
        return None
    _fields(value, ('sequence', 'package_sha256'))
    if type(value['sequence']) is not int or not 1 <= value['sequence'] < _LIMIT:
        raise ValueError('Invalid ownership head sequence')
    _hash(value['package_sha256'])
    return value


def _stamp(value):
    if not isinstance(value, str) or len(value) > 64:
        raise ValueError('Invalid ownership UTC timestamp')
    return _instant(value)


def _validate(body):
    _fields(body, ('version', 'revision', 'epoch', 'anchor_sha256', 'head', 'lease', 'local_until'))
    if body['version'] != VERSION:
        raise ValueError('Invalid research ownership version')
    if type(body['revision']) is not int or not 1 <= body['revision'] < _LIMIT:
        raise ValueError('Invalid ownership revision')
    if type(body['epoch']) is not int or not 0 <= body['epoch'] < _LIMIT:
        raise ValueError('Invalid ownership epoch')
    _hash(body['anchor_sha256'])
    _head(body['head'])
    if body['local_until'] is not None:
        _stamp(body['local_until'])
    lease = body['lease']
    if lease is not None:
        _fields(lease, ('role', 'owner_id', 'epoch', 'expires_at'))
        if lease['role'] not in ('local', 'cloud'):
            raise ValueError('Invalid ownership role')
        if not isinstance(lease['owner_id'], str) or not _OWNER.fullmatch(lease['owner_id']):
            raise ValueError('Invalid ownership identity')
        if type(lease['epoch']) is not int or lease['epoch'] != body['epoch'] or lease['epoch'] < 1:
            raise ValueError('Invalid lease epoch')
        _stamp(lease['expires_at'])
    return body


def _read_remote(*, token, key):
    try:
        remote = _api('GET', PATH, token=token)
    except RuntimeError as exc:
        if 'HTTP 404' not in str(exc):
            raise
        history = _api('GET', HISTORY, token=token)
        if not isinstance(history, list) or history:
            raise ValueError('Missing previously initialized ownership control') from None
        return None, None
    if not isinstance(remote, dict) or not isinstance(remote.get('sha'), str) or not 1 <= len(remote['sha']) <= 160:
        raise ValueError('Invalid ownership Contents response')
    encoded = remote.get('content')
    if not isinstance(encoded, str) or len(encoded) > MAX_CONTROL_BYTES * 2:
        raise ValueError('Ownership control size limit')
    try:
        raw = base64.b64decode(''.join(encoded.split()), validate=True)
    except (ValueError, UnicodeError):
        raise ValueError('Invalid ownership control encoding') from None
    if len(raw) > MAX_CONTROL_BYTES:
        raise ValueError('Ownership control size limit')
    body = _validate(verify_control(_loads(raw), key))
    return body, remote['sha']


def read_control(*, token=None, key=None):
    """Return authenticated control, or None only before its first creation."""
    body, _ = _read_remote(token=token, key=key or os.environ.get('PRICE_DATA_KEY'))
    return body


class ResearchOwner:
    def __init__(self, role, *, owner_id=None, token=None, key=None, clock=None):
        if role not in ('local', 'cloud'):
            raise ValueError('Invalid ownership role')
        owner_id = uuid.uuid4().hex if owner_id is None else owner_id
        if not isinstance(owner_id, str) or not _OWNER.fullmatch(owner_id):
            raise ValueError('Invalid ownership identity')
        self.role = role
        self.owner_id = owner_id
        self._token = token
        self._key = key or os.environ.get('PRICE_DATA_KEY')
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._control = None
        self._epoch = None
        self._checked_at = None
        self._last_now = None

    @property
    def control(self):
        return copy.deepcopy(self._control)

    @property
    def head(self):
        return copy.deepcopy(self._control['head']) if self._control else None

    def _now(self):
        now = self._clock() if callable(self._clock) else self._clock
        if not isinstance(now, datetime) or now.tzinfo is None:
            raise ValueError('Aware ownership clock required')
        now = now.astimezone(timezone.utc)
        if self._last_now is not None and now < self._last_now:
            raise OwnershipLost('Ownership clock regressed')
        self._last_now = now
        return now

    def _observe(self, body):
        prior = self._control
        if prior:
            if body is None:
                raise ValueError('Missing initialized ownership control')
            if (body['revision'] < prior['revision'] or body['epoch'] < prior['epoch'] or
                    body['anchor_sha256'] != prior['anchor_sha256']):
                raise ValueError('Authenticated ownership control regression')
            if body['revision'] == prior['revision'] and body != prior:
                raise ValueError('Authenticated ownership revision conflict')
            before, after = prior['head'], body['head']
            if before and (not after or after['sequence'] < before['sequence'] or
                    after['sequence'] == before['sequence'] and after != before):
                raise ValueError('Authenticated ownership head regression or conflict')
        self._control = copy.deepcopy(body)

    def _read(self):
        body, sha = _read_remote(token=self._token, key=self._key)
        self._observe(body)
        return body, sha

    def _update(self, change):
        # Each PUT is read back, including the last uncertain attempt. A CAS
        # conflict is always re-evaluated against the fresh authenticated state.
        for attempt in range(CAS_RETRIES + 1):
            body, sha = self._read()
            proposed, result = change(copy.deepcopy(body), self._now())
            if proposed is None:
                return result
            if attempt == CAS_RETRIES:
                break
            proposed['revision'] = (body['revision'] if body else 0) + 1
            _validate(proposed)
            raw = _canonical(sign_control(proposed, self._key))
            if len(raw) > MAX_CONTROL_BYTES:
                raise ValueError('Ownership control size limit')
            update = dict(message='Update research acquisition ownership', branch='main',
                content=base64.b64encode(raw).decode('ascii'))
            if sha is not None:
                update['sha'] = sha
            try:
                _api('PUT', PATH, token=self._token, payload=update)
            except RuntimeError:
                continue
        raise ValueError('Ownership update could not be confirmed')

    def _matches(self, body, now):
        lease = body['lease'] if body else None
        return bool(lease and lease['role'] == self.role and lease['owner_id'] == self.owner_id and
            lease['epoch'] == self._epoch and _stamp(lease['expires_at']) > now)

    def _require_live(self, body, now):
        if not self._matches(body, now):
            self._checked_at = None
            raise OwnershipLost('Research acquisition ownership lost')

    def prefer_local(self):
        if self.role != 'local':
            raise ValueError('Only a local owner can prefer local acquisition')
        until = self._now() + timedelta(seconds=LOCAL_PREFERENCE_SECONDS)
        def change(body, now):
            if body is None:
                raise ValueError('Ownership must be initialized with an authenticated anchor')
            if body['local_until'] and _stamp(body['local_until']) >= until:
                return None, self.control
            body['local_until'] = until.isoformat()
            return body, None
        return self._update(change)

    def acquire(self, initial_head=None, anchor_sha256=None):
        _head(initial_head)
        if anchor_sha256 is not None:
            _hash(anchor_sha256)
        def change(body, now):
            if body is None:
                _hash(anchor_sha256)
                body = dict(version=VERSION, revision=1, epoch=0, anchor_sha256=anchor_sha256,
                    head=copy.deepcopy(initial_head), lease=None, local_until=None)
            elif anchor_sha256 is not None and body['anchor_sha256'] != anchor_sha256:
                raise ValueError('Ownership bootstrap anchor conflict')
            lease = body['lease']
            if lease and _stamp(lease['expires_at']) > now:
                if lease['owner_id'] == self.owner_id and lease['role'] == self.role:
                    self._epoch = lease['epoch']
                    self._checked_at = now
                    return None, True
                return None, False
            if self.role == 'cloud' and body['local_until'] and _stamp(body['local_until']) > now:
                return None, False
            body['epoch'] += 1
            body['lease'] = dict(role=self.role, owner_id=self.owner_id, epoch=body['epoch'],
                expires_at=(now + timedelta(seconds=LEASE_SECONDS)).isoformat())
            return body, None
        return self._update(change)

    def guard(self, force=False):
        now = self._now()
        self._require_live(self._control, now)
        if not force and self._checked_at is not None and (now - self._checked_at).total_seconds() < GUARD_SECONDS:
            return self.control
        until = now + timedelta(seconds=LEASE_SECONDS)
        def change(body, fresh_now):
            self._require_live(body, fresh_now)
            if _stamp(body['lease']['expires_at']) >= until:
                self._checked_at = fresh_now
                return None, self.control
            body['lease']['expires_at'] = until.isoformat()
            return body, None
        return self._update(change)

    def should_yield(self):
        body, _ = self._read()
        now = self._now()
        return bool(self.role == 'cloud' and body and body['local_until'] and _stamp(body['local_until']) > now)

    def commit_head(self, payload):
        digest = package_digest(payload)
        target = dict(sequence=payload['sequence'], package_sha256=digest)
        def change(body, now):
            self._require_live(body, now)
            if payload['bootstrap_anchor_sha256'] != body['anchor_sha256']:
                raise ValueError('Ownership package anchor conflict')
            if body['head'] == target:
                self._checked_at = now
                return None, copy.deepcopy(target)
            parent = body['head']
            if (payload['sequence'] != (parent['sequence'] + 1 if parent else 1) or
                    payload['previous_package_sha256'] != (parent['package_sha256'] if parent else None)):
                raise ValueError('Ownership package parent or sequence conflict')
            body['head'] = target
            body['lease']['expires_at'] = (now + timedelta(seconds=LEASE_SECONDS)).isoformat()
            return body, None
        return self._update(change)

    def release(self):
        def change(body, now):
            lease = body['lease'] if body else None
            if not lease or (lease['role'], lease['owner_id'], lease['epoch']) != (self.role, self.owner_id, self._epoch):
                return None, self.control
            body['lease'] = None
            return body, None
        result = self._update(change)
        self._epoch = None
        self._checked_at = None
        return result
