"""Prove on real Kubernetes that runner workload Secrets do not outlive their Pod.

plant (after deploy): create, in the workload namespace, an ownerless Secret
named and labelled exactly like runner credentials for a workload with no Pod,
which the orphan sweep must release, and three near-misses it must keep.

prove (after the suite): the orphan is gone and its release logged with only
the workload and Secret name; the near-misses survive a further sweep pass;
every runner Secret whose Pod exists is owned by exactly that Pod; and deleting
one such Pod from outside the runner releases its Secrets through Kubernetes
garbage collection, not the sweep. The fixtures are removed afterwards.
"""
import json
import os
import subprocess
import sys
import time
import uuid

from runner_sweep import sweep_settings

NAMESPACE = 'agyn-workloads'
RUNNER_NAMESPACE = 'agyn-platform'
RUNNER_SELECTOR = 'app.kubernetes.io/name=k8s-runner'
MANAGED = 'app.kubernetes.io/managed-by'
WORKLOAD = 'agyn.io/workload-id'
ATTEMPT = 'agyn.io/startup-attempt'
FIXTURE = 'agyn.io/e2e-fixture'
FIXTURE_VALUE = 'secret-sweep'
RELEASED = 'deleted orphaned workload secret'
STATE = os.path.join(os.environ.get('RUNNER_TEMP', '/tmp'), 'secret-sweep-fixture.json')


def fail(message):
    print(f'::error::{message}')
    sys.exit(1)


def kubectl(*args, stdin=None, check=True):
    result = subprocess.run(['kubectl', *args], input=stdin, capture_output=True, text=True)
    if check and result.returncode != 0:
        fail(f'kubectl {" ".join(args)} failed: {result.stderr.strip()}')
    return result


def items(kind, selector):
    out = kubectl('get', kind, '-n', NAMESPACE, '-l', selector, '-o', 'json').stdout
    result = json.loads(out)['items']
    for item in result:
        # Credential content is never needed, kept or printed here.
        item.pop('data', None)
        item.pop('stringData', None)
    return result


def current(kind, name):
    out = kubectl('get', kind, name, '-n', NAMESPACE, '--ignore-not-found', '-o', 'json').stdout.strip()
    if not out:
        return None
    obj = json.loads(out)
    obj.pop('data', None)
    return obj


def present(kind, name, uid):
    obj = current(kind, name)
    return obj is not None and obj['metadata']['uid'] == uid


def wait_until(predicate, timeout, what):
    started = time.time()
    while time.time() - started < timeout:
        if predicate():
            return time.time() - started
        time.sleep(3)
    fail(f'{what} not observed within {timeout:.0f}s')


def runner_log():
    entries = []
    for extra in ([], ['--previous']):
        out = kubectl('logs', '-n', RUNNER_NAMESPACE, '-l', RUNNER_SELECTOR, '--tail=-1', *extra, check=False).stdout
        for line in out.splitlines():
            try:
                entry = json.loads(line)
            except ValueError:
                continue
            if isinstance(entry, dict):
                entries.append(entry)
    return entries


def fixture_secret(name, workload_id, attempt=True, owner=None):
    metadata = {'name': name, 'namespace': NAMESPACE,
                'labels': {MANAGED: 'k8s-runner', WORKLOAD: workload_id, FIXTURE: FIXTURE_VALUE}}
    if attempt:
        metadata['annotations'] = {ATTEMPT: str(uuid.uuid4())}
    if owner:
        metadata['ownerReferences'] = [owner]
    return {'apiVersion': 'v1', 'kind': 'Secret', 'type': 'Opaque', 'metadata': metadata}


def create(obj):
    created = json.loads(kubectl('create', '-f', '-', '-o', 'json', stdin=json.dumps(obj)).stdout)
    return [created['metadata']['name'], created['metadata']['uid']]


def plant():
    ids = {key: str(uuid.uuid4()) for key in ('orphan', 'unannotated', 'foreign', 'owned')}
    holder = create({'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': {
        'name': f'e2e-sweep-holder-{ids["owned"]}', 'namespace': NAMESPACE, 'labels': {FIXTURE: FIXTURE_VALUE}}})
    owner = {'apiVersion': 'v1', 'kind': 'ConfigMap', 'name': holder[0], 'uid': holder[1]}
    state = {'planted_at': time.time(), 'holder': holder, 'orphan_workload': ids['orphan'], 'secrets': {
        'orphan': create(fixture_secret(f'workload-{ids["orphan"]}-pull', ids['orphan'])),
        'unannotated': create(fixture_secret(f'workload-{ids["unannotated"]}-pull', ids['unannotated'], attempt=False)),
        'foreign-name': create(fixture_secret(f'e2e-sweep-{ids["foreign"]}', ids['foreign'])),
        'owned': create(fixture_secret(f'workload-{ids["owned"]}-inline-files', ids['owned'], owner=owner)),
    }}
    with open(STATE, 'w') as handle:
        json.dump(state, handle)
    for key, (name, uid) in state['secrets'].items():
        print(f'planted {key}: secret/{name} uid={uid}')


def prove_sweep(state, interval, grace):
    orphan, orphan_uid = state['secrets']['orphan']
    bound = max(30, state['planted_at'] + grace + 2 * interval + 60 - time.time())
    elapsed = wait_until(lambda: not present('secret', orphan, orphan_uid), bound, f'release of orphan {orphan}')
    print(f'orphan secret/{orphan} released ({elapsed:.0f}s after the suite; grace={grace}s interval={interval}s)')
    time.sleep(interval + 10)
    for key, (name, uid) in state['secrets'].items():
        if key != 'orphan' and not present('secret', name, uid):
            fail(f'the sweep removed {key} near-miss secret/{name}')
    released = [entry for entry in runner_log() if entry.get('msg') == RELEASED]
    names = {name for key, (name, _) in state['secrets'].items() if key != 'orphan'}
    if not any(entry.get('secret') == orphan and entry.get('workload_id') == state['orphan_workload'] for entry in released):
        fail(f'no runner log line released secret/{orphan}')
    for entry in released:
        if entry.get('secret') in names:
            fail(f'the runner logged releasing near-miss secret/{entry.get("secret")}')
        if set(entry) - {'level', 'ts', 'caller', 'msg'} != {'workload_id', 'secret'}:
            fail(f'release log carries more than the workload and Secret name: {sorted(entry)}')
    print(f'near-misses kept through another pass: {sorted(names)}')


def prove_ownership():
    """Every runner Secret of a live Pod is owned by exactly that Pod."""
    pods = {pod['metadata']['name']: pod for pod in items('pods', f'{MANAGED}=k8s-runner')}
    owned = {}
    for secret in items('secrets', f'{MANAGED}=k8s-runner,{FIXTURE}!={FIXTURE_VALUE}'):
        meta = secret['metadata']
        pod = pods.get(f'workload-{(meta.get("labels") or {}).get(WORKLOAD, "")}')
        owners = meta.get('ownerReferences') or []
        if pod is None:
            print(f'secret/{meta["name"]} has no Pod yet or any longer; owners={owners}')
            continue
        want = {'apiVersion': 'v1', 'kind': 'Pod', 'name': pod['metadata']['name'], 'uid': pod['metadata']['uid']}
        if len(owners) != 1 or {key: owners[0].get(key) for key in want} != want:
            fail(f'secret/{meta["name"]} of live pod/{pod["metadata"]["name"]} is not owned by it: {owners}')
        owned.setdefault(pod['metadata']['name'], []).append([meta['name'], meta['uid']])
    if not owned:
        fail('no runner-started workload with Secrets remains to prove release on')
    for pod, secrets in sorted(owned.items()):
        print(f'pod/{pod} owns {[name for name, _ in secrets]}')
    return pods, owned


def prove_external_deletion(pods, owned):
    name = min(owned, key=lambda pod: pods[pod]['metadata']['creationTimestamp'])
    secrets = owned[name]
    # An actor other than the runner: no Stop/Remove call is made.
    started = time.time()
    kubectl('delete', 'pod', name, '-n', NAMESPACE, '--wait=true', '--timeout=180s')
    elapsed = wait_until(lambda: not any(present('secret', secret, uid) for secret, uid in secrets), 120,
                         f'garbage collection of {[secret for secret, _ in secrets]}')
    released = {entry.get('secret') for entry in runner_log() if entry.get('msg') == RELEASED}
    if released & {secret for secret, _ in secrets}:
        fail('the sweep, not ownership, released the deleted Pod\'s Secrets')
    print(f'pod/{name} deleted externally; its Secrets were garbage-collected '
          f'{elapsed:.0f}s after deletion completed ({time.time() - started:.0f}s total)')


def remove_fixtures(state):
    for name, uid in [*state['secrets'].values()]:
        if present('secret', name, uid):
            kubectl('delete', 'secret', name, '-n', NAMESPACE, '--ignore-not-found')
    name, uid = state['holder']
    if present('configmap', name, uid):
        kubectl('delete', 'configmap', name, '-n', NAMESPACE, '--ignore-not-found')


def prove():
    with open(STATE) as handle:
        state = json.load(handle)
    interval, grace = sweep_settings()
    try:
        prove_sweep(state, interval, grace)
        prove_external_deletion(*prove_ownership())
    finally:
        remove_fixtures(state)
    print('Workload Secrets are released with their Pod and by the orphan sweep.')


if __name__ == '__main__':
    {'plant': plant, 'prove': prove}[sys.argv[1]]()
