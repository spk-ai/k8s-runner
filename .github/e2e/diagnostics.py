"""Failure diagnostics for the source runner and what it left in the VM.

Complete runner logs (current and previous container), the Orchestrator's log
lines naming any leftover workload, workload-namespace events in time order,
and every runner-managed object's metadata. Secret data is never requested
for printing: Secrets are reduced to identity, labels, owners and timestamps.
"""
import json
import subprocess

PLATFORM = 'agyn-platform'
WORKLOADS = 'agyn-workloads'
RUNNER = 'app.kubernetes.io/name=k8s-runner'
MANAGED = 'app.kubernetes.io/managed-by=k8s-runner'
WORKLOAD = 'agyn.io/workload-id'


def kubectl(*args):
    result = subprocess.run(['kubectl', *args], capture_output=True, text=True)
    return result.stdout + (f'[kubectl {" ".join(args)}: {result.stderr.strip()}]\n' if result.returncode else '')


def section(title, body):
    print(f'::group::{title}')
    print(body.rstrip())
    print('::endgroup::')


def leftovers():
    out = subprocess.run(['kubectl', 'get', 'pods,secrets,pvc,configmaps', '-n', WORKLOADS, '-l', MANAGED, '-o', 'json'],
                         capture_output=True, text=True).stdout
    try:
        items = json.loads(out)['items']
    except ValueError:
        return []
    summary = []
    for item in items:
        meta = item['metadata']
        summary.append({
            'kind': item['kind'], 'name': meta['name'], 'uid': meta.get('uid'),
            'created': meta.get('creationTimestamp'), 'deleting': meta.get('deletionTimestamp'),
            'labels': meta.get('labels') or {}, 'annotations': sorted(meta.get('annotations') or {}),
            'owners': meta.get('ownerReferences') or [], 'finalizers': meta.get('finalizers') or [],
            'phase': (item.get('status') or {}).get('phase'),
        })
    return summary


def orchestrator_lines(ids):
    names = kubectl('get', 'deployments', '-n', PLATFORM, '-o', 'name').split()
    out = []
    for name in (name for name in names if 'orchestrator' in name):
        log = kubectl('logs', '-n', PLATFORM, name, '--all-containers', '--timestamps', '--tail=-1')
        lines = log.splitlines()
        matched = [line for line in lines if any(workload in line for workload in ids)] if ids else lines[-200:]
        out.append(f'--- {name}: {len(matched)} of {len(lines)} lines' + (' (last 200)' if not ids else ''))
        out.extend(matched)
    return '\n'.join(out) or 'no orchestrator deployment found'


def main():
    section('k8s-runner pods', kubectl('get', 'pods', '-n', PLATFORM, '-l', RUNNER, '-o', 'wide') +
            kubectl('describe', 'pods', '-n', PLATFORM, '-l', RUNNER))
    for previous in (False, True):
        args = ['logs', '-n', PLATFORM, '-l', RUNNER, '--all-containers', '--prefix', '--timestamps', '--tail=-1']
        section(f'k8s-runner {"previous container" if previous else "complete"} log',
                kubectl(*args, *(['--previous'] if previous else [])))
    objects = leftovers()
    section('runner-managed objects in the workload namespace (metadata only)', json.dumps(objects, indent=1))
    ids = sorted({item['labels'][WORKLOAD] for item in objects if item['labels'].get(WORKLOAD)})
    section(f'Orchestrator log lines naming leftover workloads {ids}', orchestrator_lines(ids))
    section('workload namespace events', kubectl('get', 'events', '-n', WORKLOADS, '--sort-by=.lastTimestamp'))


if __name__ == '__main__':
    main()
