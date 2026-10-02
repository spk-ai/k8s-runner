"""Fail if workloads the runner suite started directly outlive the suite.

Agent sandboxes started through the orchestrator are collected by its idle
lifecycle after the suite ("collected later"), so they are reported with their
identity labels and age instead; the orchestrator E2E owns that collection.

Only the Orchestrator's own managed-by label marks its Pods and PVCs: the
runner stamps agyn.dev/managed-by on every Pod it creates, including the
suite's. A Secret carries neither, so it belongs to a sandbox only while it is
owned by that sandbox's live Pod; any other runner Secret is a direct leftover.
Ownership GC and the orphan sweep get one grace period plus an interval and a
margin to release it, read from the deployed runner.
"""
import json
import subprocess
import sys
import time

from runner_sweep import sweep_settings

NAMESPACE = 'agyn-workloads'
SELECTOR = 'app.kubernetes.io/managed-by=k8s-runner'
ORCHESTRATED = ('managed-by', 'agents-orchestrator')
IDENTITY = ('agent-id', 'agent-instance-id', 'thread-id', 'sandbox-id', 'agyn.io/workload-id')
INTERVAL, MARGIN, MINIMUM = 5, 60, 120


def remaining():
    out = subprocess.run(['kubectl', 'get', 'pods,pvc,secrets,configmaps', '-n', NAMESPACE,
                          '-l', SELECTOR, '-o', 'json'], check=True, capture_output=True, text=True).stdout
    items = json.loads(out)['items']
    for item in items:
        item.pop('data', None)
    return items


def classify(items):
    pods = {item['metadata']['uid']: item for item in items if item['kind'] == 'Pod'}

    def labelled(item):
        key, value = ORCHESTRATED
        return (item['metadata'].get('labels') or {}).get(key) == value

    def orchestrated(item):
        if item['kind'] != 'Secret':
            return labelled(item)
        owners = item['metadata'].get('ownerReferences') or []
        return any(owner.get('kind') == 'Pod' and owner.get('uid') in pods and labelled(pods[owner['uid']])
                   for owner in owners)

    direct = [item for item in items if not orchestrated(item)]
    return direct, [item for item in items if orchestrated(item)]


def describe(item):
    meta = item['metadata']
    labels = meta.get('labels') or {}
    ids = ' '.join(f'{key}={labels[key]}' for key in IDENTITY if labels.get(key))
    owners = ','.join(f"{owner.get('kind', '').lower()}/{owner.get('name')}" for owner in meta.get('ownerReferences') or [])
    owned = f' owner={owners}' if owners else ''
    return f"{item['kind'].lower()}/{meta['name']} created={meta['creationTimestamp']} {ids}{owned}".rstrip()


def main():
    interval, grace = sweep_settings()
    bound = max(MINIMUM, grace + interval + MARGIN)
    started = time.time()
    while True:
        direct, sandboxes = classify(remaining())
        if not direct or time.time() - started >= bound:
            break
        time.sleep(INTERVAL)
    if direct:
        print(f'::error::runner-owned resources started directly by the suite remain after {bound}s')
        for item in direct:
            print(describe(item))
        sys.exit(1)
    if sandboxes:
        print(f'::warning::{len(sandboxes)} orchestrator-managed sandbox resources await idle collection')
        for item in sandboxes:
            print(describe(item))
    print('No runner-owned resources started directly by the suite remain.')


if __name__ == '__main__':
    main()
