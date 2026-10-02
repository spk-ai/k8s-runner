"""Fail if workloads the runner suite started directly outlive the suite.

Agent sandboxes started through the orchestrator are collected by its idle
lifecycle after the suite ("collected later"), so they are reported with their
identity labels and age instead; the orchestrator E2E owns that collection.
"""
import json
import subprocess
import sys
import time

NAMESPACE = 'agyn-workloads'
SELECTOR = 'app.kubernetes.io/managed-by=k8s-runner'
ORCHESTRATED = 'agents-orchestrator'
IDENTITY = ('agent-id', 'agent-instance-id', 'thread-id', 'sandbox-id')
ATTEMPTS, INTERVAL = 24, 5


def remaining():
    out = subprocess.run(['kubectl', 'get', 'pods,pvc,secrets,configmaps', '-n', NAMESPACE,
                          '-l', SELECTOR, '-o', 'json'], check=True, capture_output=True, text=True).stdout
    return json.loads(out)['items']


def orchestrated(item):
    labels = item['metadata'].get('labels') or {}
    return ORCHESTRATED in (labels.get('agyn.dev/managed-by'), labels.get('managed-by'))


def describe(item):
    meta = item['metadata']
    labels = meta.get('labels') or {}
    ids = ' '.join(f'{key}={labels[key]}' for key in IDENTITY if labels.get(key))
    return f"{item['kind'].lower()}/{meta['name']} created={meta['creationTimestamp']} {ids}".rstrip()


def main():
    for attempt in range(ATTEMPTS):
        items = remaining()
        direct = [item for item in items if not orchestrated(item)]
        if not direct:
            break
        if attempt + 1 < ATTEMPTS:
            time.sleep(INTERVAL)
    else:
        print('::error::runner-owned resources started directly by the suite remain')
        for item in direct:
            print(describe(item))
        sys.exit(1)
    sandboxes = [item for item in items if orchestrated(item)]
    if sandboxes:
        print(f'::warning::{len(sandboxes)} orchestrator-managed sandbox resources await idle collection')
        for item in sandboxes:
            print(describe(item))
    print('No runner-owned resources started directly by the suite remain.')


if __name__ == '__main__':
    main()
