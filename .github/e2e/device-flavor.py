"""Offer the source runner a device flavor on the disposable VM, with no device.

Before deploy: add the e2e-device flavor to the synced catalog.dev.yaml, and
advertise its fake extended resource on every VM node the documented way (a
node status capacity patch), so a Pod requesting it schedules. No device plugin
backs the resource: native_flavor_devices_test proves the runner's Pod shape,
scheduling and the supplemental group, not /dev/kvm, which needs nested KVM.
Deprecated keeps the flavor out of the platform's flavor pickers, including
the smoke suite's flavor test; the runner still resolves it by name.
"""
import json
import subprocess
import sys
import time
from pathlib import Path

RESOURCE = 'e2e.agyn.dev/fake-device'
CAPACITY = '4'
FLAVOR = '''  # Added by .github/e2e/device-flavor.py for the E2E run only.
  - name: e2e-device
    deprecated: true
    resources:
      requestsCpu: "100m"
      requestsMemory: "64Mi"
      limitsCpu: "500m"
      limitsMemory: "256Mi"
    devices:
      - resource: e2e.agyn.dev/fake-device
        count: 1
    supplementalGroups: [4242]
'''


def fail(message):
    print(f'::error::{message}')
    sys.exit(1)


def kubectl(*args):
    result = subprocess.run(['kubectl', *args], capture_output=True, text=True)
    if result.returncode != 0:
        fail(f'kubectl {" ".join(args)} failed: {result.stderr.strip()}')
    return result.stdout


catalog = Path(__file__).resolve().parents[2] / 'catalog.dev.yaml'
text = catalog.read_text()
anchor = 'storageClasses:\n'
if text.count(anchor) != 1 or 'e2e-device' in text:
    fail('catalog.dev.yaml shape changed; cannot add the E2E device flavor')
catalog.write_text(text.replace(anchor, FLAVOR + anchor))

nodes = kubectl('get', 'nodes', '-o', 'jsonpath={.items[*].metadata.name}').split()
if not nodes:
    fail('no nodes to advertise the fake device on')
path = '/status/capacity/' + RESOURCE.replace('~', '~0').replace('/', '~1')
for node in nodes:
    kubectl('patch', 'node', node, '--subresource=status', '--type=json',
            '-p', json.dumps([{'op': 'add', 'path': path, 'value': CAPACITY}]))

deadline = time.time() + 120
pending = set(nodes)
while pending and time.time() < deadline:
    for node in sorted(pending):
        allocatable = json.loads(kubectl('get', 'node', node, '-o', 'json'))['status'].get('allocatable', {})
        if allocatable.get(RESOURCE) == CAPACITY:
            pending.discard(node)
    if pending:
        time.sleep(3)
if pending:
    fail(f'{RESOURCE} never became allocatable on {", ".join(sorted(pending))}')
print(f'e2e-device flavor added; {RESOURCE}={CAPACITY} allocatable on {", ".join(nodes)}')
