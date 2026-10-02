"""Install and prove the source chart's volume-backend grant in the disposable VM.

The VM's platform release predates the chart's volume-backend ClusterRole and
the source deployment is patched in place, so the chart's own render is applied
here. The grant must stay exactly: get on the single workload Namespace for the
existing runner service account. Anything broader fails the fixture.
"""
import json
import subprocess
import sys

NAMESPACE = 'agyn-platform'
ACCOUNT = 'k8s-runner'
WORKLOADS = 'agyn-workloads'
SUBJECT = f'system:serviceaccount:{NAMESPACE}:{ACCOUNT}'
ROLE = f'{NAMESPACE}-{ACCOUNT}-volume-backend'

# (verb, resource, namespace) probes; only the first may change from no to yes.
GRANTED = ('get', f'namespaces/{WORKLOADS}', None)
UNCHANGED = [
    ('get', 'namespaces/default', None),
    ('get', 'namespaces/kube-system', None),
    ('get', f'namespaces/{NAMESPACE}', None),
    ('list', 'namespaces', None),
    ('watch', 'namespaces', None),
    ('create', 'namespaces', None),
    ('update', f'namespaces/{WORKLOADS}', None),
    ('patch', f'namespaces/{WORKLOADS}', None),
    ('delete', f'namespaces/{WORKLOADS}', None),
    ('get', 'secrets', WORKLOADS),
    ('list', 'secrets', WORKLOADS),
    ('get', 'secrets', NAMESPACE),
    ('list', 'secrets', 'kube-system'),
    ('*', '*', None),
    ('*', '*', WORKLOADS),
    ('escalate', 'clusterroles', None),
    ('bind', 'clusterroles', None),
]


def run(*args, stdin=None, check=True):
    result = subprocess.run(args, input=stdin, capture_output=True, text=True)
    if check and result.returncode != 0:
        sys.exit(f'{" ".join(args)} failed: {result.stderr.strip()}')
    return result


def can_i(verb, resource, namespace):
    args = ['kubectl', 'auth', 'can-i', verb, resource, f'--as={SUBJECT}']
    args += ['-n', namespace] if namespace else []
    answer = run(*args, check=False).stdout.strip()
    if answer not in ('yes', 'no'):
        sys.exit(f'unexpected can-i answer for {verb} {resource}: {answer!r}')
    return answer == 'yes'


def matrix():
    return {probe: can_i(*probe) for probe in [GRANTED, *UNCHANGED]}


def render():
    out = run('helm', 'template', ACCOUNT, 'charts/k8s-runner', '--namespace', NAMESPACE,
              '--set', f'fullnameOverride={ACCOUNT}',
              '--set', 'serviceAccount.create=false', '--set', f'serviceAccount.name={ACCOUNT}',
              '--set', f'workloadNamespace={WORKLOADS}',
              '--show-only', 'templates/volume-backend-rbac.yaml').stdout
    text = run('kubectl', 'create', '--dry-run=client', '-o', 'json', '-f', '-', stdin=out).stdout
    decoder, items, pos = json.JSONDecoder(), [], 0
    while pos < len(text.rstrip()):
        doc, pos = decoder.raw_decode(text, pos)
        items += doc['items'] if doc.get('kind') == 'List' else [doc]
        while pos < len(text) and text[pos].isspace():
            pos += 1
    kinds = sorted(item['kind'] for item in items)
    assert kinds == ['ClusterRole', 'ClusterRoleBinding'], kinds
    role = next(item for item in items if item['kind'] == 'ClusterRole')
    binding = next(item for item in items if item['kind'] == 'ClusterRoleBinding')
    assert role['metadata']['name'] == ROLE, role['metadata']['name']
    assert role['rules'] == [{'apiGroups': [''], 'resources': ['namespaces'],
                              'resourceNames': [WORKLOADS], 'verbs': ['get']}], role['rules']
    assert 'aggregationRule' not in role
    assert binding['metadata']['name'] == ROLE, binding['metadata']['name']
    assert binding['roleRef'] == {'apiGroup': 'rbac.authorization.k8s.io',
                                  'kind': 'ClusterRole', 'name': ROLE}, binding['roleRef']
    assert binding['subjects'] == [{'kind': 'ServiceAccount', 'name': ACCOUNT,
                                    'namespace': NAMESPACE}], binding['subjects']
    return out


def main():
    manifest = render()
    run('kubectl', 'get', 'serviceaccount', ACCOUNT, '-n', NAMESPACE)
    before = matrix()
    if before[GRANTED]:
        sys.exit('the VM already grants namespace get; the fixture would prove nothing')
    run('kubectl', 'apply', '-f', '-', stdin=manifest)
    after = matrix()
    changed = sorted(' '.join(filter(None, probe)) for probe in after if after[probe] != before[probe])
    if changed != [' '.join(filter(None, GRANTED))] or not after[GRANTED]:
        sys.exit(f'volume-backend grant changed unexpected permissions: {changed}')
    print(f'{SUBJECT} gained only: get namespaces/{WORKLOADS}')
    for probe, allowed in after.items():
        print(f"  {'yes' if allowed else 'no '} {' '.join(filter(None, probe))}")


if __name__ == '__main__':
    main()
