"""Install the source chart's workload grant, with the Secret sweep, in the VM.

The VM's platform release binds the runner to an older rule set without Secret
get/list/patch, so the source runner could neither attach Pod owners to its
credentials nor sweep orphans. Apply the source chart's own namespaced Role
under a separate name, bound only to the existing runner account, and prove
that nothing outside the workload namespace changes. This is the disposable
hosted-runner VM; no production RBAC is touched.
"""
import json
import subprocess
import sys

NAMESPACE = 'agyn-platform'
ACCOUNT = 'k8s-runner'
WORKLOADS = 'agyn-workloads'
NAME = 'k8s-runner-source-workload'
SUBJECT = f'system:serviceaccount:{NAMESPACE}:{ACCOUNT}'
SWEEP_RULE = {'apiGroups': [''], 'resources': ['secrets'], 'verbs': ['list']}

# (verb, resource, namespace): owner references and the sweep need these.
REQUIRED = [(verb, 'secrets', WORKLOADS) for verb in ('get', 'list', 'patch', 'create', 'delete')]
UNCHANGED = [
    *[(verb, 'secrets', namespace) for namespace in (NAMESPACE, 'kube-system', 'default')
      for verb in ('get', 'list', 'patch', 'delete')],
    ('watch', 'secrets', WORKLOADS),
    ('list', 'namespaces', None),
    ('*', '*', None),
    ('*', '*', WORKLOADS),
    ('escalate', 'roles', WORKLOADS),
    ('bind', 'roles', WORKLOADS),
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
    return {probe: can_i(*probe) for probe in [*REQUIRED, *UNCHANGED]}


def render(sweep):
    out = run('helm', 'template', ACCOUNT, 'charts/k8s-runner', '--namespace', NAMESPACE,
              '--set', f'fullnameOverride={NAME}',
              '--set', 'serviceAccount.create=false', '--set', f'serviceAccount.name={ACCOUNT}',
              '--set', f'workloadNamespace={WORKLOADS}',
              '--set', f'workloadSecretSweep.enabled={str(sweep).lower()}',
              '--show-only', 'templates/workload-rbac.yaml').stdout
    text = run('kubectl', 'create', '--dry-run=client', '-o', 'json', '-f', '-', stdin=out).stdout
    decoder, items, pos = json.JSONDecoder(), [], 0
    while pos < len(text.rstrip()):
        doc, pos = decoder.raw_decode(text, pos)
        items += doc['items'] if doc.get('kind') == 'List' else [doc]
        while pos < len(text) and text[pos].isspace():
            pos += 1
    return out, items


def verify(items, chart_rules):
    kinds = sorted(item['kind'] for item in items)
    assert kinds == ['Role', 'RoleBinding'], kinds
    role = next(item for item in items if item['kind'] == 'Role')
    binding = next(item for item in items if item['kind'] == 'RoleBinding')
    for item in (role, binding):
        assert item['metadata']['name'] == NAME, item['metadata']['name']
        assert item['metadata']['namespace'] == WORKLOADS, item['metadata']['namespace']
    # Exactly the chart's default workload rules plus the sweep's list-only rule.
    assert role['rules'] == chart_rules + [SWEEP_RULE], role['rules']
    assert binding['roleRef'] == {'apiGroup': 'rbac.authorization.k8s.io', 'kind': 'Role', 'name': NAME}, binding['roleRef']
    assert binding['subjects'] == [{'kind': 'ServiceAccount', 'name': ACCOUNT, 'namespace': NAMESPACE}], binding['subjects']


def main():
    _, default_items = render(False)
    default_role = next(item for item in default_items if item['kind'] == 'Role')
    assert SWEEP_RULE not in default_role['rules'], 'the default chart must not grant Secret list'
    manifest, items = render(True)
    verify(items, default_role['rules'])
    run('kubectl', 'get', 'serviceaccount', ACCOUNT, '-n', NAMESPACE)
    for kind in ('role', 'rolebinding'):
        existing = run('kubectl', 'get', kind, NAME, '-n', WORKLOADS, '--ignore-not-found', '-o', 'name').stdout.strip()
        if existing:
            sys.exit(f'{existing} already exists in {WORKLOADS}; refusing to replace it')
    before = matrix()
    run('kubectl', 'apply', '-f', '-', stdin=manifest)
    after = matrix()
    missing = [' '.join(filter(None, probe)) for probe in REQUIRED if not after[probe]]
    widened = [' '.join(filter(None, probe)) for probe in UNCHANGED if after[probe] != before[probe]]
    if missing or widened:
        sys.exit(f'workload grant incomplete {missing} or widened {widened}')
    gained = sorted(' '.join(filter(None, probe)) for probe in REQUIRED if not before[probe])
    print(f'{SUBJECT} gained in {WORKLOADS}: {gained}')
    for probe, allowed in after.items():
        print(f"  {'yes' if allowed else 'no '} {' '.join(filter(None, probe))}")


if __name__ == '__main__':
    main()
