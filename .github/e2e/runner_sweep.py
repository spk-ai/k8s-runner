"""The orphan-sweep settings the deployed source runner actually runs with."""
import json
import re
import subprocess


def duration(text):
    parts = re.fullmatch(r'(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?', text or '')
    if not text or not parts:
        raise SystemExit(f'unsupported duration {text!r}')
    hours, minutes, seconds = (int(part or 0) for part in parts.groups())
    return hours * 3600 + minutes * 60 + seconds


def sweep_settings(namespace='agyn-platform'):
    """Interval and grace in seconds, read from the Deployment, never copied."""
    out = subprocess.run(['kubectl', 'get', 'deployment', 'k8s-runner', '-n', namespace, '-o', 'json'],
                         check=True, capture_output=True, text=True).stdout
    env = {var['name']: var.get('value') for container in json.loads(out)['spec']['template']['spec']['containers']
           for var in container.get('env', [])}
    return duration(env.get('WORKLOAD_SECRET_SWEEP_INTERVAL')), duration(env.get('WORKLOAD_SECRET_SWEEP_GRACE'))
