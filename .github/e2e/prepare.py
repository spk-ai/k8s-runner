from pathlib import Path
import sys

# The downloaded action is pinned in e2e.yml. Fail closed if its contract changes.
root = Path(sys.argv[1])
action = root / '.github/actions/provision-vm/action.yml'
s = action.read_text()
anchor = '    - name: Upgrade the platform to the newest charts\n'
assert s.count(anchor) == 1
step = '''    # This is only the disposable hosted-runner VM. The chart's historical
    # Quay mc tag is unavailable; build the same release from verified source.
    - name: Import source-built MinIO hook client
      shell: bash
      run: |
        set -euo pipefail
        image=quay.io/minio/mc:RELEASE.2024-11-21T17-21-54Z
        docker build -f .github/e2e/Dockerfile.mc -t "$image" .github/e2e
        docker run --rm --entrypoint /usr/bin/mc "$image" --version
        docker save "$image" | LIMA_HOME="${HOME}/.agyn/local/lima" \\
          limactl shell agyn -- sudo k3s ctr images import -

'''
s = s.replace(anchor, step + anchor)
# Composite bash runs with -e from Actions. Unavailable logs must not prevent
# describes/events, which contain the image pull error.
s = s.replace('set -uo pipefail', 'set +e\n        set -uo pipefail')
action.write_text(s)
