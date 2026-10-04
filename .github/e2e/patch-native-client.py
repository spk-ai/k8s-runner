from pathlib import Path
import sys

suite = Path(sys.argv[1]) / 'suites/go-core'
root = suite / 'tests'
p = root / 'grpc.go'
s = p.read_text()
anchor = 'func dialGRPC(t *testing.T, addr string, opts ...grpc.DialOption) *grpc.ClientConn {\n'
assert s.count(anchor) == 1 and 'dialNativeRunner' not in s, 'pinned E2E dialer contract changed'
config = suite / 'buf.gen.yaml'
config_text = config.read_text()
old = '  - module: buf.build/agynio/api'
assert config_text.count(old) == 1, 'pinned E2E API input contract changed'
fixture = Path(__file__).with_name('runner_ziti_test.go.txt').read_text()

p.write_text(s.replace(anchor, anchor + '\tif addr == runnerAddr { return dialNativeRunner(t, opts...) }\n'))
(root / 'runner_ziti_test.go').write_text(fixture)
# Needs the flavor and node capacity .github/e2e/device-flavor.py adds.
(root / 'native_flavor_devices_test.go').write_text(Path(__file__).with_name('native_flavor_devices_test.go.txt').read_text())
config.write_text(config_text.replace(old, '  - git_repo: https://github.com/spk-ai/api.git\n    ref: f62e2ad47a5ea5451d88c3cc0138f73be708eb55\n    subdir: proto'))

# The checked-removal contract is part of the same pinned native API.
import subprocess
subprocess.run([sys.executable, str(Path(__file__).with_name('patch-native-volumes.py')), sys.argv[1]], check=True)
