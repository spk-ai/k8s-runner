from pathlib import Path
import sys

root = Path(sys.argv[1]) / 'suites/go-core/tests'
changes = {}
def edit(name, old, new, count=1):
    p = root / name
    s = changes.get(p, p.read_text())
    assert s.count(old) == count, f'pinned volume fixture changed: {name}: {old[:70]}'
    changes[p] = s.replace(old, new)

edit('k8s_runner_helpers_test.go', '\tregisterWorkloadCleanup(t, client, workloadID)', '\tregisterNativeFixtureCleanup(t, client, req, workloadID)')
for name in ['k8s_runner_helpers_test.go', 'main_test.go']:
    edit(name, 'RemoveVolumes: true,', 'RemoveVolumes: false,')
for name, indent in [('k8s_runner_volume_test.go','\t\t'), ('k8s_runner_workload_test.go','\t\t\t')]:
    edit(name, indent+'PersistentName: volumeName,', indent+'PersistentName: volumeName,\n'+indent+'Labels: map[string]string{"volume_key": volumeName},')
old = '''		// Removing a volume that is not there is not an error: the runner
		// answers a missing PVC with success, deliberately, so the call is
		// idempotent. Asserting NotFound asserted a contract it does not offer.
		_, err := client.RemoveVolume(ctx, &runnerv1.RemoveVolumeRequest{VolumeName: "missing-volume"})
		require.NoError(t, err)'''
edit('k8s_runner_errors_test.go', old, '''		// Absence by reusable name cannot authorize unchecked deletion.
		_, err := client.RemoveVolume(ctx, &runnerv1.RemoveVolumeRequest{VolumeName: "missing-volume"})
		requireGRPCCode(t, err, codes.FailedPrecondition)''')
old = '''		_, err = client.RemoveVolume(ctx, &runnerv1.RemoveVolumeRequest{VolumeName: volumeName})
		require.NoError(t, err)
		// And again: the runner answers a missing PVC with success, so removing
		// twice is the same as removing once.
		_, err = client.RemoveVolume(ctx, &runnerv1.RemoveVolumeRequest{VolumeName: volumeName})
		require.NoError(t, err)'''
edit('k8s_runner_volume_test.go', old, '''		expected := nativeVolume(t, ctx, client, volumeName)
		removeNativeVolume(t, ctx, client, expected)
		// Idempotence retains the same backend, UID, key and ownership snapshot.
		removeNativeVolume(t, ctx, client, expected)''')
edit('k8s_runner_workload_test.go', '"github.com/stretchr/testify/require"', '"github.com/stretchr/testify/require"\n "google.golang.org/grpc/codes"')
edit('k8s_runner_workload_test.go', '"remove_workload_with_volumes"', '"remove_workload_with_checked_volumes"')
old = '''		_, err := client.RemoveWorkload(ctx, &runnerv1.RemoveWorkloadRequest{
			WorkloadId:    workloadID,
			Force:         true,
			RemoveVolumes: true,
		})
		require.NoError(t, err)
		waitGone(t, ctx, client, workloadID)'''
edit('k8s_runner_workload_test.go', old, '''		expected := nativeVolume(t, ctx, client, volumeName)
		_, err := client.RemoveWorkload(ctx, &runnerv1.RemoveWorkloadRequest{
			WorkloadId: workloadID, Force: true, RemoveVolumes: true,
		})
		requireGRPCCode(t, err, codes.FailedPrecondition)
		_, err = client.RemoveWorkload(ctx, &runnerv1.RemoveWorkloadRequest{
			WorkloadId: workloadID, Force: true, RemoveVolumes: false,
		})
		require.NoError(t, err)
		waitGone(t, ctx, client, workloadID)
		removeNativeVolume(t, ctx, client, expected)''')
edit('k8s_runner_workload_test.go', '''		// RemoveWorkload deletes the PVCs it annotated, so the volume should be
		// gone. Asked of the listing, not of RemoveVolume: that answers nil for
		// a volume it does not have -- it left the runner holding [] and still
		// returned success -- so it cannot tell absence from a second removal.''', '''		// Independently confirm that bound removal removed the observed volume.''')
edit('k8s_runner_workload_test.go', 'RemoveWorkload(RemoveVolumes) left %s behind', 'checked volume removal left %s behind')
fixture = Path(__file__).with_name('native_volumes_test.go.txt').read_text()
for p, s in changes.items(): p.write_text(s)
(root / 'native_volumes_test.go').write_text(fixture)
