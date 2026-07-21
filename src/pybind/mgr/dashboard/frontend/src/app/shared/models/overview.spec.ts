import { buildHealthCardVM } from './overview';
import { HealthSnapshotMap } from './health.interface';

function makeSnapshot(checks: Record<string, any> = {}): HealthSnapshotMap {
  return {
    fsid: 'test-fsid',
    health: { status: 'HEALTH_OK', checks, mutes: [] },
    monmap: { num_mons: 3 },
    osdmap: { in: 3, up: 3, num_osds: 3 },
    pgmap: {
      pgs_by_state: [{ state_name: 'active+clean', count: 100 }],
      num_pools: 1,
      bytes_used: 0,
      bytes_total: 1000,
      num_pgs: 100,
      write_bytes_sec: 0,
      read_bytes_sec: 0,
      recovering_bytes_per_sec: 0
    },
    mgrmap: { num_active: 1, num_standbys: 1 },
    fsmap: { num_active: 0, num_standbys: 0 },
    num_rgw_gateways: 0,
    num_iscsi_gateways: { up: 0, down: 0 },
    num_hosts: 3
  } as any;
}

describe('buildHealthCardVM', () => {
  it('should not include pgAlertCount in the VM', () => {
    const vm = buildHealthCardVM(makeSnapshot());
    expect('pgAlertCount' in vm).toBe(false);
  });

  it('should build resiliencyHealth from health checks', () => {
    const vm = buildHealthCardVM(
      makeSnapshot({
        PG_DEGRADED: {
          severity: 'HEALTH_WARN',
          summary: { message: 'degraded', count: 5 },
          muted: false
        }
      })
    );
    expect(vm.resiliencyHealth.severity).toBe('warn');
  });

  it('should report ok resiliency when no PG checks', () => {
    const vm = buildHealthCardVM(makeSnapshot());
    expect(vm.resiliencyHealth.severity).toBe('ok');
  });
});
