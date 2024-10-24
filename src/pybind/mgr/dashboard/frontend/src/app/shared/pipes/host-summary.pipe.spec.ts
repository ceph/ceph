import { HostList } from '../models/host-schema';
import { HostSummaryPipe } from './host-summary.pipe';

describe('HostSummaryPipe', () => {
  const mockHosts: HostList = [
    {
      ceph_version: '',
      services: [],
      sources: {
        ceph: false,
        orchestrator: true
      },
      hostname: 'ceph-node-00',
      addr: '192.168.100.100',
      labels: ['_admin'],
      status: ''
    },
    {
      ceph_version: '',
      services: [],
      sources: {
        ceph: false,
        orchestrator: true
      },
      hostname: 'ceph-node-01',
      addr: '192.168.100.101',
      labels: [],
      status: 'Offline'
    },
    {
      ceph_version: '',
      services: [],
      sources: {
        ceph: false,
        orchestrator: true
      },
      hostname: 'ceph-node-02',
      addr: '192.168.100.102',
      labels: [],
      status: 'maintenance'
    }
  ];

  const pipe = new HostSummaryPipe();

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('transforms all hosts', () => {
    expect(pipe.transform(mockHosts)).toEqual({
      success: 1,
      warn: 1,
      error: 1,
      total: 3
    });
  });
});
