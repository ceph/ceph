import { Permissions } from './permissions';

describe('cd-notification classes', () => {
  it('should show empty permissions', () => {
    expect(new Permissions({})).toEqual({
      cephfs: { create: false, delete: false, read: false, update: false },
      configOpt: { create: false, delete: false, read: false, update: false },
      grafana: { create: false, delete: false, read: false, update: false },
      hosts: { create: false, delete: false, read: false, update: false },
      iscsi: { create: false, delete: false, read: false, update: false },
      log: { create: false, delete: false, read: false, update: false },
      manager: { create: false, delete: false, read: false, update: false },
      monitor: { create: false, delete: false, read: false, update: false },
      nfs: { create: false, delete: false, read: false, update: false },
      osd: { create: false, delete: false, read: false, update: false },
      pool: { create: false, delete: false, read: false, update: false },
      prometheus: { create: false, delete: false, read: false, update: false },
      rbdImage: { create: false, delete: false, read: false, update: false },
      rbdMirroring: { create: false, delete: false, read: false, update: false },
      rgw: { create: false, delete: false, read: false, update: false },
      user: { create: false, delete: false, read: false, update: false }
    });
  });

  it('should show full permissions', () => {
    const fullyGranted = {
      cephfs: ['create', 'read', 'update', 'delete'],
      'config-opt': ['create', 'read', 'update', 'delete'],
      grafana: ['create', 'read', 'update', 'delete'],
      hosts: ['create', 'read', 'update', 'delete'],
      iscsi: ['create', 'read', 'update', 'delete'],
      log: ['create', 'read', 'update', 'delete'],
      manager: ['create', 'read', 'update', 'delete'],
      monitor: ['create', 'read', 'update', 'delete'],
      osd: ['create', 'read', 'update', 'delete'],
      pool: ['create', 'read', 'update', 'delete'],
      prometheus: ['create', 'read', 'update', 'delete'],
      'rbd-image': ['create', 'read', 'update', 'delete'],
      'rbd-mirroring': ['create', 'read', 'update', 'delete'],
      rgw: ['create', 'read', 'update', 'delete'],
      user: ['create', 'read', 'update', 'delete']
    };
    expect(new Permissions(fullyGranted)).toEqual({
      cephfs: { create: true, delete: true, read: true, update: true },
      configOpt: { create: true, delete: true, read: true, update: true },
      grafana: { create: true, delete: true, read: true, update: true },
      hosts: { create: true, delete: true, read: true, update: true },
      iscsi: { create: true, delete: true, read: true, update: true },
      log: { create: true, delete: true, read: true, update: true },
      manager: { create: true, delete: true, read: true, update: true },
      monitor: { create: true, delete: true, read: true, update: true },
      nfs: { create: false, delete: false, read: false, update: false },
      osd: { create: true, delete: true, read: true, update: true },
      pool: { create: true, delete: true, read: true, update: true },
      prometheus: { create: true, delete: true, read: true, update: true },
      rbdImage: { create: true, delete: true, read: true, update: true },
      rbdMirroring: { create: true, delete: true, read: true, update: true },
      rgw: { create: true, delete: true, read: true, update: true },
      user: { create: true, delete: true, read: true, update: true }
    });
  });
});
