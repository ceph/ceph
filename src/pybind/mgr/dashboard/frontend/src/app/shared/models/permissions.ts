export class Permission {
  read: boolean;
  create: boolean;
  update: boolean;
  delete: boolean;

  constructor(serverPermission: Array<string> = []) {
    ['read', 'create', 'update', 'delete'].forEach(
      (permission) => (this[permission] = serverPermission.includes(permission))
    );
  }
}

export class Permissions {
  hosts: Permission;
  configOpt: Permission;
  pool: Permission;
  osd: Permission;
  monitor: Permission;
  rbdImage: Permission;
  iscsi: Permission;
  rbdMirroring: Permission;
  rgw: Permission;
  cephfs: Permission;
  manager: Permission;
  log: Permission;
  user: Permission;
  grafana: Permission;
  prometheus: Permission;
  nfs: Permission;

  constructor(serverPermissions: any) {
    this.hosts = new Permission(serverPermissions['hosts']);
    this.configOpt = new Permission(serverPermissions['config-opt']);
    this.pool = new Permission(serverPermissions['pool']);
    this.osd = new Permission(serverPermissions['osd']);
    this.monitor = new Permission(serverPermissions['monitor']);
    this.rbdImage = new Permission(serverPermissions['rbd-image']);
    this.iscsi = new Permission(serverPermissions['iscsi']);
    this.rbdMirroring = new Permission(serverPermissions['rbd-mirroring']);
    this.rgw = new Permission(serverPermissions['rgw']);
    this.cephfs = new Permission(serverPermissions['cephfs']);
    this.manager = new Permission(serverPermissions['manager']);
    this.log = new Permission(serverPermissions['log']);
    this.user = new Permission(serverPermissions['user']);
    this.grafana = new Permission(serverPermissions['grafana']);
    this.prometheus = new Permission(serverPermissions['prometheus']);
    this.nfs = new Permission(serverPermissions['nfs-ganesha']);
  }
}
