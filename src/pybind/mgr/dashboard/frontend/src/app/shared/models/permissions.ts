export class Permission {
  read: boolean;
  create: boolean;
  update: boolean;
  delete: boolean;

  constructor(serverPermission: Array<string> = []) {
    this.read = serverPermission.indexOf('read') !== -1;
    this.create = serverPermission.indexOf('create') !== -1;
    this.update = serverPermission.indexOf('update') !== -1;
    this.delete = serverPermission.indexOf('delete') !== -1;
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
  }
}
