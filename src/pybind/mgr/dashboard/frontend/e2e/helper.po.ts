import { browser } from 'protractor';
import { ImagesPageHelper } from './block/images.po';
import { IscsiPageHelper } from './block/iscsi.po';
import { MirroringPageHelper } from './block/mirroring.po';
import { AlertsPageHelper } from './cluster/alerts.po';
import { ConfigurationPageHelper } from './cluster/configuration.po';
import { CrushMapPageHelper } from './cluster/crush-map.po';
import { HostsPageHelper } from './cluster/hosts.po';
import { LogsPageHelper } from './cluster/logs.po';
import { ManagerModulesPageHelper } from './cluster/mgr-modules.po';
import { MonitorsPageHelper } from './cluster/monitors.po';
import { OSDsPageHelper } from './cluster/osds.po';
import { DashboardPageHelper } from './dashboard.po';
import { FilesystemsPageHelper } from './filesystems/filesystems.po';
import { NfsPageHelper } from './nfs/nfs.po';
import { PoolPageHelper } from './pools/pools.po';
import { BucketsPageHelper } from './rgw/buckets.po';
import { DaemonsPageHelper } from './rgw/daemons.po';
import { UsersPageHelper } from './rgw/users.po';
import { UserMgmtPageHelper } from './user-mgmt.po';

export class Helper {
  pools: PoolPageHelper;
  buckets: BucketsPageHelper;
  images: ImagesPageHelper;
  mirroring: MirroringPageHelper;
  dashboard: DashboardPageHelper;
  usermgmt: UserMgmtPageHelper;
  daemons: DaemonsPageHelper;
  users: UsersPageHelper;
  nfs: NfsPageHelper;
  filesystems: FilesystemsPageHelper;
  osds: OSDsPageHelper;
  monitors: MonitorsPageHelper;
  mgrModules: ManagerModulesPageHelper;
  logs: LogsPageHelper;
  hosts: HostsPageHelper;
  crushMap: CrushMapPageHelper;
  configuration: ConfigurationPageHelper;
  alerts: AlertsPageHelper;
  iscsi: IscsiPageHelper;

  constructor() {
    this.pools = new PoolPageHelper();
    this.buckets = new BucketsPageHelper();
    this.images = new ImagesPageHelper();
    this.iscsi = new IscsiPageHelper();
    this.mirroring = new MirroringPageHelper();
    this.dashboard = new DashboardPageHelper();
    this.usermgmt = new UserMgmtPageHelper();
    this.daemons = new DaemonsPageHelper();
    this.users = new UsersPageHelper();
    this.nfs = new NfsPageHelper();
    this.filesystems = new FilesystemsPageHelper();
    this.osds = new OSDsPageHelper();
    this.monitors = new MonitorsPageHelper();
    this.mgrModules = new ManagerModulesPageHelper();
    this.logs = new LogsPageHelper();
    this.hosts = new HostsPageHelper();
    this.crushMap = new CrushMapPageHelper();
    this.configuration = new ConfigurationPageHelper();
    this.alerts = new AlertsPageHelper();
    this.mirroring = new MirroringPageHelper();
    this.iscsi = new IscsiPageHelper();
    this.dashboard = new DashboardPageHelper();
  }

  /**
   * Checks if there are any errors on the browser
   *
   * @static
   * @memberof Helper
   */
  static async checkConsole() {
    let browserLog = await browser
      .manage()
      .logs()
      .get('browser');

    browserLog = browserLog.filter((log) => log.level.value > 900);

    if (browserLog.length > 0) {
      console.log('\n log: ' + require('util').inspect(browserLog));
    }

    await expect(browserLog.length).toEqual(0);
  }
}
