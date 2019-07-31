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

export class Helper {
  static EC = browser.ExpectedConditions;
  static TIMEOUT = 10000;

  buckets: BucketsPageHelper;
  daemons: DaemonsPageHelper;
  users: UsersPageHelper;
  pools: PoolPageHelper;
  nfs: NfsPageHelper;
  filesystems: FilesystemsPageHelper;
  alerts: AlertsPageHelper;
  configuration: ConfigurationPageHelper;
  crushmap: CrushMapPageHelper;
  hosts: HostsPageHelper;
  logs: LogsPageHelper;
  mgrmodules: ManagerModulesPageHelper;
  monitors: MonitorsPageHelper;
  osds: OSDsPageHelper;
  images: ImagesPageHelper;
  iscsi: IscsiPageHelper;
  mirroring: MirroringPageHelper;
  dashboard: DashboardPageHelper;

  constructor() {
    this.buckets = new BucketsPageHelper();
    this.daemons = new DaemonsPageHelper();
    this.users = new UsersPageHelper();
    this.pools = new PoolPageHelper();
    this.nfs = new NfsPageHelper();
    this.filesystems = new FilesystemsPageHelper();
    this.alerts = new AlertsPageHelper();
    this.configuration = new ConfigurationPageHelper();
    this.crushmap = new CrushMapPageHelper();
    this.hosts = new HostsPageHelper();
    this.logs = new LogsPageHelper();
    this.mgrmodules = new ManagerModulesPageHelper();
    this.monitors = new MonitorsPageHelper();
    this.osds = new OSDsPageHelper();
    this.images = new ImagesPageHelper();
    this.iscsi = new IscsiPageHelper();
    this.mirroring = new MirroringPageHelper();
    this.dashboard = new DashboardPageHelper();
  }

  /**
   * Checks if there are any errors on the browser
   *
   * @static
   * @memberof Helper
   */
  static checkConsole() {
    browser
      .manage()
      .logs()
      .get('browser')
      .then(function(browserLog) {
        browserLog = browserLog.filter((log) => {
          return log.level.value > 900; // SEVERE level
        });

        if (browserLog.length > 0) {
          console.log('\n log: ' + require('util').inspect(browserLog));
        }

        expect(browserLog.length).toEqual(0);
      });
  }
}
