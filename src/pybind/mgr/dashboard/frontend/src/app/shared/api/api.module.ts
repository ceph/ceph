import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';

import { AuthService } from './auth.service';
import { CephfsService } from './cephfs.service';
import { ConfigurationService } from './configuration.service';
import { DashboardService } from './dashboard.service';
import { HostService } from './host.service';
import { LoggingService } from './logging.service';
import { MonitorService } from './monitor.service';
import { OsdService } from './osd.service';
import { PerformanceCounterService } from './performance-counter.service';
import { PoolService } from './pool.service';
import { RbdMirroringService } from './rbd-mirroring.service';
import { RbdService } from './rbd.service';
import { RgwBucketService } from './rgw-bucket.service';
import { RgwDaemonService } from './rgw-daemon.service';
import { RgwUserService } from './rgw-user.service';
import { TcmuIscsiService } from './tcmu-iscsi.service';

@NgModule({
  imports: [CommonModule],
  declarations: [],
  providers: [
    AuthService,
    CephfsService,
    ConfigurationService,
    DashboardService,
    HostService,
    MonitorService,
    OsdService,
    PoolService,
    RbdService,
    RbdMirroringService,
    RgwBucketService,
    RgwDaemonService,
    RgwUserService,
    PerformanceCounterService,
    LoggingService,
    TcmuIscsiService
  ]
})
export class ApiModule {}
