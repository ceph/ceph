import { Component, OnDestroy, OnInit } from '@angular/core';

import _ from 'lodash';
import { Subscription } from 'rxjs';

import { HealthService } from '~/app/shared/api/health.service';
import { Permissions } from '~/app/shared/models/permissions';
import { RefreshIntervalService } from '~/app/shared/services/refresh-interval.service';
import { RgwDaemonService } from '~/app/shared/api/rgw-daemon.service';
import { RgwRealmService } from '~/app/shared/api/rgw-realm.service';
import { RgwZoneService } from '~/app/shared/api/rgw-zone.service';
import { RgwZonegroupService } from '~/app/shared/api/rgw-zonegroup.service';
import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';
import { RgwUserService } from '~/app/shared/api/rgw-user.service';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import {
  FeatureTogglesMap$,
  FeatureTogglesService
} from '~/app/shared/services/feature-toggles.service';

@Component({
  selector: 'cd-rgw-overview-dashboard',
  templateUrl: './rgw-overview-dashboard.component.html',
  styleUrls: ['./rgw-overview-dashboard.component.scss']
})
export class RgwOverviewDashboardComponent implements OnInit, OnDestroy {
  interval = new Subscription();
  permissions: Permissions;
  enabledFeature$: FeatureTogglesMap$;
  rgwDaemonCount = 0;
  rgwRealmCount = 0;
  rgwZonegroupCount = 0;
  rgwZoneCount = 0;
  rgwBucketCount = 0;
  objectCount = 0;
  UserCount = 0;
  totalPoolUsedBytes = 0;
  averageObjectSize = 0;
  realmData: any;
  daemonSub: Subscription;
  realmSub: Subscription;
  ZonegroupSub: Subscription;
  ZoneSUb: Subscription;
  UserSub: Subscription;
  HealthSub: Subscription;
  BucketSub: Subscription;

  constructor(
    private authStorageService: AuthStorageService,
    private featureToggles: FeatureTogglesService,
    private healthService: HealthService,
    private refreshIntervalService: RefreshIntervalService,
    private rgwDaemonService: RgwDaemonService,
    private rgwRealmService: RgwRealmService,
    private rgwZonegroupService: RgwZonegroupService,
    private rgwZoneService: RgwZoneService,
    private rgwBucketService: RgwBucketService,
    private rgwUserService: RgwUserService
  ) {
    this.permissions = this.authStorageService.getPermissions();
    this.enabledFeature$ = this.featureToggles.get();
  }

  ngOnInit() {
    this.interval = this.refreshIntervalService.intervalData$.subscribe(() => {
      this.daemonSub = this.rgwDaemonService.list().subscribe((data: any) => {
        this.rgwDaemonCount = data.length;
      });
      this.BucketSub = this.rgwBucketService.list().subscribe((data: any) => {
        this.rgwBucketCount = data.length;
      });
      this.UserSub = this.rgwUserService.list().subscribe((data: any) => {
        this.UserCount = data.length;
      });
      this.HealthSub = this.healthService.getClusterCapacity().subscribe((data: any) => {
        this.objectCount = data['total_objects'];
        this.totalPoolUsedBytes = data['total_pool_bytes_used'];
        this.averageObjectSize = data['average_object_size'];
      });
    });
    this.realmSub = this.rgwRealmService.list().subscribe((data: any) => {
      this.rgwRealmCount = data['realms'].length;
    });
    this.ZonegroupSub = this.rgwZonegroupService.list().subscribe((data: any) => {
      this.rgwZonegroupCount = data['zonegroups'].length;
    });
    this.ZoneSUb = this.rgwZoneService.list().subscribe((data: any) => {
      this.rgwZoneCount = data['zones'].length;
    });
  }

  ngOnDestroy() {
    this.interval.unsubscribe();
    this.daemonSub.unsubscribe();
    this.realmSub.unsubscribe();
    this.ZonegroupSub.unsubscribe();
    this.ZoneSUb.unsubscribe();
    this.BucketSub.unsubscribe();
    this.UserSub.unsubscribe();
    this.HealthSub.unsubscribe();
  }
}
