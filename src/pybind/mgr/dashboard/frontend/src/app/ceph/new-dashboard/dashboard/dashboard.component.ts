import { Component, OnDestroy, OnInit } from '@angular/core';

import _ from 'lodash';
import { Observable, Subscription } from 'rxjs';

import { ClusterService } from '~/app/shared/api/cluster.service';
import { ConfigurationService } from '~/app/shared/api/configuration.service';
import { MgrModuleService } from '~/app/shared/api/mgr-module.service';
import { OsdService } from '~/app/shared/api/osd.service';
import { DashboardDetails } from '~/app/shared/models/cd-details';
import { Permissions } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import {
  FeatureTogglesMap$,
  FeatureTogglesService
} from '~/app/shared/services/feature-toggles.service';
import { SummaryService } from '~/app/shared/services/summary.service';

@Component({
  selector: 'cd-dashboard',
  templateUrl: './dashboard.component.html',
  styleUrls: ['./dashboard.component.scss']
})
export class DashboardComponent implements OnInit, OnDestroy {
  detailsCardData: DashboardDetails = {};
  osdSettings$: Observable<any>;
  interval = new Subscription();
  permissions: Permissions;
  enabledFeature$: FeatureTogglesMap$;
  color: string;
  capacity$: Observable<any>;
  constructor(
    private summaryService: SummaryService,
    private configService: ConfigurationService,
    private mgrModuleService: MgrModuleService,
    private clusterService: ClusterService,
    private osdService: OsdService,
    private authStorageService: AuthStorageService,
    private featureToggles: FeatureTogglesService
  ) {
    this.permissions = this.authStorageService.getPermissions();
    this.enabledFeature$ = this.featureToggles.get();
  }

  ngOnInit() {
    this.getDetailsCardData();

    this.osdSettings$ = this.osdService.getOsdSettings();
    this.capacity$ = this.clusterService.getCapacity();
  }

  ngOnDestroy() {
    this.interval.unsubscribe();
  }

  getDetailsCardData() {
    this.configService.get('fsid').subscribe((data) => {
      this.detailsCardData.fsid = data['value'][0]['value'];
    });
    this.mgrModuleService.getConfig('orchestrator').subscribe((data) => {
      const orchStr = data['orchestrator'];
      this.detailsCardData.orchestrator = orchStr.charAt(0).toUpperCase() + orchStr.slice(1);
    });
    this.summaryService.subscribe((summary) => {
      const version = summary.version.replace('ceph version ', '').split(' ');
      this.detailsCardData.cephVersion =
        version[0] + ' ' + version.slice(2, version.length).join(' ');
    });
  }
}
