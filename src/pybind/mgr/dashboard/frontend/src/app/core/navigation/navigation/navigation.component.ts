import { Component, OnDestroy, OnInit } from '@angular/core';
import { Router } from '@angular/router';

import * as _ from 'lodash';
import { Subscription } from 'rxjs';
import { MultiClusterService } from '~/app/shared/api/multi-cluster.service';
import { SettingsService } from '~/app/shared/api/settings.service';

import { MultiCluster } from '~/app/shared/models/multi-cluster';
import { Permissions } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { CookiesService } from '~/app/shared/services/cookie.service';
import {
  FeatureTogglesMap$,
  FeatureTogglesService
} from '~/app/shared/services/feature-toggles.service';
import { PrometheusAlertService } from '~/app/shared/services/prometheus-alert.service';
import { SummaryService } from '~/app/shared/services/summary.service';

@Component({
  selector: 'cd-navigation',
  templateUrl: './navigation.component.html',
  styleUrls: ['./navigation.component.scss']
})
export class NavigationComponent implements OnInit, OnDestroy {
  clusterDetails: any[] = [];

  permissions: Permissions;
  enabledFeature$: FeatureTogglesMap$;
  clusterTokenStatus: object = {};
  summaryData: any;

  rightSidebarOpen = false; // rightSidebar only opens when width is less than 768px
  showMenuSidebar = true;

  simplebar = {
    autoHide: false
  };
  displayedSubMenu = {};
  private subs = new Subscription();

  clustersMap: Map<string, any> = new Map<string, any>();
  selectedCluster: {
    name: string;
    cluster_alias: string;
    user: string;
    cluster_connection_status?: number;
  };
  currentClusterName: string;

  constructor(
    private authStorageService: AuthStorageService,
    private multiClusterService: MultiClusterService,
    private router: Router,
    private summaryService: SummaryService,
    private featureToggles: FeatureTogglesService,
    public prometheusAlertService: PrometheusAlertService,
    private cookieService: CookiesService,
    private settingsService: SettingsService
  ) {
    this.permissions = this.authStorageService.getPermissions();
    this.enabledFeature$ = this.featureToggles.get();
  }

  ngOnInit() {
    this.subs.add(
      this.multiClusterService.subscribe((resp: object) => {
        const clustersConfig = resp['config'];
        if (clustersConfig) {
          this.clustersMap.clear();
          Object.keys(clustersConfig).forEach((clusterKey: string) => {
            const clusterDetailsList = clustersConfig[clusterKey];
            clusterDetailsList.forEach((clusterDetails: MultiCluster) => {
              const clusterUser = clusterDetails['user'];
              const clusterUrl = clusterDetails['url'];
              const clusterUniqueKey = `${clusterUrl}-${clusterUser}`;
              this.clustersMap.set(clusterUniqueKey, clusterDetails);
              this.checkClusterConnectionStatus();
            });
          });
          this.selectedCluster =
            this.clustersMap.get(`${resp['current_url']}-${resp['current_user']}`) || {};
          this.currentClusterName = `${this.selectedCluster?.name} - ${this.selectedCluster?.cluster_alias} - ${this.selectedCluster?.user}`;
        }
      })
    );

    this.subs.add(
      this.summaryService.subscribe((summary) => {
        this.summaryData = summary;
      })
    );
    this.subs.add(
      this.multiClusterService.subscribeClusterTokenStatus((resp: object) => {
        this.clusterTokenStatus = resp;
        this.checkClusterConnectionStatus();
      })
    );
  }

  ngOnDestroy(): void {
    this.subs.unsubscribe();
  }

  checkClusterConnectionStatus() {
    this.clustersMap.forEach((clusterDetails, clusterName) => {
      const clusterTokenStatus = this.clusterTokenStatus[clusterDetails.name];
      const connectionStatus = clusterTokenStatus ? clusterTokenStatus.status : 0;
      const user = clusterTokenStatus ? clusterTokenStatus.user : clusterDetails.user;

      this.clustersMap.set(clusterName, {
        ...clusterDetails,
        cluster_connection_status: connectionStatus,
        user: user
      });

      if (clusterDetails.cluster_alias === 'local-cluster') {
        this.clustersMap.set(clusterName, {
          ...clusterDetails,
          cluster_connection_status: 0,
          user: user
        });
      }
    });
  }

  blockHealthColor() {
    if (this.summaryData && this.summaryData.rbd_mirroring) {
      if (this.summaryData.rbd_mirroring.errors > 0) {
        return { color: '#f4926c' };
      } else if (this.summaryData.rbd_mirroring.warnings > 0) {
        return { color: '#f0ad4e' };
      }
    }

    return undefined;
  }

  toggleSubMenu(menu: string) {
    this.displayedSubMenu[menu] = !this.displayedSubMenu[menu];
  }

  toggleRightSidebar() {
    this.rightSidebarOpen = !this.rightSidebarOpen;
  }

  onClusterSelection(value: object) {
    this.multiClusterService.setCluster(value).subscribe(
      (resp: any) => {
        if (value['cluster_alias'] === 'local-cluster') {
          localStorage.setItem('cluster_api_url', '');
        } else {
          localStorage.setItem('current_cluster_name', `${value['name']}-${value['user']}`);
          localStorage.setItem('cluster_api_url', value['url']);
        }
        this.selectedCluster = this.clustersMap.get(`${value['url']}-${value['user']}`) || {};
        const clustersConfig = resp['config'];
        if (clustersConfig && typeof clustersConfig === 'object') {
          Object.keys(clustersConfig).forEach((clusterKey: string) => {
            const clusterDetailsList = clustersConfig[clusterKey];

            clusterDetailsList.forEach((clusterDetails: any) => {
              const clusterName = clusterDetails['name'];
              const clusterToken = clusterDetails['token'];
              const clusterUser = clusterDetails['user'];

              if (
                clusterName === this.selectedCluster['name'] &&
                clusterUser === this.selectedCluster['user'] &&
                clusterDetails['cluster_alias'] !== 'local-cluster'
              ) {
                this.cookieService.setToken(`${clusterName}-${clusterUser}`, clusterToken);
              }
            });
          });
        }
      },
      () => {},
      () => {
        // force refresh grafana api url to get the correct url for the selected cluster
        this.settingsService.ifSettingConfigured(
          'api/grafana/url',
          () => {},
          () => {},
          true
        );
        const currentRoute = this.router.url.split('?')[0];
        this.multiClusterService.refreshMultiCluster(currentRoute);
      }
    );
  }

  trackByFn(item: any) {
    return item;
  }
}
