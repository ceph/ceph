import { Component, OnInit } from '@angular/core';

import { PrometheusService } from '../../../shared/api/prometheus.service';
import { Permissions } from '../../../shared/models/permissions';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import {
  FeatureTogglesMap$,
  FeatureTogglesService
} from '../../../shared/services/feature-toggles.service';
import { PrometheusAlertService } from '../../../shared/services/prometheus-alert.service';
import { SummaryService } from '../../../shared/services/summary.service';

@Component({
  selector: 'cd-navigation',
  templateUrl: './navigation.component.html',
  styleUrls: ['./navigation.component.scss']
})
export class NavigationComponent implements OnInit {
  permissions: Permissions;
  summaryData: any;

  isCollapsed = true;
  isAlertmanagerConfigured = false;
  isPrometheusConfigured = false;
  enabledFeature$: FeatureTogglesMap$;

  constructor(
    private authStorageService: AuthStorageService,
    private prometheusService: PrometheusService,
    private summaryService: SummaryService,
    private featureToggles: FeatureTogglesService,
    public prometheusAlertService: PrometheusAlertService
  ) {
    this.permissions = this.authStorageService.getPermissions();
    this.enabledFeature$ = this.featureToggles.get();
  }

  ngOnInit() {
    this.summaryService.subscribe((data: any) => {
      if (!data) {
        return;
      }
      this.summaryData = data;
    });
    if (this.permissions.configOpt.read) {
      this.prometheusService.ifAlertmanagerConfigured(() => {
        this.isAlertmanagerConfigured = true;
      });
      this.prometheusService.ifPrometheusConfigured(() => {
        this.isPrometheusConfigured = true;
      });
    }
  }

  blockHealthColor() {
    if (this.summaryData && this.summaryData.rbd_mirroring) {
      if (this.summaryData.rbd_mirroring.errors > 0) {
        return { color: '#d9534f' };
      } else if (this.summaryData.rbd_mirroring.warnings > 0) {
        return { color: '#f0ad4e' };
      }
    }
  }
}
