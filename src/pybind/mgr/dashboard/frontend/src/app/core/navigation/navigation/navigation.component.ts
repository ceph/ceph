import { Component, HostBinding, OnInit } from '@angular/core';

import { PrometheusService } from '../../../shared/api/prometheus.service';
import { Icons } from '../../../shared/enum/icons.enum';
import { Permissions } from '../../../shared/models/permissions';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import {
  FeatureTogglesMap$,
  FeatureTogglesService
} from '../../../shared/services/feature-toggles.service';
import { SummaryService } from '../../../shared/services/summary.service';

@Component({
  selector: 'cd-navigation',
  templateUrl: './navigation.component.html',
  styleUrls: ['./navigation.component.scss']
})
export class NavigationComponent implements OnInit {
  @HostBinding('class.isPwdDisplayed') isPwdDisplayed = false;

  permissions: Permissions;
  summaryData: any;
  icons = Icons;

  isAlertmanagerConfigured = false;
  isPrometheusConfigured = false;
  enabledFeature$: FeatureTogglesMap$;

  isCollapsed = true;
  showMenuSidebar = true;
  displayedSubMenu = '';

  simplebar = {
    autoHide: false
  };

  constructor(
    private authStorageService: AuthStorageService,
    private prometheusService: PrometheusService,
    private summaryService: SummaryService,
    private featureToggles: FeatureTogglesService
  ) {
    this.permissions = this.authStorageService.getPermissions();
  }

  ngOnInit() {
    this.enabledFeature$ = this.featureToggles.get();
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
    this.authStorageService.isPwdDisplayed$.subscribe((isDisplayed) => {
      this.isPwdDisplayed = isDisplayed;
    });
  }

  blockHealthColor() {
    if (this.summaryData && this.summaryData.rbd_mirroring) {
      if (this.summaryData.rbd_mirroring.errors > 0) {
        return { color: '#d9534f' };
      } else if (this.summaryData.rbd_mirroring.warnings > 0) {
        return { color: '#f0ad4e' };
      }
    }

    return undefined;
  }

  toggleSubMenu(menu: string) {
    if (this.displayedSubMenu === menu) {
      this.displayedSubMenu = '';
    } else {
      this.displayedSubMenu = menu;
    }
  }
}
