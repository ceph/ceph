import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';

import { PrometheusAlertService } from '~/app/shared/services/prometheus-alert.service';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { Permission } from '~/app/shared/models/permissions';

const MONITORING_PATH = 'monitoring';

enum TABS {
  activeAlerts = 'active-alerts',
  silences = 'silences',
  alerts = 'alerts'
}

@Component({
  selector: 'cd-prometheus-tabs',
  templateUrl: './prometheus-tabs.component.html',
  styleUrls: ['./prometheus-tabs.component.scss'],
  standalone: false
})
export class PrometheusTabsComponent implements OnInit {
  selectedTab: TABS;
  activeTab: TABS = TABS.activeAlerts;
  prometheusPermissions: Permission;

  constructor(
    public prometheusAlertService: PrometheusAlertService,
    private router: Router,
    private authStorageService: AuthStorageService
  ) {
    this.prometheusPermissions = this.authStorageService.getPermissions().prometheus;
  }

  ngOnInit(): void {
    const currentPath = this.router.url;
    this.activeTab = Object.values(TABS).find((t) => currentPath.includes(t)) || TABS.activeAlerts;
  }

  onSelected(tab: TABS) {
    this.selectedTab = tab;
    this.router.navigate([`${MONITORING_PATH}/${tab}`]);
  }

  public get Tabs(): typeof TABS {
    return TABS;
  }
}
