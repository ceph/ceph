import { Component } from '@angular/core';

import { PrometheusAlertService } from '~/app/shared/services/prometheus-alert.service';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { Permission } from '~/app/shared/models/permissions';

@Component({
  selector: 'cd-prometheus-tabs',
  templateUrl: './prometheus-tabs.component.html',
  styleUrls: ['./prometheus-tabs.component.scss'],
  standalone: false
})
export class PrometheusTabsComponent {
  prometheusPermissions: Permission;

  constructor(
    public prometheusAlertService: PrometheusAlertService,
    private authStorageService: AuthStorageService
  ) {
    this.prometheusPermissions = this.authStorageService.getPermissions().prometheus;
  }
}
