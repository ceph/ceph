import { Component } from '@angular/core';

import { PrometheusAlertService } from '~/app/shared/services/prometheus-alert.service';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';

@Component({
  selector: 'cd-prometheus-tabs',
  templateUrl: './prometheus-tabs.component.html',
  styleUrls: ['./prometheus-tabs.component.scss'],
  standalone: false
})
export class PrometheusTabsComponent {
  canViewSilences: boolean;

  constructor(
    public prometheusAlertService: PrometheusAlertService,
    private authStorageService: AuthStorageService
  ) {
    const prometheusPermission = this.authStorageService.getPermissions().prometheus;
    this.canViewSilences = prometheusPermission.read;
  }
}
