import {
  ChangeDetectionStrategy,
  Component,
  Input,
  OnInit,
  ViewEncapsulation,
  inject
} from '@angular/core';
import { PrometheusAlertService } from '~/app/shared/services/prometheus-alert.service';
import {
  ButtonModule,
  GridModule,
  LayoutModule,
  LinkModule,
  SkeletonModule,
  TilesModule
} from 'carbon-components-angular';
import { RouterModule } from '@angular/router';
import { ProductiveCardComponent } from '~/app/shared/components/productive-card/productive-card.component';
import { CommonModule } from '@angular/common';
import { map, shareReplay, startWith } from 'rxjs/operators';
import { PipesModule } from '~/app/shared/pipes/pipes.module';
import { IconComponent } from '~/app/shared/components/icon/icon.component';

const AlertIcon = {
  error: 'error',
  warning: 'warning',
  success: 'success'
};

@Component({
  selector: 'cd-overview-alerts-card',
  standalone: true,
  imports: [
    CommonModule,
    GridModule,
    TilesModule,
    IconComponent,
    RouterModule,
    ProductiveCardComponent,
    ButtonModule,
    LinkModule,
    LayoutModule,
    PipesModule,
    SkeletonModule
  ],
  templateUrl: './overview-alerts-card.component.html',
  styleUrl: './overview-alerts-card.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
  encapsulation: ViewEncapsulation.None
})
export class OverviewAlertsCardComponent implements OnInit {
  @Input() compact = true;
  @Input() filterType: 'block' | 'file' | 'object' | 'all' = 'all';
  private readonly prometheusAlertService = inject(PrometheusAlertService);

  ngOnInit(): void {
    this.prometheusAlertService.getGroupedAlerts(true);
  }

  readonly vm$ = this.prometheusAlertService.alerts$.pipe(
    startWith([]),
    map((alerts) => {
      const filtered = (alerts || []).filter((alert) => {
        if (alert.status?.state !== 'active') return false;
        if (this.filterType === 'all') return true;
        const name = (alert.labels?.alertname || '').toLowerCase();
        if (this.filterType === 'block') {
          // Block storage includes RBD, NVMe-oF, iSCSI, TCMU, and OSD errors
          return (
            name.includes('rbd') ||
            name.includes('nvme') ||
            name.includes('iscsi') ||
            name.includes('tcmu') ||
            name.includes('osd')
          );
        }
        if (this.filterType === 'file') {
          // File storage includes CephFS and MDS (Metadata Servers)
          return name.includes('cephfs') || name.includes('mds') || name.includes('filesystem');
        }
        if (this.filterType === 'object') {
          // Object storage includes RGW (RADOS Gateway) and Multisite sync
          return name.includes('rgw') || name.includes('multisite') || name.includes('bucket');
        }
        return true;
      });

      const critical = filtered.filter((alert) => alert.labels?.severity === 'critical').length;
      const warning = filtered.filter((alert) => alert.labels?.severity === 'warning').length;
      const total = critical + warning;
      const hasCritical = critical > 0;
      const hasWarning = warning > 0;

      const icon = hasCritical
        ? AlertIcon.error
        : hasWarning
          ? AlertIcon.warning
          : AlertIcon.success;

      const statusText = hasCritical
        ? $localize`Need attention`
        : hasWarning
          ? $localize`Warning`
          : $localize`No active alerts`;

      const badges = [
        hasCritical && { key: 'critical', icon: AlertIcon.error, count: critical },
        hasWarning && { key: 'warning', icon: AlertIcon.warning, count: warning }
      ].filter(Boolean);

      return { total, icon, statusText, badges };
    }),
    shareReplay({ bufferSize: 1, refCount: true })
  );
}
