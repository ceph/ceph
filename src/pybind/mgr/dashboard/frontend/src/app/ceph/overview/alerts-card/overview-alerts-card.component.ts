import {
  ChangeDetectionStrategy,
  Component,
  Input,
  OnInit,
  ViewEncapsulation,
  inject
} from '@angular/core';
import { CommonModule } from '@angular/common';
import { combineLatest } from 'rxjs';

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
import { ComponentsModule } from '~/app/shared/components/components.module';
import { map, shareReplay, startWith } from 'rxjs/operators';
import { PipesModule } from '~/app/shared/pipes/pipes.module';

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
    ComponentsModule,
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
  private readonly prometheusAlertService = inject(PrometheusAlertService);

  ngOnInit(): void {
    this.prometheusAlertService.getGroupedAlerts(true);
  }

  readonly vm$ = combineLatest([
    this.prometheusAlertService.criticalAlerts$.pipe(startWith(0)),
    this.prometheusAlertService.warningAlerts$.pipe(startWith(0))
  ]).pipe(
    map(([critical, warning]) => {
      const total = (critical ?? 0) + (warning ?? 0);
      const hasAlerts = total > 0;
      const hasCritical = critical > 0;
      const hasWarning = warning > 0;

      const icon = !hasAlerts
        ? AlertIcon.success
        : hasCritical
        ? AlertIcon.error
        : AlertIcon.warning;

      const statusText = hasAlerts ? $localize`Need attention` : $localize`No active alerts`;

      const badges = [
        hasCritical && { key: 'critical', icon: AlertIcon.error, count: critical },
        hasWarning && { key: 'warning', icon: AlertIcon.warning, count: warning }
      ].filter(Boolean);

      return { total, icon, statusText, badges };
    }),
    shareReplay({ bufferSize: 1, refCount: true })
  );
}
