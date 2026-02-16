import {
  ChangeDetectionStrategy,
  Component,
  EventEmitter,
  inject,
  Input,
  Output,
  ViewEncapsulation
} from '@angular/core';
import { SkeletonModule, ButtonModule, LinkModule, TooltipModule } from 'carbon-components-angular';
import { ProductiveCardComponent } from '~/app/shared/components/productive-card/productive-card.component';
import { RouterModule } from '@angular/router';
import { ComponentsModule } from '~/app/shared/components/components.module';
import { SummaryService } from '~/app/shared/services/summary.service';
import { Summary } from '~/app/shared/models/summary.model';
import { combineLatest, Observable, of, ReplaySubject } from 'rxjs';
import { CommonModule } from '@angular/common';
import { PipesModule } from '~/app/shared/pipes/pipes.module';
import { UpgradeInfoInterface } from '~/app/shared/models/upgrade.interface';
import { UpgradeService } from '~/app/shared/api/upgrade.service';
import { catchError, filter, map, startWith } from 'rxjs/operators';
import { HealthIconMap, HealthStatus } from '~/app/shared/models/overview';

type OverviewHealthData = {
  summary: Summary;
  upgrade: UpgradeInfoInterface;
  currentHealth: Health;
};

type Health = {
  message: string;
  title: string;
  icon: string;
};

const WarnAndErrMessage = $localize`There are active alerts and unresolved health warnings.`;

const HealthMap: Record<HealthStatus, Health> = {
  HEALTH_OK: {
    message: $localize`All core services are running normally`,
    icon: HealthIconMap['HEALTH_OK'],
    title: $localize`Healthy`
  },
  HEALTH_WARN: {
    message: WarnAndErrMessage,
    icon: HealthIconMap['HEALTH_WARN'],
    title: $localize`Warning`
  },
  HEALTH_ERR: {
    message: WarnAndErrMessage,
    icon: HealthIconMap['HEALTH_ERR'],
    title: $localize`Critical`
  }
};

@Component({
  selector: 'cd-overview-health-card',
  imports: [
    CommonModule,
    ProductiveCardComponent,
    SkeletonModule,
    ButtonModule,
    RouterModule,
    ComponentsModule,
    LinkModule,
    PipesModule,
    TooltipModule
  ],
  standalone: true,
  templateUrl: './overview-health-card.component.html',
  styleUrl: './overview-health-card.component.scss',
  encapsulation: ViewEncapsulation.None,
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class OverviewHealthCardComponent {
  private readonly summaryService = inject(SummaryService);
  private readonly upgradeService = inject(UpgradeService);

  @Input() fsid!: string;
  @Input()
  set status(value: HealthStatus) {
    this.health$.next(value);
  }
  @Input() incidents!: number;
  @Output() viewIncidents = new EventEmitter<void>();

  private health$ = new ReplaySubject<HealthStatus>(1);

  readonly data$: Observable<OverviewHealthData> = combineLatest([
    this.summaryService.summaryData$.pipe(filter((summary): summary is Summary => !!summary)),
    this.upgradeService.listCached().pipe(
      startWith(null as UpgradeInfoInterface),
      catchError(() => of(null))
    ),
    this.health$
  ]).pipe(
    map(([summary, upgrade, health]) => ({ summary, upgrade, currentHealth: HealthMap?.[health] }))
  );

  onViewIncidentsClick() {
    this.viewIncidents.emit();
  }
}
