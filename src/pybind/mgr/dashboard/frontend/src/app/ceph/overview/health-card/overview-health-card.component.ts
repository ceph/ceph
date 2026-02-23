import {
  ChangeDetectionStrategy,
  Component,
  EventEmitter,
  inject,
  Input,
  Output,
  ViewEncapsulation
} from '@angular/core';
import {
  SkeletonModule,
  ButtonModule,
  LinkModule,
  TooltipModule,
  TabsModule,
  LayoutModule
} from 'carbon-components-angular';
import { ProductiveCardComponent } from '~/app/shared/components/productive-card/productive-card.component';
import { RouterModule } from '@angular/router';
import { ComponentsModule } from '~/app/shared/components/components.module';
import { SummaryService } from '~/app/shared/services/summary.service';
import { Summary } from '~/app/shared/models/summary.model';
import { combineLatest, Observable, of } from 'rxjs';
import { CommonModule } from '@angular/common';
import { PipesModule } from '~/app/shared/pipes/pipes.module';
import { UpgradeInfoInterface } from '~/app/shared/models/upgrade.interface';
import { UpgradeService } from '~/app/shared/api/upgrade.service';
import { catchError, filter, map, shareReplay, startWith, switchMap } from 'rxjs/operators';
import { HealthCardTabSection, HealthCardVM } from '~/app/shared/models/overview';
import { HardwareService } from '~/app/shared/api/hardware.service';
import { MgrModuleService } from '~/app/shared/api/mgr-module.service';
import { RefreshIntervalService } from '~/app/shared/services/refresh-interval.service';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { HardwareNameMapping } from '~/app/shared/enum/hardware.enum';

type OverviewHealthData = {
  summary: Summary;
  upgrade: UpgradeInfoInterface | null;
};

interface HealthItemConfig {
  key: 'mon' | 'mgr' | 'osd' | 'hosts';
  label: string;
  prefix?: string;
  i18n?: boolean;
}

type HwKey = keyof typeof HardwareNameMapping;

type HwRowVM = {
  key: HwKey;
  label: string;
  ok: number;
  error: number;
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
    TooltipModule,
    TabsModule,
    LayoutModule
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
  private readonly hardwareService = inject(HardwareService);
  private readonly mgrModuleService = inject(MgrModuleService);
  private readonly refreshIntervalService = inject(RefreshIntervalService);
  private readonly authStorageService = inject(AuthStorageService);

  @Input({ required: true }) vm!: HealthCardVM;
  @Output() viewIncidents = new EventEmitter<void>();
  @Output() activeSectionChange = new EventEmitter<HealthCardTabSection | null>();

  activeSection: HealthCardTabSection | null = null;

  healthItems: HealthItemConfig[] = [
    { key: 'mon', label: $localize`Monitor` },
    { key: 'mgr', label: $localize`Manager` },
    { key: 'osd', label: $localize`OSD` },
    { key: 'hosts', label: $localize`Nodes` }
  ];

  toggleSection(section: HealthCardTabSection) {
    this.activeSection = this.activeSection === section ? null : section;
    this.activeSectionChange.emit(this.activeSection);
  }

  readonly data$: Observable<OverviewHealthData> = combineLatest([
    this.summaryService.summaryData$.pipe(filter((summary): summary is Summary => !!summary)),
    this.upgradeService.listCached().pipe(
      startWith(null as UpgradeInfoInterface | null),
      catchError(() => of(null))
    )
  ]).pipe(map(([summary, upgrade]) => ({ summary, upgrade })));

  onViewIncidentsClick() {
    this.viewIncidents.emit();
  }

  private readonly permissions = this.authStorageService.getPermissions();

  readonly enabled$: Observable<boolean> = this.permissions?.configOpt?.read
    ? this.mgrModuleService.getConfig('cephadm').pipe(
        map((resp: any) => !!resp?.hw_monitoring),
        catchError(() => of(false)),
        shareReplay({ bufferSize: 1, refCount: true })
      )
    : of(false);

  private readonly hardwareSummary$ = this.enabled$.pipe(
    switchMap((enabled) => {
      if (!enabled) return of(null);

      return this.refreshIntervalService.intervalData$.pipe(
        startWith(null),
        switchMap(() => this.hardwareService.getSummary().pipe(catchError(() => of(null))))
      );
    }),
    shareReplay({ bufferSize: 1, refCount: true })
  );

  private readonly hardwareRows$: Observable<HwRowVM[] | null> = this.hardwareSummary$.pipe(
    map((hw) => {
      const category = hw?.total?.category;
      if (!category) return null;

      return (Object.keys(HardwareNameMapping) as HwKey[]).map((key) => ({
        key,
        label: HardwareNameMapping[key],
        ok: Number(category?.[key]?.ok ?? 0),
        error: Number(category?.[key]?.error ?? 0)
      }));
    }),
    shareReplay({ bufferSize: 1, refCount: true })
  );

  readonly sections$: Observable<HwRowVM[][] | null> = this.hardwareRows$.pipe(
    map((rows) => {
      if (!rows) return null;

      const result: HwRowVM[][] = [];
      for (let i = 0; i < rows.length; i += 2) {
        result.push(rows.slice(i, i + 2));
      }
      return result.slice(0, 3);
    }),
    shareReplay({ bufferSize: 1, refCount: true })
  );
}
