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
import { catchError, filter, map, startWith } from 'rxjs/operators';
import { HealthCardVM } from '~/app/shared/models/overview';

type OverviewHealthData = {
  summary: Summary;
  upgrade: UpgradeInfoInterface;
};

type TabSection = 'system' | 'hardware' | 'resiliency';

interface HealthItemConfig {
  key: 'mon' | 'mgr' | 'osd' | 'hosts';
  label: string;
  prefix?: string;
  i18n?: boolean;
}

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

  @Input({ required: true }) vm!: HealthCardVM;
  @Output() viewIncidents = new EventEmitter<void>();

  activeSection: TabSection | null = null;
  healthItems: HealthItemConfig[] = [
    { key: 'mon', label: $localize`Monitor` },
    { key: 'mgr', label: $localize`Manager` },
    { key: 'osd', label: $localize`OSD` },
    { key: 'hosts', label: $localize`Nodes` }
  ];

  toggleSection(section: TabSection) {
    this.activeSection = this.activeSection === section ? null : section;
  }

  readonly data$: Observable<OverviewHealthData> = combineLatest([
    this.summaryService.summaryData$.pipe(filter((summary): summary is Summary => !!summary)),
    this.upgradeService.listCached().pipe(
      startWith(null as UpgradeInfoInterface),
      catchError(() => of(null))
    )
  ]).pipe(map(([summary, upgrade]) => ({ summary, upgrade })));

  onViewIncidentsClick() {
    this.viewIncidents.emit();
  }
}
