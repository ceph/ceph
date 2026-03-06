import {
  Component,
  OnDestroy,
  OnInit,
  ViewEncapsulation,
  inject,
  signal,
  computed
} from '@angular/core';
import { Icons, IconSize } from '~/app/shared/enum/icons.enum';
import { PrometheusService } from '~/app/shared/api/prometheus.service';
import {
  METRIC_UNIT_MAP,
  PerformanceData,
  PerformanceType,
  StorageType
} from '~/app/shared/models/performance-data';
import { PerformanceCardService } from '../../api/performance-card.service';
import { DropdownModule, GridModule, LayoutModule, ListItem } from 'carbon-components-angular';
import { Subject, Subscription } from 'rxjs';
import { takeUntil } from 'rxjs/operators';
import { ProductiveCardComponent } from '../productive-card/productive-card.component';
import { CommonModule } from '@angular/common';
import { TimePickerComponent } from '../time-picker/time-picker.component';
import { AreaChartComponent } from '../area-chart/area-chart.component';
import { MgrModuleService } from '../../api/mgr-module.service';
import { toSignal } from '@angular/core/rxjs-interop';

@Component({
  selector: 'cd-performance-card',
  templateUrl: './performance-card.component.html',
  styleUrl: './performance-card.component.scss',
  standalone: true,
  imports: [
    ProductiveCardComponent,
    CommonModule,
    DropdownModule,
    AreaChartComponent,
    TimePickerComponent,
    LayoutModule,
    GridModule
  ],
  encapsulation: ViewEncapsulation.None
})
export class PerformanceCardComponent implements OnInit, OnDestroy {
  chartDataSignal = signal<PerformanceData | null>(null);
  chartDataLengthSignal = computed(() => {
    const data = this.chartDataSignal();
    return data ? Object.keys(data).length : 0;
  });
  performanceTypes = PerformanceType;
  metricUnitMap = METRIC_UNIT_MAP;
  icons = Icons;
  iconSize = IconSize;
  emptyStateText = {
    prometheusNotAvailable: $localize`You must have prometheus configured to access this capability.`,
    storageNotAvailable: $localize`You must have storage configured to access this capability.`,
    prometheusDisabled: $localize`You must enable prometheus to access this capability.`
  };
  emptyStateKey = signal<
    'prometheusNotAvailable' | 'storageNotAvailable' | 'prometheusDisabled' | ''
  >('prometheusNotAvailable');

  private destroy$ = new Subject<void>();

  storageTypes: ListItem[] = [
    { content: 'All', value: StorageType.All, selected: true },
    {
      content: 'Filesystem',
      value: StorageType.Filesystem,
      selected: false
    },
    {
      content: 'Block',
      value: StorageType.Block,
      selected: false
    },
    {
      content: 'Object',
      value: StorageType.Object,
      selected: false
    }
  ];

  selectedStorageType = StorageType.All;

  private prometheusService = inject(PrometheusService);
  private performanceCardService = inject(PerformanceCardService);
  private mgrModuleService = inject(MgrModuleService);

  time = { ...this.prometheusService.lastHourDateObject };
  private chartSub?: Subscription;

  readonly list = toSignal(this.mgrModuleService.list(), { initialValue: [] });

  ngOnInit() {
    this.loadCharts(this.time);
  }

  loadCharts(time: { start: number; end: number; step: number }) {
    this.time = { ...time };

    this.chartSub?.unsubscribe();

    this.chartSub = this.performanceCardService
      .getChartData(time, this.selectedStorageType)
      .pipe(takeUntil(this.destroy$))
      .subscribe((data) => {
        this.chartDataSignal.set(data);
        this.prometheusService.ifPrometheusConfigured(
          () => {
            let enabled$ = this.list().filter((a) => a.name === 'prometheus')[0].enabled;
            if (enabled$) {
              this.chartDataSignal.set(data);
              this.emptyStateKey.set('');
            } else if (!enabled$) {
              this.emptyStateKey.set('prometheusDisabled');
            } else {
              this.emptyStateKey.set('storageNotAvailable');
            }
          },
          () => {
            this.emptyStateKey.set('prometheusNotAvailable');
          }
        );
      });
  }

  onStorageTypeSelection(event: any) {
    this.selectedStorageType = event.item.value;
    this.loadCharts(this.time);
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
    this.chartSub?.unsubscribe();
  }
}
