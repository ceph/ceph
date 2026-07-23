import {
  Component,
  OnDestroy,
  OnInit,
  ViewEncapsulation,
  inject,
  signal,
  computed,
  Input
} from '@angular/core';
import { EMPTY_STATE_IMAGE, Icons, IconSize } from '~/app/shared/enum/icons.enum';
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
import { EmptyStateComponent } from '../empty-state/empty-state.component';

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
    GridModule,
    EmptyStateComponent
  ],
  encapsulation: ViewEncapsulation.None
})
export class PerformanceCardComponent implements OnInit, OnDestroy {
  @Input() prometheusEmptyState: boolean = false;
  @Input() storageEmptyState: boolean = false;

  chartDataSignal = signal<PerformanceData | null>(null);
  chartDataLengthSignal = computed(() => {
    const data = this.chartDataSignal();
    return data ? Object.keys(data).length : 0;
  });
  performanceTypes = PerformanceType;
  metricUnitMap = METRIC_UNIT_MAP;
  icons = Icons;
  iconSize = IconSize;
  emptyState = EMPTY_STATE_IMAGE;

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

  role: string = '';

  private prometheusService = inject(PrometheusService);
  private performanceCardService = inject(PerformanceCardService);

  time = { ...this.prometheusService.lastHourDateObject };
  private chartSub?: Subscription;

  ngOnInit() {
    this.loadCharts(this.time);
  }

  loadCharts(time: { start: number; end: number; step: number }) {
    this.time = { ...time };

    this.chartSub?.unsubscribe();

    if (this.storageEmptyState || this.prometheusEmptyState) {
      this.chartDataSignal.set(null);
      return;
    }

    this.chartSub = this.performanceCardService
      .getChartData(time)
      .pipe(takeUntil(this.destroy$))
      .subscribe((data) => {
        this.chartDataSignal.set(data);
      });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
    this.chartSub?.unsubscribe();
  }
}
