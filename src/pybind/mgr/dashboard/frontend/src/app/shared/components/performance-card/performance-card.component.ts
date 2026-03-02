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

  time = { ...this.prometheusService.lastHourDateObject };
  private chartSub?: Subscription;

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
