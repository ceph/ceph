import {
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  Component,
  inject,
  Input,
  OnDestroy,
  OnInit,
  ViewEncapsulation
} from '@angular/core';
import {
  CheckboxModule,
  DropdownModule,
  GridModule,
  TilesModule,
  TooltipModule,
  SkeletonModule,
  LayoutModule
} from 'carbon-components-angular';
import { ProductiveCardComponent } from '~/app/shared/components/productive-card/productive-card.component';
import { MeterChartComponent, MeterChartOptions } from '@carbon/charts-angular';
import {
  PrometheusService,
  PromethuesGaugeMetricResult,
  PromqlGuageMetric
} from '~/app/shared/api/prometheus.service';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { interval, Subject } from 'rxjs';
import { startWith, switchMap, takeUntil } from 'rxjs/operators';
import { AreaChartComponent } from '~/app/shared/components/area-chart/area-chart.component';

const CHART_HEIGHT = '45px';
const REFRESH_INTERVAL_MS = 15_000;

const StorageType = {
  ALL: $localize`All`,
  BLOCK: $localize`Block`,
  FILE: $localize`File system`,
  OBJECT: $localize`Object`
};

type ChartData = {
  group: string;
  value: number;
};

const RawUsedByStorageType =
  'sum by (application) (ceph_pool_bytes_used * on(pool_id) group_left(instance, name, application) ceph_pool_metadata{application=~"(.*Block.*)|(.*Filesystem.*)|(.*Object.*)|(..*)"})';

const TotalRawUsedTrend = {
  TOTAL_RAW_USED: 'sum(ceph_pool_bytes_used)'
};

const chartGroupLabels = [StorageType.BLOCK, StorageType.FILE, StorageType.OBJECT];

@Component({
  selector: 'cd-overview-storage-card',
  standalone: true,
  imports: [
    GridModule,
    TilesModule,
    ProductiveCardComponent,
    MeterChartComponent,
    CheckboxModule,
    DropdownModule,
    TooltipModule,
    SkeletonModule,
    LayoutModule,
    AreaChartComponent
  ],
  templateUrl: './overview-storage-card.component.html',
  styleUrl: './overview-storage-card.component.scss',
  encapsulation: ViewEncapsulation.None,
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class OverviewStorageCardComponent implements OnInit, OnDestroy {

  private readonly prometheusService = inject(PrometheusService);
  private readonly formatterService = inject(FormatterService);
  private readonly cdr = inject(ChangeDetectorRef);
  private destroy$ = new Subject<void>();

  trendData: { timestamp: Date; values: { [x: string]: number } }[] = [];

  totalRaw!: number;
  usedRaw!: number;
  totalRawUnit!: string;
  usedRawUnit!: string;

  averageConsumption = '';
  timeUntilFull = '';

  selectedStorageType: string = StorageType.ALL;

  dropdownItems = [
    { content: StorageType.ALL },
    { content: StorageType.BLOCK },
    { content: StorageType.FILE },
    { content: StorageType.OBJECT }
  ];

  options: MeterChartOptions = {
    height: CHART_HEIGHT,
    meter: {
      proportional: {
        total: null,
        unit: ''
      }
    },
    toolbar: { enabled: false },
    color: { pairing: { option: 2 } }
  };

  allData: ChartData[] = null;
  displayData: ChartData[] = null;
  displayUsedRaw: number;

  @Input()
  set total(value: number) {
    const [val, unit] = this.formatterService.formatToBinary(value, true);
    if (!Number.isNaN(val)) {
      this.totalRaw = val;
      this.totalRawUnit = unit;
      this.updateMeter();
    }
  }

  @Input()
  set used(value: number) {
    const [val, unit] = this.formatterService.formatToBinary(value, true);
    if (!Number.isNaN(val)) {
      this.usedRaw = val;
      this.usedRawUnit = unit;
      this.updateMeter();
    }
  }

  ngOnInit() {

    interval(REFRESH_INTERVAL_MS)
      .pipe(
        startWith(0),
        switchMap(() =>
          this.prometheusService.getPrometheusQueryData({
            params: RawUsedByStorageType
          })
        ),
        takeUntil(this.destroy$)
      )
      .subscribe((data: PromqlGuageMetric) => {
        this.allData = this.mapStorageData(data);
        this.setChartData();
        this.cdr.markForCheck();
      });

    this.loadTrendData();
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  private mapStorageData(data: PromqlGuageMetric): ChartData[] {
    const result = data?.result ?? [];
    return result
      .map((r: PromethuesGaugeMetricResult) => {
        const group = r?.metric?.application;
        const value = this.formatterService.convertToUnit(
          r?.value?.[1],
          'B',
          this.usedRawUnit,
          1
        );
        return { group: group === 'Filesystem' ? StorageType.FILE : group, value };
      })
      .filter(r => chartGroupLabels.includes(r?.group) && r?.value > 0);
  }

  private setChartData() {
    if (this.selectedStorageType === StorageType.ALL) {
      this.displayData = this.allData;
      this.displayUsedRaw = this.usedRaw;
    } else {
      this.displayData = this.allData?.filter(d => d.group === this.selectedStorageType);
      this.displayUsedRaw = this.displayData?.[0]?.value;
    }
  }

  public onStorageTypeSelect(selected: any) {
    this.selectedStorageType = selected?.item?.content;
    this.setChartData();
    if (this.selectedStorageType === StorageType.ALL) {
      this.loadTrendData();
    }
  }

  private getSevenDaysTime() {
    const now = Math.floor(Date.now() / 1000);
    return {
      start: now - 7 * 24 * 60 * 60,
      end: now,
      step: 3600
    };
  }

  private loadTrendData() {

    const time = this.getSevenDaysTime();

    this.prometheusService
      .getRangeQueriesData(time, TotalRawUsedTrend, true)
      .pipe(takeUntil(this.destroy$))
      .subscribe(result => {

        const values = result?.TOTAL_RAW_USED || [];
        this.trendData = this.toSeries(values, 'Used Capacity');

        if (values.length > 1) {
          const first = Number(values[0][1]);
          const last = Number(values[values.length - 1][1]);

          const growth = last - first;
          const avgPerDay = growth / 7;

          const [val, unit] =
            this.formatterService.formatToBinary(avgPerDay, true);

          this.averageConsumption = `${val} ${unit}/day`;

          const totalBytes = this.formatterService.toBytes(
            `${this.totalRaw} ${this.totalRawUnit}`
          ) || 0;

          const usedBytes = this.formatterService.toBytes(
            `${this.usedRaw} ${this.usedRawUnit}`
          ) || 0;

          const remaining = totalBytes - usedBytes;

          if (avgPerDay > 0) {
            const daysLeft = remaining / avgPerDay;
            this.timeUntilFull = this.formatDuration(daysLeft);
          } else {
            this.timeUntilFull = '∞';
          }
        }

        this.cdr.markForCheck();
      });
  }

  private toSeries(metric: [number, string][], label: string) {
    return metric.map(([ts, val]) => ({
      timestamp: new Date(ts * 1000),
      values: { [label]: Number(val) }
    }));
  }

  private formatDuration(days: number): string {
    if (!isFinite(days) || days <= 0) {
      return '∞';
    }

    if (days < 1) {
      const hours = days * 24;
      return `${hours.toFixed(1)} hours`;
    }

    if (days < 30) {
      return `${days.toFixed(1)} days`;
    }

    const months = days / 30;
    return `${months.toFixed(1)} months`;
  }

  private updateMeter() {
    this.options = {
      ...this.options,
      meter: {
        ...this.options.meter,
        proportional: {
          ...this.options.meter.proportional,
          total: this.totalRaw,
          unit: this.totalRawUnit
        }
      }
    };
    this.cdr.markForCheck();
  }
}
