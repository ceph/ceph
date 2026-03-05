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
import { OverviewStorageService } from '~/app/shared/api/storage-overview.service';
import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';
import { AreaChartComponent } from '~/app/shared/components/area-chart/area-chart.component';
import { PieChartComponent } from '~/app/shared/components/pie-chart/pie-chart.component';

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

const PROMQL_RAW_USED_BY_STORAGE_TYPE =
  'sum by (application) (ceph_pool_bytes_used * on(pool_id) group_left(instance, name, application) ceph_pool_metadata{application=~"(.*Block.*)|(.*Filesystem.*)|(.*Object.*)|(..*)"})';

const PROMQL_TOP_POOLS_BLOCK = `
  topk(5,
    (ceph_pool_bytes_used * on(pool_id) group_left(name,application) ceph_pool_metadata{application="Block"})
    /
    (ceph_pool_max_avail * on(pool_id) group_left(name,application) ceph_pool_metadata{application="Block"})
  )
`;

const PROMQL_TOP_POOLS_FILESYSTEM = `
  topk(5,
    (ceph_pool_bytes_used * on(pool_id) group_left(name,application) ceph_pool_metadata{application="Filesystem"})
    /
    (ceph_pool_max_avail * on(pool_id) group_left(name,application) ceph_pool_metadata{application="Filesystem"})
  )
`;

const PROMQL_TOP_POOLS_OBJECT = `
  topk(5,
    (ceph_pool_bytes_used * on(pool_id) group_left(name,application) ceph_pool_metadata{application="Object"})
    /
    (ceph_pool_max_avail * on(pool_id) group_left(name,application) ceph_pool_metadata{application="Object"})
  )
`;

const PROMQL_COUNT_BLOCK_POOLS = 'count(ceph_pool_metadata{application="Block"})';

const PROMQL_COUNT_RBD_IMAGES = 'count(ceph_rbd_image_metadata)';

const PROMQL_COUNT_FILESYSTEMS = 'count(ceph_fs_metadata)';

const PROMQL_COUNT_FILESYSTEM_POOLS = 'count(ceph_pool_metadata{application="Filesystem"})';

const chartGroupLabels = [StorageType.BLOCK, StorageType.FILE, StorageType.OBJECT];

const TopPoolsQueryMap = {
  Block: PROMQL_TOP_POOLS_BLOCK,
  'File system': PROMQL_TOP_POOLS_FILESYSTEM,
  Object: PROMQL_TOP_POOLS_OBJECT
};

@Component({
  selector: 'cd-overview-storage-card',
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
    AreaChartComponent,
    PieChartComponent
  ],
  standalone: true,
  templateUrl: './overview-storage-card.component.html',
  styleUrl: './overview-storage-card.component.scss',
  encapsulation: ViewEncapsulation.None,
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class OverviewStorageCardComponent implements OnInit, OnDestroy {
  private readonly prometheusService = inject(PrometheusService);
  private readonly formatterService = inject(FormatterService);
  private readonly overviewStorageService = inject(OverviewStorageService);
  private readonly rgw = inject(RgwBucketService);
  private readonly cdr = inject(ChangeDetectorRef);
  private destroy$ = new Subject<void>();
  trendData: { timestamp: Date; values: { Used: number } }[];

  @Input()
  set total(value: number) {
    const [totalValue, totalUnit] = this.formatterService.formatToBinary(value, true);
    if (Number.isNaN(totalValue)) return;
    this.totalRaw = totalValue;
    this.totalRawUnit = totalUnit;
    this._setTotalAndUsed();
  }
  @Input()
  set used(value: number) {
    const [usedValue, usedUnit] = this.formatterService.formatToBinary(value, true);
    if (Number.isNaN(usedValue)) return;
    this.usedRaw = usedValue;
    this.usedRawUnit = usedUnit;
    this._setTotalAndUsed();
  }
  totalRaw: number;
  usedRaw: number;
  totalRawUnit: string;
  usedRawUnit: string;
  options: MeterChartOptions = {
    height: CHART_HEIGHT,
    meter: {
      proportional: {
        total: null,
        unit: '',
        breakdownFormatter: (_e) => null,
        totalFormatter: (_e) => null
      }
    },
    toolbar: {
      enabled: false
    },
    color: {
      pairing: {
        option: 2
      }
    }
  };
  allData: ChartData[] = null;
  displayData: ChartData[] = null;
  displayUsedRaw: number;
  selectedStorageType: string = StorageType.ALL;
  dropdownItems = [
    { content: StorageType.ALL },
    { content: StorageType.BLOCK },
    { content: StorageType.FILE },
    { content: StorageType.OBJECT }
  ];
  topPoolsData = null;

  storageMetrics = {
    Block: {
      metrics: [
        { label: 'block pools', value: 0 },
        { label: 'volumes', value: 0 }
      ]
    },
    'File system': {
      metrics: [
        { label: 'filesystems', value: 0 },
        { label: 'filesystem pools', value: 0 }
      ]
    },
    Object: {
      metrics: [
        { label: 'buckets', value: 0 },
        { label: 'object pools', value: 0 }
      ]
    }
  };

  averageConsumption = '';
  timeUntilFull = '';

  private _setTotalAndUsed() {
    // Chart reacts to 'options' and 'data' object changes only, hence mandatory to replace whole object.
    this.options = {
      ...this.options,
      meter: {
        ...this.options.meter,
        proportional: {
          ...this.options.meter.proportional,
          total: this.totalRaw,
          unit: this.totalRawUnit
        }
      },
      tooltip: {
        valueFormatter: (value) => `${value.toLocaleString()} ${this.usedRawUnit}`
      }
    };
    this._updateCard();
  }

  private _getAllData(data: PromqlGuageMetric) {
    const result = data?.result ?? [];
    const chartData = result
      .map((r: PromethuesGaugeMetricResult) => {
        const group = r?.metric?.application;
        const value = this.formatterService.convertToUnit(r?.value?.[1], 'B', this.usedRawUnit, 1);
        return { group: group === 'Filesystem' ? StorageType.FILE : group, value };
      })
      // Removing 0 values and legends other than Block, File system, and Object.
      .filter((r) => chartGroupLabels.includes(r?.group) && r?.value > 0);
    return chartData;
  }

  private _setChartData() {
    if (this.selectedStorageType === StorageType.ALL) {
      this.displayData = this.allData;
      this.displayUsedRaw = this.usedRaw;
    } else {
      this.displayData = this.allData?.filter(
        (d: ChartData) => d.group === this.selectedStorageType
      );
      this.displayUsedRaw = this.displayData?.[0]?.value;
    }
  }

  private _setDropdownItemsAndStorageType() {
    const newData = this.allData?.map((data) => ({ content: data.group }));
    if (newData.length) {
      this.dropdownItems = [{ content: StorageType.ALL }, ...newData];
    } else {
      this.dropdownItems = [{ content: StorageType.ALL }];
    }
  }

  private _updateCard() {
    this.cdr.markForCheck();
  }

  private loadTrend() {
    const now = Math.floor(Date.now() / 1000);
    const range = { start: now - 7 * 86400, end: now, step: 3600 };

    this.overviewStorageService
      .getTrendData(range.start, range.end, range.step)
      .pipe(takeUntil(this.destroy$))
      .subscribe((result) => {
        const values = result?.TOTAL_RAW_USED ?? [];
        this.trendData = values.map(([ts, val]) => ({
          timestamp: new Date(ts * 1000),
          values: { Used: Number(val) }
        }));
        this.cdr.markForCheck();
      });
  }

  private loadAverageConsumption() {
    this.overviewStorageService
      .getAverageConsumption()
      .pipe(takeUntil(this.destroy$))
      .subscribe((v) => {
        this.averageConsumption = v;
        this.cdr.markForCheck();
      });
  }

  private loadTimeUntilFull() {
    this.overviewStorageService
      .getTimeUntilFull()
      .pipe(takeUntil(this.destroy$))
      .subscribe((v) => {
        this.timeUntilFull = v;
        this.cdr.markForCheck();
      });
  }

  private loadTopPools() {
    const query = TopPoolsQueryMap[this.selectedStorageType];
    if (!query) return;

    this.overviewStorageService
      .getTopPools(query)
      .pipe(takeUntil(this.destroy$))
      .subscribe((data) => {
        this.topPoolsData = data;
        this.cdr.markForCheck();
      });
  }

  private loadCounts() {
    const type = this.selectedStorageType;

    if (type === StorageType.BLOCK) {
      this.overviewStorageService
        .getCount(PROMQL_COUNT_BLOCK_POOLS)
        .pipe(takeUntil(this.destroy$))
        .subscribe((value) => {
          this.storageMetrics.Block.metrics[0].value = value;
          this.cdr.markForCheck();
        });

      this.overviewStorageService
        .getCount(PROMQL_COUNT_RBD_IMAGES)
        .pipe(takeUntil(this.destroy$))
        .subscribe((value) => {
          this.storageMetrics.Block.metrics[1].value = value;
          this.cdr.markForCheck();
        });
    }

    if (type === StorageType.FILE) {
      this.overviewStorageService
        .getCount(PROMQL_COUNT_FILESYSTEMS)
        .pipe(takeUntil(this.destroy$))
        .subscribe((value) => {
          this.storageMetrics['File system'].metrics[0].value = value;
          this.cdr.markForCheck();
        });

      this.overviewStorageService
        .getCount(PROMQL_COUNT_FILESYSTEM_POOLS)
        .pipe(takeUntil(this.destroy$))
        .subscribe((value) => {
          this.storageMetrics['File system'].metrics[1].value = value;
          this.cdr.markForCheck();
        });
    }

    if (type === StorageType.OBJECT) {
      this.overviewStorageService
        .getObjectCounts(this.rgw)
        .pipe(takeUntil(this.destroy$))
        .subscribe((value) => {
          this.storageMetrics.Object.metrics[0].value = value.buckets;
          this.storageMetrics.Object.metrics[1].value = value.pools;
          this.cdr.markForCheck();
        });
    }
  }

  onStorageTypeSelect(event: any) {
    this.selectedStorageType = event?.item?.content;
    this._setChartData();

    if (this.selectedStorageType === StorageType.ALL) {
      this.loadTrend();
      this.topPoolsData = null;
    } else {
      this.loadTopPools();
      this.loadCounts();
    }
  }

  ngOnInit() {
    interval(REFRESH_INTERVAL_MS)
      .pipe(
        startWith(0),
        switchMap(() =>
          this.prometheusService.getPrometheusQueryData({
            params: PROMQL_RAW_USED_BY_STORAGE_TYPE
          })
        ),
        takeUntil(this.destroy$)
      )
      .subscribe((data: PromqlGuageMetric) => {
        this.allData = this._getAllData(data);
        this._setDropdownItemsAndStorageType();
        this._setChartData();
        this._updateCard();
        if (this.selectedStorageType === StorageType.ALL) {
          this.loadAverageConsumption();
          this.loadTimeUntilFull();
        } else {
          this.loadTopPools();
        }

        this.cdr.markForCheck();
      });
    this.loadTrend();
    this.loadAverageConsumption();
    this.loadTimeUntilFull();
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
