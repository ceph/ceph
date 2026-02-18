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

const chartGroupLabels = [StorageType.BLOCK, StorageType.FILE, StorageType.OBJECT];

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
    LayoutModule
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
  private readonly cdr = inject(ChangeDetectorRef);
  private destroy$ = new Subject<void>();

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

  public onStorageTypeSelect(selected: { item: { content: string; selected: true } }) {
    this.selectedStorageType = selected?.item?.content;
    this._setChartData();
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
        this.allData = this._getAllData(data);
        this._setDropdownItemsAndStorageType();
        this._setChartData();
        this._updateCard();
      });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
