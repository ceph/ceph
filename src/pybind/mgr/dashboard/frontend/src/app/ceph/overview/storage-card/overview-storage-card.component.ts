import {
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  Component,
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
import { BehaviorSubject, Subject } from 'rxjs';
import { switchMap, takeUntil } from 'rxjs/operators';

const CHART_HEIGHT = '45px';

const StorageType = {
  ALL: $localize`All`,
  BLOCK: $localize`Block`,
  FILE: $localize`Filesystem`,
  OBJECT: $localize`Object`
};

const CapacityType = {
  RAW: 'raw',
  USED: 'used'
};

type ChartData = {
  group: string;
  value: number;
};

const Query = {
  [CapacityType.RAW]: `sum by (application) (ceph_pool_bytes_used * on(pool_id) group_left(instance, name, application) ceph_pool_metadata{application=~"(.*Block.*)|(.*Filesystem.*)|(.*Object.*)|(..*)"})`,
  [CapacityType.USED]: `sum by (application) (ceph_pool_stored * on(pool_id) group_left(instance, name, application) ceph_pool_metadata{application=~"(.*Block.*)|(.*Filesystem.*)|(.*Object.*)|(..*)"})`
};

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
  @Input()
  set total(value: number) {
    const [totalValue, totalUnit] = this.formatterService.formatToBinary(value, true);
    if (Number.isNaN(totalValue)) return;
    this.totalRaw = totalValue;
    this.totalRawUnit = totalUnit;
    this.setTotalAndUsed();
  }
  @Input()
  set used(value: number) {
    const [usedValue, usedUnit] = this.formatterService.formatToBinary(value, true);
    if (Number.isNaN(usedValue)) return;
    this.usedRaw = usedValue;
    this.usedRawUnit = usedUnit;
    this.setTotalAndUsed();
  }
  totalRaw: number;
  usedRaw: number;
  totalRawUnit: string;
  usedRawUnit: string;
  isRawCapacity: boolean = true;
  selectedStorageType: string = StorageType.ALL;
  selectedCapacityType: string = CapacityType.RAW;
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
  dropdownItems = [
    { content: StorageType.ALL },
    { content: StorageType.BLOCK },
    { content: StorageType.FILE },
    { content: StorageType.OBJECT }
  ];

  constructor(
    private prometheusService: PrometheusService,
    private formatterService: FormatterService,
    private cdr: ChangeDetectorRef
  ) {}

  private destroy$ = new Subject<void>();
  private capacityType$ = new BehaviorSubject<string>(CapacityType.RAW);

  private setTotalAndUsed() {
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
    this.updateCard();
  }

  private getAllData(data: PromqlGuageMetric) {
    const result = data?.result ?? [];
    const chartData = result
      .map((r: PromethuesGaugeMetricResult) => {
        const group = r?.metric?.application;
        const value = this.formatterService.convertToUnit(r?.value?.[1], 'B', this.usedRawUnit, 10);
        return { group, value };
      })
      // Removing 0 values and legends other than Block, Filesystem, and Object.
      .filter((r) => chartGroupLabels.includes(r.group) && r.value > 0);
    return chartData;
  }

  private setChartData() {
    if (this.selectedStorageType === StorageType.ALL) {
      this.displayData = this.allData;
    } else {
      this.displayData = this.allData.filter(
        (d: ChartData) => d.group === this.selectedStorageType
      );
    }
  }

  private setDropdownItemsAndStorageType() {
    const dynamicItems = this.allData.map((data) => ({ content: data.group }));
    const hasExistingItem = dynamicItems.some((item) => item.content === this.selectedStorageType);

    if (dynamicItems.length === 1) {
      this.dropdownItems = dynamicItems;
      this.selectedStorageType = dynamicItems[0]?.content;
    } else {
      this.dropdownItems = [{ content: StorageType.ALL }, ...dynamicItems];
    }
    // Change the current dropdown selection to 'ALL' if prev selection is absent in current data, and current data has more than one item.
    if (!hasExistingItem && dynamicItems.length > 1) {
      this.selectedStorageType = StorageType.ALL;
    }
  }

  private updateCard() {
    this.cdr.markForCheck();
  }

  public toggleRawCapacity(isChecked: boolean) {
    this.isRawCapacity = isChecked;
    this.selectedCapacityType = isChecked ? CapacityType.RAW : CapacityType.USED;
    // Reloads Prometheus Query
    this.capacityType$.next(this.selectedCapacityType);
  }

  public onStorageTypeSelect(selected: { item: { content: string; selected: true } }) {
    this.selectedStorageType = selected?.item?.content;
    this.setChartData();
  }

  ngOnInit() {
    this.capacityType$
      .pipe(
        switchMap((capacityType) =>
          this.prometheusService.getPrometheusQueryData({
            params: Query[capacityType]
          })
        ),
        takeUntil(this.destroy$)
      )
      .subscribe((data: PromqlGuageMetric) => {
        this.allData = this.getAllData(data);
        this.setDropdownItemsAndStorageType();
        this.setChartData();
        this.updateCard();
      });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
