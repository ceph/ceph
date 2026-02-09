import { Component, Input, OnChanges, ViewEncapsulation } from '@angular/core';
import {
  CheckboxModule,
  DropdownModule,
  GridModule,
  TilesModule,
  TooltipModule
} from 'carbon-components-angular';
import { ProductiveCardComponent } from '~/app/shared/components/productive-card/productive-card.component';
import { MeterChartComponent, MeterChartOptions } from '@carbon/charts-angular';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';

const StorageType = {
  ALL: $localize`All`,
  BLOCK: $localize`Block`,
  FILE: $localize`File`,
  OBJECT: $localize`Object`
};

// const Query = {
//   RAW: {
//     [StorageType.ALL]: `sum by (application) (ceph_pool_bytes_used * on(pool_id) group_left(instance, name, application) ceph_pool_metadata{application=~"(.*Block.*)|(.*Filesystem.*)|(.*Object.*)|(..*)"})`,
//     [StorageType.BLOCK]: "",
//     [StorageType.FILE]: "",
//     [StorageType.OBJECT]: ""
//   },
//   USED: {
//     [StorageType.ALL]: `sum by (application) (ceph_pool_stored * on(pool_id) group_left(instance, name, application) ceph_pool_metadata{application=~"(.*Block.*)|(.*Filesystem.*)|(.*Object.*)|(..*)"})`,
//     [StorageType.BLOCK]: "",
//     [StorageType.FILE]: "",
//     [StorageType.OBJECT]: ""
//   }
// }

/**
 * 1. Fetch snapshot query -> [pass total and used raw]
 * 2. Show the usage title -> always fixed
 * 3. The chart total -> raw total always fixed
 * 4. Set data for block, file , object, all -> raw, sep queries
 * 5. Set data for block, file object + replicated -> usable
 * 6. Dont show what is 0
 */

@Component({
  selector: 'cd-overview-storage-card',
  imports: [
    GridModule,
    TilesModule,
    ProductiveCardComponent,
    MeterChartComponent,
    CheckboxModule,
    DropdownModule,
    TooltipModule
  ],
  standalone: true,
  templateUrl: './overview-storage-card.component.html',
  styleUrl: './overview-storage-card.component.scss',
  encapsulation: ViewEncapsulation.None
})
export class OverviewStorageCardComponent implements OnChanges {
  @Input() total!: number;
  @Input() used!: number;
  totalRaw: string;
  usedRaw: string;
  totalRawUnit: string;
  usedRawUnit: string;
  options: MeterChartOptions = {
    height: '45px',
    meter: {
      proportional: {
        total: null,
        unit: 'GB',
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
    },
    canvasZoom: {
      enabled: false
    }
  };
  allData = [
    {
      group: StorageType.BLOCK,
      value: 100,
    },
    {
      group: StorageType.FILE,
      value: 105
    },
    {
      group: StorageType.OBJECT,
      value: 60    }
  ];
  dropdownItems = [
    { content: StorageType.ALL },
    { content: StorageType.BLOCK },
    { content: StorageType.FILE },
    { content: StorageType.OBJECT }
  ];
  isRawCapacity: boolean = true;
  selectedStorageType: string = StorageType.ALL;
  displayData = this.allData;

  constructor(private dimlessBinaryPipe: DimlessBinaryPipe) {}

ngOnChanges(): void {
  if (this.total == null || this.used == null) return;

  const totalRaw = this.dimlessBinaryPipe.transform(this.total);
  const usedRaw = this.dimlessBinaryPipe.transform(this.used);

  const [totalValue, totalUnit] = totalRaw.split(/\s+/);
  const [usedValue, usedUnit] = usedRaw.split(/\s+/);

  const cleanedTotal = Number(totalValue.replace(/,/g, '').trim());

  if (Number.isNaN(cleanedTotal)) return;

  this.totalRaw = totalValue;
  this.totalRawUnit = totalUnit;
  this.usedRaw = usedValue;
  this.usedRawUnit = usedUnit;

  // chart reacts to 'options' and 'data' changes only, hence mandatory to replace whole object
  this.options = {
    ...this.options,
    meter: {
      ...this.options.meter,
      proportional: {
        ...this.options.meter.proportional,
        total: cleanedTotal,
        unit: totalUnit
      }
    }
  };
}

  toggleRawCapacity(isChecked: boolean) {
    this.isRawCapacity = isChecked;
  }

  onStorageTypeSelect(selected: { item: { content: string; selected: true } }) {
    this.selectedStorageType = selected?.item?.content;
    if (this.selectedStorageType === StorageType.ALL) {
      this.displayData = this.allData;
    } else this.displayData = this.allData.filter((d) => d.group === this.selectedStorageType);
  }
}
