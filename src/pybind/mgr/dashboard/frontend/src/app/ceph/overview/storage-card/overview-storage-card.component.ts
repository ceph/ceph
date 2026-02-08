import { Component, ViewEncapsulation } from '@angular/core';
import {
  CheckboxModule,
  DropdownModule,
  GridModule,
  TilesModule,
  TooltipModule
} from 'carbon-components-angular';
import { ProductiveCardComponent } from '~/app/shared/components/productive-card/productive-card.component';
import { MeterChartComponent, MeterChartOptions } from '@carbon/charts-angular';

const StorageType = {
  ALL: $localize`All`,
  BLOCK: $localize`Block`,
  FILE: $localize`File`,
  OBJECT: $localize`Object`
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
    TooltipModule
  ],
  standalone: true,
  templateUrl: './overview-storage-card.component.html',
  styleUrl: './overview-storage-card.component.scss',
  encapsulation: ViewEncapsulation.None
})
export class OverviewStorageCardComponent {
  options: MeterChartOptions = {
    height: '45px',
    meter: {
      proportional: {
        total: 2000,
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
      value: 202
    },
    {
      group: StorageType.FILE,
      value: 654
    },
    {
      group: StorageType.OBJECT,
      value: 120
    }
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
