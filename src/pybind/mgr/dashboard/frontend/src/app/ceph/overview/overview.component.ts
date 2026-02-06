import {
  Component,
  inject,
  OnInit
} from '@angular/core';

import {
  GridModule,
  TilesModule,
  LayoutModule,
  DropdownModule,
  ListItem
} from 'carbon-components-angular';
import { take } from 'rxjs/operators';

import { PrometheusService } from '~/app/shared/api/prometheus.service';
import { METRIC_UNIT_MAP, PerformanceType, STORAGE_QUERY_MAP, StorageType } from '~/app/shared/models/performance-data';
import { ComponentsModule } from "~/app/shared/components/components.module";
import { CommonModule } from '@angular/common';
import { TimePickerComponent } from '~/app/shared/components/time-picker/time-picker.component';

@Component({
  selector: 'cd-overview',
  standalone: true,
  imports: [GridModule, TilesModule, LayoutModule, DropdownModule, ComponentsModule, CommonModule, TimePickerComponent],
  templateUrl: './overview.component.html',
  styleUrl: './overview.component.scss'
})
export class OverviewComponent implements OnInit {
  
  chartData: any;
  performanceTypes = PerformanceType;
  metricUnitMap = METRIC_UNIT_MAP;

  storageTypes: ListItem[] = [
    {
      content: "Filesystem", value: StorageType.Filesystem,
      selected: false
    },
    {
      content: "Block", value: StorageType.Block,
      selected: false
    },
    {
      content: "Object", value: StorageType.Object,
      selected: false
    }
  ];

  selectedStorageType = StorageType.Filesystem;

  private prometheusService = inject(PrometheusService);
  time: { start: number; end: number; step: number; };

  ngOnInit() {
    this.loadCharts(this.prometheusService.lastHourDateObject);
  }

  queriesResults = {};

loadCharts(time: { start: number; end: number; step: number; }) {
  this.time = time;
  const queries = STORAGE_QUERY_MAP[this.selectedStorageType];

  this.prometheusService.getRangeQueriesData(
    time,
    queries,
    this.queriesResults,
    true
  );

  this.prometheusService.updatedChrtData
    .pipe(take(1))
    .subscribe(updated => {
      this.chartData = this.prometheusService.convertPerformanceData(updated);
    });
}

  onStorageTypeSelection(event: any) {    
    this.selectedStorageType = event.item.value;    
    this.loadCharts(this.time);
  }
}
