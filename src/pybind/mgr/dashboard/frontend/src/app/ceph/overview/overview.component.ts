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
import { ComponentsModule } from "~/app/shared/components/components.module";
import { CommonModule } from '@angular/common';
import { TimePickerComponent } from '~/app/shared/components/time-picker/time-picker.component';
import { AVERAGE_CONSUMPTION_QUERY_MAP, CONSUMPTION_QUERY_MAPS, TIME_UNTIL_FULL_QUERY_MAP } from '~/app/shared/enum/dashboard-promqls.enum';
import { METRIC_UNIT_MAP, PerformanceType, STORAGE_QUERY_MAP, StorageType } from '~/app/shared/models/performance-data';

@Component({
  selector: 'cd-overview',
  standalone: true,
  imports: [GridModule, TilesModule, LayoutModule, DropdownModule, ComponentsModule, CommonModule, TimePickerComponent],
  templateUrl: './overview.component.html',
  styleUrl: './overview.component.scss'
})
export class OverviewComponent implements OnInit {
  
  performancechartData: any;
  consumptionChartData: any;
  averageConsumptionChartData: any;
  performanceTypes = PerformanceType;
  metricUnitMap = METRIC_UNIT_MAP;
  averageConsumption = '–';
timeUntilFull = '–';

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
    this.loadPerformanceCharts(this.prometheusService.lastHourDateObject);
    this.loadConsumptionChart(this.prometheusService.lastHourDateObject);
    this.loadAverageConsumption();
    this.loadTimeUntilFull();
  }

  PerfromanceQueriesResults = {};
  consumptionQueryResult = {};
  averageConsumptionQueryResult = {};

loadPerformanceCharts(time: { start: number; end: number; step: number; }) {
  this.time = time;
  const queries = STORAGE_QUERY_MAP[this.selectedStorageType];

  this.prometheusService.getRangeQueriesData(
    time,
    queries,
    this.PerfromanceQueriesResults,
    true
  );

  this.prometheusService.updatedChrtData
    .pipe(take(1))
    .subscribe(updated => {
      this.performancechartData = this.prometheusService.convertPerformanceData(updated);
    });
}

  onStorageTypeSelection(event: any) {
  this.selectedStorageType = event.item.value;

  this.loadPerformanceCharts(this.time);
  this.loadConsumptionChart(this.time);
  this.loadAverageConsumption();
  this.loadTimeUntilFull();
}


  loadConsumptionChart(time) {
  const promql = CONSUMPTION_QUERY_MAPS[this.selectedStorageType].CONSUMPTION;

  const query = { CONSUMPTION: promql };

  this.prometheusService.getRangeQueriesData(
    time,
    query,
    this.consumptionQueryResult,
    true
  );

  this.prometheusService.updatedconsumptionChrtData
    .pipe(take(1))
    .subscribe(updated => {
      const values = updated.CONSUMPTION || [];
      this.consumptionChartData =
        this.prometheusService.toSeries(values, "Consumption");
    });
}


  loadAverageConsumption() {
  const promql = AVERAGE_CONSUMPTION_QUERY_MAP[this.selectedStorageType].AVERAGE;

  this.prometheusService.getGaugeQueryData(promql)
    .pipe(take(1))
    .subscribe(result => {
      const val = Number(result?.result?.[0]?.value?.[1]);
      this.averageConsumption = this.formatBytes(val);
    });
}


loadTimeUntilFull() {
  const promql = TIME_UNTIL_FULL_QUERY_MAP[this.selectedStorageType].TUF;

  this.prometheusService.getGaugeQueryData(promql)
    .pipe(take(1))
    .subscribe(result => {
      const seconds = Number(result?.result?.[0]?.value?.[1]);

      if (!seconds || seconds <= 0 || seconds === Infinity)
        this.timeUntilFull = "∞";
      else
        this.timeUntilFull = (seconds / 86400).toFixed(1) + " days";
    });
}


formatBytes(bytes: number): string {
  if (bytes < 1024) return bytes + ' B';
  const units = ['KB','MB','GB','TB','PB'];
  let i = -1;
  do { bytes /= 1024; i++; } while (bytes >= 1024 && i < units.length - 1);
  return bytes.toFixed(2) + ' ' + units[i];
}


}