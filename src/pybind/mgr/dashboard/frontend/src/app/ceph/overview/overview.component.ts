import { Component, inject, OnInit } from '@angular/core';
import { ComponentsModule } from '~/app/shared/components/components.module';
import { GridModule, TilesModule } from 'carbon-components-angular';
import { PrometheusService } from '~/app/shared/api/prometheus.service';
import { UtilizationCardQueries} from '~/app/shared/enum/dashboard-promqls.enum';
import { METRIC_UNIT_MAP, PerformanceType } from '~/app/shared/models/performance-data';
import { take } from 'rxjs/operators';

@Component({
  selector: 'cd-overview',
  imports: [GridModule, TilesModule, ComponentsModule],
  standalone: true,
  templateUrl: './overview.component.html',
  styleUrl: './overview.component.scss'
})
export class OverviewComponent implements OnInit {
  
  queriesResults: { [key: string]: [] } = {
    USEDCAPACITY: [],
    IPS: [],
    OPS: [],
    READLATENCY: [],
    WRITELATENCY: [],
    READCLIENTTHROUGHPUT: [],
    WRITECLIENTTHROUGHPUT: [],
    RECOVERYBYTES: [],
    READIOPS: [],
    WRITEIOPS: []
  };
  chartData: any;
  performanceTypes = PerformanceType;
  metricUnitMap = METRIC_UNIT_MAP;

  private prometheusService = inject(PrometheusService);

  ngOnInit(): void {
    this.getPrometheusData(this.prometheusService.lastHourDateObject);
  }

  // public getPrometheusData(selectedTime: any) {
  //     this.queriesResults = this.prometheusService.getRangeQueriesData(
  //       selectedTime,
  //       UtilizationCardQueries,
  //       this.queriesResults
  //     );
  //     this.chartData = this.prometheusService.convertPerformanceData(this.queriesResults);

  //     console.log("OVERVIEW CARBON-READY DATA:", this.chartData);
  //   }

  public getPrometheusData(selectedTime: any) {
  this.prometheusService.getRangeQueriesData(
    selectedTime,
    UtilizationCardQueries,
    this.queriesResults,
    true
  );

  this.prometheusService.updatedChrtData
    .pipe(take(1))
    .subscribe(updated => {
      this.queriesResults = updated;

      this.chartData = this.prometheusService.convertPerformanceData(updated);

      console.log("Chart Data:", this.chartData);
    });
}
}
  