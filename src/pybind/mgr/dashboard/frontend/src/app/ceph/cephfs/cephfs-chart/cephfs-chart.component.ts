import { Component, ElementRef, Input, OnChanges, OnInit, ViewChild } from '@angular/core';

import { ChartDataSets, ChartOptions, ChartPoint, ChartType } from 'chart.js';
import * as _ from 'lodash';
import * as moment from 'moment';

import { ChartTooltip } from '../../../shared/models/chart-tooltip';

@Component({
  selector: 'cd-cephfs-chart',
  templateUrl: './cephfs-chart.component.html',
  styleUrls: ['./cephfs-chart.component.scss']
})
export class CephfsChartComponent implements OnChanges, OnInit {
  @ViewChild('chartCanvas', { static: true })
  chartCanvas: ElementRef;
  @ViewChild('chartTooltip', { static: true })
  chartTooltip: ElementRef;

  @Input()
  mdsCounter: any;

  lhsCounter = 'mds_mem.ino';
  rhsCounter = 'mds_server.handle_client_request';

  chart: {
    datasets: ChartDataSets[];
    options: ChartOptions;
    chartType: ChartType;
  } = {
    datasets: [
      {
        label: this.lhsCounter,
        yAxisID: 'LHS',
        data: [],
        lineTension: 0.1
      },
      {
        label: this.rhsCounter,
        yAxisID: 'RHS',
        data: [],
        lineTension: 0.1
      }
    ],
    options: {
      title: {
        text: '',
        display: true
      },
      responsive: true,
      maintainAspectRatio: false,
      legend: {
        position: 'top'
      },
      scales: {
        xAxes: [
          {
            position: 'top',
            type: 'time',
            time: {
              displayFormats: {
                quarter: 'MMM YYYY'
              }
            },
            ticks: {
              maxRotation: 0
            }
          }
        ],
        yAxes: [
          {
            id: 'LHS',
            type: 'linear',
            position: 'left'
          },
          {
            id: 'RHS',
            type: 'linear',
            position: 'right'
          }
        ]
      },
      tooltips: {
        enabled: false,
        mode: 'index',
        intersect: false,
        position: 'nearest',
        callbacks: {
          // Pick the Unix timestamp of the first tooltip item.
          title: (tooltipItems, data): string => {
            let ts = 0;
            if (tooltipItems.length > 0) {
              const item = tooltipItems[0];
              const point = data.datasets[item.datasetIndex].data[item.index] as ChartPoint;
              ts = point.x as number;
            }
            return ts.toString();
          }
        }
      }
    },
    chartType: 'line'
  };

  ngOnInit() {
    if (_.isUndefined(this.mdsCounter)) {
      return;
    }
    this.setChartTooltip();
    this.updateChart();
  }

  ngOnChanges() {
    if (_.isUndefined(this.mdsCounter)) {
      return;
    }
    this.updateChart();
  }

  private setChartTooltip() {
    const chartTooltip = new ChartTooltip(
      this.chartCanvas,
      this.chartTooltip,
      (tooltip: any) => tooltip.caretX + 'px',
      (tooltip: any) => tooltip.caretY - tooltip.height - 23 + 'px'
    );
    chartTooltip.getTitle = (ts) => moment(ts, 'x').format('LTS');
    chartTooltip.checkOffset = true;
    const chartOptions: ChartOptions = {
      title: {
        text: this.mdsCounter.name
      },
      tooltips: {
        custom: (tooltip) => chartTooltip.customTooltips(tooltip)
      }
    };
    _.merge(this.chart, { options: chartOptions });
  }

  private updateChart() {
    const chartDataSets: ChartDataSets[] = [
      {
        data: this.convertTimeSeries(this.mdsCounter[this.lhsCounter])
      },
      {
        data: this.deltaTimeSeries(this.mdsCounter[this.rhsCounter])
      }
    ];
    _.merge(this.chart, {
      datasets: chartDataSets
    });
    this.chart.datasets = [...this.chart.datasets]; // Force angular to update
  }

  /**
   * Convert ceph-mgr's time series format (list of 2-tuples
   * with seconds-since-epoch timestamps) into what chart.js
   * can handle (list of objects with millisecs-since-epoch
   * timestamps)
   */
  private convertTimeSeries(sourceSeries: any) {
    const data: any[] = [];
    _.each(sourceSeries, (dp) => {
      data.push({
        x: dp[0] * 1000,
        y: dp[1]
      });
    });

    /**
     * MDS performance counters chart is expecting the same number of items
     * from each data series. Since in deltaTimeSeries we are ignoring the first
     * element, we will do the same here.
     */
    data.shift();

    return data;
  }

  private deltaTimeSeries(sourceSeries: any) {
    let i;
    let prev = sourceSeries[0];
    const result = [];
    for (i = 1; i < sourceSeries.length; i++) {
      const cur = sourceSeries[i];

      result.push({
        x: cur[0] * 1000,
        y: cur[1] - prev[1]
      });

      prev = cur;
    }
    return result;
  }
}
