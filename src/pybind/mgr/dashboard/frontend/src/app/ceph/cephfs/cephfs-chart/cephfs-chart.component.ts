import { Component, ElementRef, Input, OnChanges, OnInit, ViewChild } from '@angular/core';

import _ from 'lodash';
import moment from 'moment';
import 'chartjs-adapter-moment';

import { ChartTooltip } from '~/app/shared/models/chart-tooltip';

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
    datasets: any[];
    options: any;
    chartType: any;
  } = {
    datasets: [
      {
        label: this.lhsCounter,
        yAxisID: 'LHS',
        data: [],
        tension: 0.1,
        fill: {
          target: 'origin'
        }
      },
      {
        label: this.rhsCounter,
        yAxisID: 'RHS',
        data: [],
        tension: 0.1,
        fill: {
          target: 'origin'
        }
      }
    ],
    options: {
      plugins: {
        title: {
          text: '',
          display: true
        },
        tooltip: {
          enabled: false,
          mode: 'index',
          intersect: false,
          position: 'nearest',
          callbacks: {
            // Pick the Unix timestamp of the first tooltip item.
            title: (context: any): string => {
              let ts = '';
              if (context.length > 0) {
                ts = context[0].label;
              }
              return moment(ts).format('LTS');
            }
          }
        },
        legend: {
          position: 'top'
        }
      },
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        x: {
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
        },
        LHS: {
          type: 'linear',
          position: 'left'
        },
        RHS: {
          type: 'linear',
          position: 'right'
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
    chartTooltip.checkOffset = true;
    const chartOptions: any = {
      title: {
        text: this.mdsCounter.name
      },
      tooltip: {
        external: (context: any) => chartTooltip.customTooltips(context)
      }
    };
    _.merge(this.chart, { options: { plugins: chartOptions } });
  }

  private updateChart() {
    const chartDataset: any[] = [
      {
        data: this.convertTimeSeries(this.mdsCounter[this.lhsCounter])
      },
      {
        data: this.deltaTimeSeries(this.mdsCounter[this.rhsCounter])
      }
    ];
    _.merge(this.chart, {
      datasets: chartDataset
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
