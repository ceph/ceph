import { Component, ElementRef, Input, OnChanges, OnInit, ViewChild } from '@angular/core';

import * as _ from 'lodash';
import * as moment from 'moment';

import { ChartTooltip } from '../../../shared/models/chart-tooltip';

@Component({
  selector: 'cd-cephfs-chart',
  templateUrl: './cephfs-chart.component.html',
  styleUrls: ['./cephfs-chart.component.scss']
})
export class CephfsChartComponent implements OnChanges, OnInit {
  @ViewChild('chartCanvas')
  chartCanvas: ElementRef;
  @ViewChild('chartTooltip')
  chartTooltip: ElementRef;

  @Input()
  mdsCounter: any;

  lhsCounter = 'mds.inodes';
  rhsCounter = 'mds_server.handle_client_request';

  chart: any;

  constructor() {}

  ngOnInit() {
    if (_.isUndefined(this.mdsCounter)) {
      return;
    }

    const getTitle = (ts) => {
      return moment(ts, 'x').format('LTS');
    };

    const getStyleTop = (tooltip) => {
      return tooltip.caretY - tooltip.height - 15 + 'px';
    };

    const getStyleLeft = (tooltip) => {
      return tooltip.caretX + 'px';
    };

    const chartTooltip = new ChartTooltip(
      this.chartCanvas,
      this.chartTooltip,
      getStyleLeft,
      getStyleTop
    );
    chartTooltip.getTitle = getTitle;
    chartTooltip.checkOffset = true;

    const lhsData = this.convert_timeseries(this.mdsCounter[this.lhsCounter]);
    const rhsData = this.delta_timeseries(this.mdsCounter[this.rhsCounter]);

    this.chart = {
      datasets: [
        {
          label: this.lhsCounter,
          yAxisID: 'LHS',
          data: lhsData,
          tension: 0.1
        },
        {
          label: this.rhsCounter,
          yAxisID: 'RHS',
          data: rhsData,
          tension: 0.1
        }
      ],
      options: {
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
              position: 'left',
              min: 0
            },
            {
              id: 'RHS',
              type: 'linear',
              position: 'right',
              min: 0
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
            title: function(tooltipItems, data) {
              let ts = 0;
              if (tooltipItems.length > 0) {
                const item = tooltipItems[0];
                ts = data.datasets[item.datasetIndex].data[item.index].x;
              }
              return ts;
            }
          },
          custom: (tooltip) => {
            chartTooltip.customTooltips(tooltip);
          }
        }
      },
      chartType: 'line'
    };
  }

  ngOnChanges() {
    if (!this.chart) {
      return;
    }

    const lhsData = this.convert_timeseries(this.mdsCounter[this.lhsCounter]);
    const rhsData = this.delta_timeseries(this.mdsCounter[this.rhsCounter]);

    this.chart.datasets[0].data = lhsData;
    this.chart.datasets[1].data = rhsData;
  }

  // Convert ceph-mgr's time series format (list of 2-tuples
  // with seconds-since-epoch timestamps) into what chart.js
  // can handle (list of objects with millisecs-since-epoch
  // timestamps)
  convert_timeseries(sourceSeries) {
    const data = [];
    _.each(sourceSeries, (dp) => {
      data.push({
        x: dp[0] * 1000,
        y: dp[1]
      });
    });

    return data;
  }

  delta_timeseries(sourceSeries) {
    let i;
    let prev = sourceSeries[0];
    const result = [];
    for (i = 1; i < sourceSeries.length; i++) {
      const cur = sourceSeries[i];
      const tdelta = cur[0] - prev[0];
      const vdelta = cur[1] - prev[1];
      const rate = vdelta / tdelta;

      result.push({
        x: cur[0] * 1000,
        y: rate
      });

      prev = cur;
    }
    return result;
  }
}
