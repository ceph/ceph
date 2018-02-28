import { Component, Input, OnChanges, OnInit } from '@angular/core';

import * as _ from 'lodash';

@Component({
  selector: 'cd-cephfs-chart',
  templateUrl: './cephfs-chart.component.html',
  styleUrls: ['./cephfs-chart.component.scss']
})
export class CephfsChartComponent implements OnChanges, OnInit {
  @Input() mdsCounter: any;

  lhsCounter = 'mds.inodes';
  rhsCounter = 'mds_server.handle_client_request';

  chart: any;

  constructor() {}

  ngOnInit() {
    if (_.isUndefined(this.mdsCounter)) {
      return;
    }

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
    _.each(sourceSeries, dp => {
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
