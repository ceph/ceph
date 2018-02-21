import { Component, OnDestroy, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { ActivatedRoute } from '@angular/router';

import * as _ from 'lodash';

import { DimlessBinaryPipe } from '../../../shared/pipes/dimless-binary.pipe';
import { DimlessPipe } from '../../../shared/pipes/dimless.pipe';
import { CephfsService } from '../cephfs.service';

@Component({
  selector: 'cd-cephfs',
  templateUrl: './cephfs.component.html',
  styleUrls: ['./cephfs.component.scss']
})
export class CephfsComponent implements OnInit, OnDestroy {
  @ViewChild('poolProgressTmpl') poolProgressTmpl: TemplateRef<any>;
  @ViewChild('activityTmpl') activityTmpl: TemplateRef<any>;

  routeParamsSubscribe: any;

  objectValues = Object.values;

  single: any[];
  multi: any[];

  view: any[] = [700, 400];

  id: number;
  name: string;
  ranks: any;
  pools: any;
  standbys = [];
  clientCount: number;

  mdsCounters = {};

  lhsCounter = 'mds.inodes';
  rhsCounter = 'mds_server.handle_client_request';
  charts = {};
  interval: any;

  constructor(
    private route: ActivatedRoute,
    private cephfsService: CephfsService,
    private dimlessBinary: DimlessBinaryPipe,
    private dimless: DimlessPipe
  ) {}

  ngOnInit() {
    this.ranks = {
      columns: [
        { prop: 'rank' },
        { prop: 'state' },
        { prop: 'mds', name: 'Daemon' },
        { prop: 'activity', cellTemplate: this.activityTmpl },
        { prop: 'dns', name: 'Dentries', pipe: this.dimless },
        { prop: 'inos', name: 'Inodes', pipe: this.dimless }
      ],
      data: []
    };

    this.pools = {
      columns: [
        { prop: 'pool' },
        { prop: 'type' },
        { prop: 'used', pipe: this.dimlessBinary },
        { prop: 'avail', pipe: this.dimlessBinary },
        {
          name: 'Usage',
          cellTemplate: this.poolProgressTmpl,
          comparator: (valueA, valueB, rowA, rowB, sortDirection) => {
            const valA = rowA.used / rowA.avail;
            const valB = rowB.used / rowB.avail;

            if (valA === valB) {
              return 0;
            }

            if (valA > valB) {
              return 1;
            } else {
              return -1;
            }
          }
        }
      ],
      data: []
    };

    this.routeParamsSubscribe = this.route.params.subscribe((params: { id: number }) => {
      this.id = params.id;

      this.ranks.data = [];
      this.pools.data = [];
      this.standbys = [];
      this.mdsCounters = {};
    });
  }

  ngOnDestroy() {
    this.routeParamsSubscribe.unsubscribe();
  }

  refresh() {
    this.cephfsService.getCephfs(this.id).subscribe((data: any) => {
      this.ranks.data = data.cephfs.ranks;
      this.pools.data = data.cephfs.pools;
      this.standbys = [
        {
          key: 'Standby daemons',
          value: data.standbys.map(value => value.name).join(', ')
        }
      ];
      this.name = data.cephfs.name;
      this.clientCount = data.cephfs.client_count;
      this.draw_chart();
    });
  }

  draw_chart() {
    this.cephfsService.getMdsCounters(this.id).subscribe(data => {
      const topChart = true;

      _.each(this.mdsCounters, (value, key) => {
        if (data[key] === undefined) {
          delete this.mdsCounters[key];
        }
      });

      _.each(data, (mdsData, mdsName) => {
        const lhsData = this.convert_timeseries(mdsData[this.lhsCounter]);
        const rhsData = this.delta_timeseries(mdsData[this.rhsCounter]);

        if (this.mdsCounters[mdsName] === undefined) {
          this.mdsCounters[mdsName] = {
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
                position: 'top',
                display: topChart
              },
              scales: {
                xAxes: [
                  {
                    position: 'top',
                    type: 'time',
                    display: topChart,
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
        } else {
          this.mdsCounters[mdsName].datasets[0].data = lhsData;
          this.mdsCounters[mdsName].datasets[1].data = rhsData;
        }
      });
    });
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
