import { Component, OnDestroy, OnInit } from '@angular/core';

import * as _ from 'lodash';

import { DashboardService } from '../dashboard.service';

@Component({
  selector: 'cd-health',
  templateUrl: './health.component.html',
  styleUrls: ['./health.component.scss']
})
export class HealthComponent implements OnInit, OnDestroy {
  contentData: any;
  interval: number;

  constructor(private dashboardService: DashboardService) {}

  ngOnInit() {
    this.getInfo();
    this.interval = window.setInterval(() => {
      this.getInfo();
    }, 5000);
  }

  ngOnDestroy() {
    clearInterval(this.interval);
  }

  getInfo() {
    this.dashboardService.getHealth().subscribe((data: any) => {
      this.contentData = data;
    });
  }

  prepareRawUsage(chart, data) {
    let rawUsageChartColor;

    const rawUsageText =
      Math.round(100 * (data.df.stats.total_used_bytes / data.df.stats.total_bytes)) + '%';

    if (data.df.stats.total_used_bytes / data.df.stats.total_bytes >= data.osd_map.full_ratio) {
      rawUsageChartColor = '#ff0000';
    } else if (
      data.df.stats.total_used_bytes / data.df.stats.total_bytes >=
      data.osd_map.backfillfull_ratio
    ) {
      rawUsageChartColor = '#ff6600';
    } else if (
      data.df.stats.total_used_bytes / data.df.stats.total_bytes >=
      data.osd_map.nearfull_ratio
    ) {
      rawUsageChartColor = '#ffc200';
    } else {
      rawUsageChartColor = '#00bb00';
    }

    chart.dataset[0].data = [data.df.stats.total_used_bytes, data.df.stats.total_avail_bytes];
    chart.options.center_text = rawUsageText;
    chart.colors = [{ backgroundColor: [rawUsageChartColor, '#424d52'] }];
    chart.labels = ['Raw Used', 'Raw Available'];
  }

  preparePoolUsage(chart, data) {
    const colors = [
      '#3366CC',
      '#109618',
      '#990099',
      '#3B3EAC',
      '#0099C6',
      '#DD4477',
      '#66AA00',
      '#B82E2E',
      '#316395',
      '#994499',
      '#22AA99',
      '#AAAA11',
      '#6633CC',
      '#E67300',
      '#8B0707',
      '#329262',
      '#5574A6',
      '#FF9900',
      '#DC3912',
      '#3B3EAC'
    ];

    const poolLabels = [];
    const poolData = [];

    _.each(data.df.pools, (pool, i) => {
      poolLabels.push(pool['name']);
      poolData.push(pool['stats']['bytes_used']);
    });

    chart.dataset[0].data = poolData;
    chart.colors = [{ backgroundColor: colors }];
    chart.labels = poolLabels;
  }
}
