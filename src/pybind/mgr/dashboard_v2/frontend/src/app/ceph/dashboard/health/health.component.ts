import { Component, OnDestroy, OnInit } from '@angular/core';

import * as Chart from 'chart.js';
import * as _ from 'lodash';

import { DimlessBinaryPipe } from '../../../shared/pipes/dimless-binary.pipe';
import { DashboardService } from '../dashboard.service';

@Component({
  selector: 'cd-health',
  templateUrl: './health.component.html',
  styleUrls: ['./health.component.scss']
})
export class HealthComponent implements OnInit, OnDestroy {
  contentData: any;
  interval: any;
  poolUsage: any = {
    chartType: 'doughnut'
  };
  rawUsage: any = {
    chartType: 'doughnut',
    center_text: 0
  };

  constructor(
    private dimlessBinary: DimlessBinaryPipe,
    private dashboardService: DashboardService
  ) {}

  ngOnInit() {
    // An extension to Chart.js to enable rendering some
    // text in the middle of a doughnut
    Chart.pluginService.register({
      beforeDraw: function(chart) {
        if (!chart.options.center_text) {
          return;
        }
        const width = chart.chart.width,
          height = chart.chart.height,
          ctx = chart.chart.ctx;

        ctx.restore();
        const fontSize = (height / 114).toFixed(2);
        ctx.font = fontSize + 'em sans-serif';
        ctx.textBaseline = 'middle';

        const text = chart.options.center_text,
          textX = Math.round((width - ctx.measureText(text).width) / 2),
          textY = height / 2;

        ctx.fillText(text, textX, textY);
        ctx.save();
      }
    });

    this.getInfo();
    this.interval = setInterval(() => {
      this.getInfo();
    }, 5000);
  }

  ngOnDestroy() {
    clearInterval(this.interval);
  }

  getInfo() {
    this.dashboardService.getHealth().subscribe((data: any) => {
      this.contentData = data;
      this.draw_usage_charts();
    });
  }

  draw_usage_charts() {
    let rawUsageChartColor;
    const rawUsageText =
      Math.round(
        100 *
          (this.contentData.df.stats.total_used_bytes /
            this.contentData.df.stats.total_bytes)
      ) + '%';
    if (
      this.contentData.df.stats.total_used_bytes /
        this.contentData.df.stats.total_bytes >=
      this.contentData.osd_map.full_ratio
    ) {
      rawUsageChartColor = '#ff0000';
    } else if (
      this.contentData.df.stats.total_used_bytes /
        this.contentData.df.stats.total_bytes >=
      this.contentData.osd_map.backfillfull_ratio
    ) {
      rawUsageChartColor = '#ff6600';
    } else if (
      this.contentData.df.stats.total_used_bytes /
        this.contentData.df.stats.total_bytes >=
      this.contentData.osd_map.nearfull_ratio
    ) {
      rawUsageChartColor = '#ffc200';
    } else {
      rawUsageChartColor = '#00bb00';
    }

    this.rawUsage = {
      chartType: 'doughnut',
      dataset: [
        {
          label: null,
          borderWidth: 0,
          data: [
            this.contentData.df.stats.total_used_bytes,
            this.contentData.df.stats.total_avail_bytes
          ]
        }
      ],
      options: {
        center_text: rawUsageText,
        responsive: true,
        legend: { display: false },
        animation: { duration: 0 },
        tooltips: {
          callbacks: {
            label: (tooltipItem, chart) => {
              return (
                chart.labels[tooltipItem.index] +
                ': ' +
                this.dimlessBinary.transform(
                  chart.datasets[0].data[tooltipItem.index]
                )
              );
            }
          }
        }
      },
      colors: [
        {
          backgroundColor: [rawUsageChartColor, '#424d52'],
          borderColor: 'transparent'
        }
      ],
      labels: ['Raw Used', 'Raw Available']
    };

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

    _.each(this.contentData.df.pools, function(pool, i) {
      poolLabels.push(pool['name']);
      poolData.push(pool['stats']['bytes_used']);
    });

    this.poolUsage = {
      chartType: 'doughnut',
      dataset: [
        {
          label: null,
          borderWidth: 0,
          data: poolData
        }
      ],
      options: {
        responsive: true,
        legend: { display: false },
        animation: { duration: 0 },
        tooltips: {
          callbacks: {
            label: (tooltipItem, chart) => {
              return (
                chart.labels[tooltipItem.index] +
                ': ' +
                this.dimlessBinary.transform(
                  chart.datasets[0].data[tooltipItem.index]
                )
              );
            }
          }
        }
      },
      colors: [
        {
          backgroundColor: colors,
          borderColor: 'transparent'
        }
      ],
      labels: poolLabels
    };
  }
}
