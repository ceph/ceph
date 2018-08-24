import { ViewportScroller } from '@angular/common';
import { Component, OnDestroy, OnInit } from '@angular/core';

import * as _ from 'lodash';

import { DashboardService } from '../../../shared/api/dashboard.service';

@Component({
  selector: 'cd-health',
  templateUrl: './health.component.html',
  styleUrls: ['./health.component.scss']
})
export class HealthComponent implements OnInit, OnDestroy {
  contentData: any;
  interval: number;

  constructor(
    private dashboardService: DashboardService,
    public viewportScroller: ViewportScroller
  ) {}

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

  prepareReadWriteRatio(chart, data) {
    const ratioLabels = [];
    const ratioData = [];

    ratioLabels.push('Writes');
    ratioData.push(this.contentData.client_perf.write_op_per_sec);
    ratioLabels.push('Reads');
    ratioData.push(this.contentData.client_perf.read_op_per_sec);

    chart.dataset[0].data = ratioData;
    chart.labels = ratioLabels;
  }

  prepareRawUsage(chart, data) {
    const percentAvailable = Math.round(
      100 *
        ((data.df.stats.total_bytes - data.df.stats.total_used_bytes) / data.df.stats.total_bytes)
    );

    const percentUsed = Math.round(
      100 * (data.df.stats.total_used_bytes / data.df.stats.total_bytes)
    );

    chart.dataset[0].data = [data.df.stats.total_used_bytes, data.df.stats.total_avail_bytes];
    if (chart === 'doughnut') {
      chart.options.cutoutPercentage = 65;
    }
    chart.labels = [`Used (${percentUsed}%)`, `Avail. (${percentAvailable}%)`];
  }

  preparePgStatus(chart, data) {
    const pgCategoryClean = 'Clean';
    const pgCategoryCleanStates = ['active', 'clean'];
    const pgCategoryWarning = 'Warning';
    const pgCategoryWarningStates = [
      'backfill_toofull',
      'backfill_unfound',
      'down',
      'incomplete',
      'inconsistent',
      'recovery_toofull',
      'recovery_unfound',
      'remapped',
      'snaptrim_error',
      'stale',
      'undersized'
    ];
    const pgCategoryUnknown = 'Unknown';
    const pgCategoryWorking = 'Working';
    const pgCategoryWorkingStates = [
      'activating',
      'backfill_wait',
      'backfilling',
      'creating',
      'deep',
      'degraded',
      'forced_backfill',
      'forced_recovery',
      'peering',
      'peered',
      'recovering',
      'recovery_wait',
      'repair',
      'scrubbing',
      'snaptrim',
      'snaptrim_wait'
    ];
    let totalPgClean = 0;
    let totalPgWarning = 0;
    let totalPgUnknown = 0;
    let totalPgWorking = 0;

    _.forEach(data.pg_info.statuses, (pgAmount, pgStatesText) => {
      const pgStates = pgStatesText.split('+');
      const isWarning = _.intersection(pgCategoryWarningStates, pgStates).length > 0;
      const pgWorkingStates = _.intersection(pgCategoryWorkingStates, pgStates);
      const pgCleanStates = _.intersection(pgCategoryCleanStates, pgStates);

      if (isWarning) {
        totalPgWarning += pgAmount;
      } else if (pgStates.length > pgCleanStates.length + pgWorkingStates.length) {
        totalPgUnknown += pgAmount;
      } else if (pgWorkingStates.length > 0) {
        totalPgWorking = pgAmount;
      } else {
        totalPgClean += pgAmount;
      }
    });

    chart.labels = [pgCategoryWarning, pgCategoryClean, pgCategoryUnknown, pgCategoryWorking];
    chart.dataset[0].data = [totalPgWarning, totalPgClean, totalPgUnknown, totalPgWorking];
  }
}
