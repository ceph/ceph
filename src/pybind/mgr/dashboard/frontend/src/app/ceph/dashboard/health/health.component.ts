import { Component, OnDestroy, OnInit } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';
import * as _ from 'lodash';

import { HealthService } from '../../../shared/api/health.service';

@Component({
  selector: 'cd-health',
  templateUrl: './health.component.html',
  styleUrls: ['./health.component.scss']
})
export class HealthComponent implements OnInit, OnDestroy {
  healthData: any;
  interval: number;

  constructor(private healthService: HealthService, private i18n: I18n) {}

  ngOnInit() {
    this.getHealth();
    this.interval = window.setInterval(() => {
      this.getHealth();
    }, 5000);
  }

  ngOnDestroy() {
    clearInterval(this.interval);
  }

  getHealth() {
    this.healthService.getMinimalHealth().subscribe((data: any) => {
      this.healthData = data;
    });
  }

  prepareReadWriteRatio(chart, data) {
    const ratioLabels = [];
    const ratioData = [];

    ratioLabels.push(this.i18n('Writes'));
    ratioData.push(this.healthData.client_perf.write_op_per_sec);
    ratioLabels.push(this.i18n('Reads'));
    ratioData.push(this.healthData.client_perf.read_op_per_sec);

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
    chart.labels = [
      `${this.i18n('Used')} (${percentUsed}%)`,
      `${this.i18n('Avail.')} (${percentAvailable}%)`
    ];
  }

  preparePgStatus(chart, data) {
    const pgCategoryClean = this.i18n('Clean');
    const pgCategoryCleanStates = ['active', 'clean'];
    const pgCategoryWarning = this.i18n('Warning');
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
    const pgCategoryUnknown = this.i18n('Unknown');
    const pgCategoryWorking = this.i18n('Working');
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
