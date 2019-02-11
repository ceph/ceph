import { Component, OnDestroy, OnInit } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';
import * as _ from 'lodash';

import { HealthService } from '../../../shared/api/health.service';
import { Permissions } from '../../../shared/models/permissions';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import {
  FeatureTogglesMap$,
  FeatureTogglesService
} from '../../../shared/services/feature-toggles.service';
import { PgCategoryService } from '../../shared/pg-category.service';
import { HealthPieColor } from '../health-pie/health-pie-color.enum';

@Component({
  selector: 'cd-health',
  templateUrl: './health.component.html',
  styleUrls: ['./health.component.scss']
})
export class HealthComponent implements OnInit, OnDestroy {
  healthData: any;
  interval: number;
  permissions: Permissions;
  enabledFeature$: FeatureTogglesMap$;

  constructor(
    private healthService: HealthService,
    private i18n: I18n,
    private authStorageService: AuthStorageService,
    private pgCategoryService: PgCategoryService,
    private featureToggles: FeatureTogglesService
  ) {
    this.permissions = this.authStorageService.getPermissions();
    this.enabledFeature$ = this.featureToggles.get();
  }

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
        ((data.df.stats.total_bytes - data.df.stats.total_used_raw_bytes) /
          data.df.stats.total_bytes)
    );

    const percentUsed = Math.round(
      100 * (data.df.stats.total_used_raw_bytes / data.df.stats.total_bytes)
    );

    chart.dataset[0].data = [data.df.stats.total_used_raw_bytes, data.df.stats.total_avail_bytes];
    if (chart === 'doughnut') {
      chart.options.cutoutPercentage = 65;
    }
    chart.labels = [
      `${this.i18n('Used')} (${percentUsed}%)`,
      `${this.i18n('Avail.')} (${percentAvailable}%)`
    ];
  }

  preparePgStatus(chart, data) {
    const categoryPgAmount = {};
    chart.labels = [
      this.i18n('Clean'),
      this.i18n('Working'),
      this.i18n('Warning'),
      this.i18n('Unknown')
    ];
    chart.colors = [
      {
        backgroundColor: [
          HealthPieColor.DEFAULT_GREEN,
          HealthPieColor.DEFAULT_BLUE,
          HealthPieColor.DEFAULT_ORANGE,
          HealthPieColor.DEFAULT_RED
        ]
      }
    ];

    _.forEach(data.pg_info.statuses, (pgAmount, pgStatesText) => {
      const categoryType = this.pgCategoryService.getTypeByStates(pgStatesText);

      if (_.isUndefined(categoryPgAmount[categoryType])) {
        categoryPgAmount[categoryType] = 0;
      }
      categoryPgAmount[categoryType] += pgAmount;
    });

    chart.dataset[0].data = this.pgCategoryService
      .getAllTypes()
      .map((categoryType) => categoryPgAmount[categoryType]);
  }

  isClientReadWriteChartShowable() {
    const readOps = this.healthData.client_perf.read_op_per_sec || 0;
    const writeOps = this.healthData.client_perf.write_op_per_sec || 0;

    return readOps + writeOps > 0;
  }
}
