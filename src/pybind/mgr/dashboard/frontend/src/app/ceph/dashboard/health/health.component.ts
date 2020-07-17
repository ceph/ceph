import { Component, OnDestroy, OnInit } from '@angular/core';

import * as _ from 'lodash';
import { Subscription } from 'rxjs';

import { HealthService } from '../../../shared/api/health.service';
import { Icons } from '../../../shared/enum/icons.enum';
import { Permissions } from '../../../shared/models/permissions';
import { DimlessBinaryPipe } from '../../../shared/pipes/dimless-binary.pipe';
import { DimlessPipe } from '../../../shared/pipes/dimless.pipe';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import {
  FeatureTogglesMap$,
  FeatureTogglesService
} from '../../../shared/services/feature-toggles.service';
import { RefreshIntervalService } from '../../../shared/services/refresh-interval.service';
import { PgCategoryService } from '../../shared/pg-category.service';
import { HealthPieColor } from '../health-pie/health-pie-color.enum';

@Component({
  selector: 'cd-health',
  templateUrl: './health.component.html',
  styleUrls: ['./health.component.scss']
})
export class HealthComponent implements OnInit, OnDestroy {
  healthData: any;
  interval = new Subscription();
  permissions: Permissions;
  enabledFeature$: FeatureTogglesMap$;
  icons = Icons;

  rawCapacityChartConfig = {
    options: {
      title: { display: true, position: 'bottom' }
    }
  };
  objectsChartConfig = {
    options: {
      title: { display: true, position: 'bottom' }
    },
    colors: [
      {
        backgroundColor: [
          HealthPieColor.DEFAULT_GREEN,
          HealthPieColor.DEFAULT_MAGENTA,
          HealthPieColor.DEFAULT_ORANGE,
          HealthPieColor.DEFAULT_RED
        ]
      }
    ]
  };
  pgStatusChartConfig = {
    colors: [
      {
        backgroundColor: [
          HealthPieColor.DEFAULT_GREEN,
          HealthPieColor.DEFAULT_BLUE,
          HealthPieColor.DEFAULT_ORANGE,
          HealthPieColor.DEFAULT_RED
        ]
      }
    ]
  };

  constructor(
    private healthService: HealthService,
    private authStorageService: AuthStorageService,
    private pgCategoryService: PgCategoryService,
    private featureToggles: FeatureTogglesService,
    private refreshIntervalService: RefreshIntervalService,
    private dimlessBinary: DimlessBinaryPipe,
    private dimless: DimlessPipe
  ) {
    this.permissions = this.authStorageService.getPermissions();
    this.enabledFeature$ = this.featureToggles.get();
  }

  ngOnInit() {
    this.interval = this.refreshIntervalService.intervalData$.subscribe(() => {
      this.getHealth();
    });
  }

  ngOnDestroy() {
    this.interval.unsubscribe();
  }

  getHealth() {
    this.healthService.getMinimalHealth().subscribe((data: any) => {
      this.healthData = data;
    });
  }

  prepareReadWriteRatio(chart: Record<string, any>) {
    const ratioLabels = [];
    const ratioData = [];

    const total =
      this.healthData.client_perf.write_op_per_sec + this.healthData.client_perf.read_op_per_sec;

    ratioLabels.push(
      `${$localize`Writes`} (${this.calcPercentage(
        this.healthData.client_perf.write_op_per_sec,
        total
      )}%)`
    );
    ratioData.push(this.healthData.client_perf.write_op_per_sec);
    ratioLabels.push(
      `${$localize`Reads`} (${this.calcPercentage(
        this.healthData.client_perf.read_op_per_sec,
        total
      )}%)`
    );
    ratioData.push(this.healthData.client_perf.read_op_per_sec);

    chart.dataset[0].data = ratioData;
    chart.labels = ratioLabels;
  }

  prepareRawUsage(chart: Record<string, any>, data: Record<string, any>) {
    const percentAvailable = this.calcPercentage(
      data.df.stats.total_bytes - data.df.stats.total_used_raw_bytes,
      data.df.stats.total_bytes
    );
    const percentUsed = this.calcPercentage(
      data.df.stats.total_used_raw_bytes,
      data.df.stats.total_bytes
    );

    chart.dataset[0].data = [data.df.stats.total_used_raw_bytes, data.df.stats.total_avail_bytes];

    chart.labels = [
      $localize`${this.dimlessBinary.transform(
        data.df.stats.total_used_raw_bytes
      )} Used (${percentUsed}%)`,
      $localize`${this.dimlessBinary.transform(
        data.df.stats.total_bytes - data.df.stats.total_used_raw_bytes
      )} Avail. (${percentAvailable}%)`
    ];

    chart.options.title.text = `${this.dimlessBinary.transform(
      data.df.stats.total_bytes
    )} ${$localize`total`}`;
  }

  preparePgStatus(chart: Record<string, any>, data: Record<string, any>) {
    const categoryPgAmount: Record<string, number> = {};
    let totalPgs = 0;

    _.forEach(data.pg_info.statuses, (pgAmount, pgStatesText) => {
      const categoryType = this.pgCategoryService.getTypeByStates(pgStatesText);

      if (_.isUndefined(categoryPgAmount[categoryType])) {
        categoryPgAmount[categoryType] = 0;
      }
      categoryPgAmount[categoryType] += pgAmount;
      totalPgs += pgAmount;
    });

    chart.dataset[0].data = this.pgCategoryService
      .getAllTypes()
      .map((categoryType) => categoryPgAmount[categoryType]);

    chart.labels = [
      `${$localize`Clean`} (${this.calcPercentage(categoryPgAmount['clean'], totalPgs)}%)`,
      `${$localize`Working`} (${this.calcPercentage(categoryPgAmount['working'], totalPgs)}%)`,
      `${$localize`Warning`} (${this.calcPercentage(categoryPgAmount['warning'], totalPgs)}%)`,
      `${$localize`Unknown`} (${this.calcPercentage(categoryPgAmount['unknown'], totalPgs)}%)`
    ];
  }

  prepareObjects(chart: Record<string, any>, data: Record<string, any>) {
    const totalReplicas = data.pg_info.object_stats.num_object_copies;
    const healthy =
      totalReplicas -
      data.pg_info.object_stats.num_objects_misplaced -
      data.pg_info.object_stats.num_objects_degraded -
      data.pg_info.object_stats.num_objects_unfound;

    chart.labels = [
      `${$localize`Healthy`} (${this.calcPercentage(healthy, totalReplicas)}%)`,
      `${$localize`Misplaced`} (${this.calcPercentage(
        data.pg_info.object_stats.num_objects_misplaced,
        totalReplicas
      )}%)`,
      `${$localize`Degraded`} (${this.calcPercentage(
        data.pg_info.object_stats.num_objects_degraded,
        totalReplicas
      )}%)`,
      `${$localize`Unfound`} (${this.calcPercentage(
        data.pg_info.object_stats.num_objects_unfound,
        totalReplicas
      )}%)`
    ];

    chart.dataset[0].data = [
      healthy,
      data.pg_info.object_stats.num_objects_misplaced,
      data.pg_info.object_stats.num_objects_degraded,
      data.pg_info.object_stats.num_objects_unfound
    ];

    chart.options.title.text = `${this.dimless.transform(
      data.pg_info.object_stats.num_objects
    )} ${$localize`total`} (${this.dimless.transform(totalReplicas)} ${$localize`replicas`})`;

    chart.options.maintainAspectRatio = window.innerWidth >= 375;
  }

  isClientReadWriteChartShowable() {
    const readOps = this.healthData.client_perf.read_op_per_sec || 0;
    const writeOps = this.healthData.client_perf.write_op_per_sec || 0;

    return readOps + writeOps > 0;
  }

  private calcPercentage(dividend: number, divisor: number) {
    if (!_.isNumber(dividend) || !_.isNumber(divisor) || divisor === 0) {
      return 0;
    }

    return Math.round((dividend / divisor) * 100);
  }
}
