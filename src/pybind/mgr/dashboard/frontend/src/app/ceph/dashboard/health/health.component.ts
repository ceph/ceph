import { Component, OnDestroy, OnInit } from '@angular/core';

import _ from 'lodash';
import { Subscription } from 'rxjs';
import { take } from 'rxjs/operators';

import { PgCategoryService } from '~/app/ceph/shared/pg-category.service';
import { HealthService } from '~/app/shared/api/health.service';
import { OsdService } from '~/app/shared/api/osd.service';
import { CssHelper } from '~/app/shared/classes/css-helper';
import { Icons } from '~/app/shared/enum/icons.enum';
import { OsdSettings } from '~/app/shared/models/osd-settings';
import { Permissions } from '~/app/shared/models/permissions';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';
import { DimlessPipe } from '~/app/shared/pipes/dimless.pipe';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import {
  FeatureTogglesMap$,
  FeatureTogglesService
} from '~/app/shared/services/feature-toggles.service';
import { RefreshIntervalService } from '~/app/shared/services/refresh-interval.service';

@Component({
  selector: 'cd-health',
  templateUrl: './health.component.html',
  styleUrls: ['./health.component.scss']
})
export class HealthComponent implements OnInit, OnDestroy {
  healthData: any;
  osdSettings = new OsdSettings();
  interval = new Subscription();
  permissions: Permissions;
  enabledFeature$: FeatureTogglesMap$;
  icons = Icons;
  color: string;

  clientStatsConfig: any = {};
  rawCapacityChartConfig: any = {};

  pgStatusChartConfig = {
    options: {
      events: ['']
    }
  };

  constructor(
    private healthService: HealthService,
    private osdService: OsdService,
    private authStorageService: AuthStorageService,
    private pgCategoryService: PgCategoryService,
    private featureToggles: FeatureTogglesService,
    private refreshIntervalService: RefreshIntervalService,
    private dimlessBinary: DimlessBinaryPipe,
    private dimless: DimlessPipe,
    private cssHelper: CssHelper
  ) {
    this.permissions = this.authStorageService.getPermissions();
    this.enabledFeature$ = this.featureToggles.get();
  }

  ngOnInit() {
    this.clientStatsConfig = {
      dataset: [
        {
          backgroundColor: [
            this.cssHelper.propertyValue('chart-color-cyan'),
            this.cssHelper.propertyValue('chart-color-purple')
          ]
        }
      ]
    };

    this.rawCapacityChartConfig = {
      dataset: [
        {
          backgroundColor: [
            this.cssHelper.propertyValue('chart-color-blue'),
            this.cssHelper.propertyValue('chart-color-gray')
          ]
        }
      ]
    };
    this.interval = this.refreshIntervalService.intervalData$.subscribe(() => {
      this.getHealth();
    });

    this.osdService
      .getOsdSettings()
      .pipe(take(1))
      .subscribe((data: any) => {
        this.osdSettings = data;
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
      `${$localize`Reads`}: ${this.dimless.transform(
        this.healthData.client_perf.read_op_per_sec
      )} ${$localize`/s`}`
    );
    ratioData.push(this.calcPercentage(this.healthData.client_perf.read_op_per_sec, total));
    ratioLabels.push(
      `${$localize`Writes`}: ${this.dimless.transform(
        this.healthData.client_perf.write_op_per_sec
      )} ${$localize`/s`}`
    );
    ratioData.push(this.calcPercentage(this.healthData.client_perf.write_op_per_sec, total));

    chart.labels = ratioLabels;
    chart.dataset[0].data = ratioData;
    chart.dataset[0].label = `${this.dimless.transform(total)}\n${$localize`IOPS`}`;
  }

  prepareClientThroughput(chart: Record<string, any>) {
    const ratioLabels = [];
    const ratioData = [];

    const total =
      this.healthData.client_perf.read_bytes_sec + this.healthData.client_perf.write_bytes_sec;

    ratioLabels.push(
      `${$localize`Reads`}: ${this.dimlessBinary.transform(
        this.healthData.client_perf.read_bytes_sec
      )}${$localize`/s`}`
    );
    ratioData.push(this.calcPercentage(this.healthData.client_perf.read_bytes_sec, total));
    ratioLabels.push(
      `${$localize`Writes`}: ${this.dimlessBinary.transform(
        this.healthData.client_perf.write_bytes_sec
      )}${$localize`/s`}`
    );
    ratioData.push(this.calcPercentage(this.healthData.client_perf.write_bytes_sec, total));

    chart.labels = ratioLabels;
    chart.dataset[0].data = ratioData;
    chart.dataset[0].label = `${this.dimlessBinary
      .transform(total)
      .replace(' ', '\n')}${$localize`/s`}`;
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

    if (percentUsed / 100 >= this.osdSettings.nearfull_ratio) {
      this.color = 'chart-color-red';
    } else if (percentUsed / 100 >= this.osdSettings.full_ratio) {
      this.color = 'chart-color-yellow';
    } else {
      this.color = 'chart-color-blue';
    }
    this.rawCapacityChartConfig.dataset[0].backgroundColor[0] = this.cssHelper.propertyValue(
      this.color
    );

    chart.dataset[0].data = [percentUsed, percentAvailable];

    chart.labels = [
      `${$localize`Used`}: ${this.dimlessBinary.transform(data.df.stats.total_used_raw_bytes)}`,
      `${$localize`Avail.`}: ${this.dimlessBinary.transform(
        data.df.stats.total_bytes - data.df.stats.total_used_raw_bytes
      )}`
    ];

    chart.dataset[0].label = `${percentUsed}%\nof ${this.dimlessBinary.transform(
      data.df.stats.total_bytes
    )}`;
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

    for (const categoryType of this.pgCategoryService.getAllTypes()) {
      if (_.isUndefined(categoryPgAmount[categoryType])) {
        categoryPgAmount[categoryType] = 0;
      }
    }

    chart.dataset[0].data = this.pgCategoryService
      .getAllTypes()
      .map((categoryType) => this.calcPercentage(categoryPgAmount[categoryType], totalPgs));

    chart.labels = [
      `${$localize`Clean`}: ${this.dimless.transform(categoryPgAmount['clean'])}`,
      `${$localize`Working`}: ${this.dimless.transform(categoryPgAmount['working'])}`,
      `${$localize`Warning`}: ${this.dimless.transform(categoryPgAmount['warning'])}`,
      `${$localize`Unknown`}: ${this.dimless.transform(categoryPgAmount['unknown'])}`
    ];

    chart.dataset[0].label = `${totalPgs}\n${$localize`PGs`}`;
  }

  prepareObjects(chart: Record<string, any>, data: Record<string, any>) {
    const objectCopies = data.pg_info.object_stats.num_object_copies;
    const healthy =
      objectCopies -
      data.pg_info.object_stats.num_objects_misplaced -
      data.pg_info.object_stats.num_objects_degraded -
      data.pg_info.object_stats.num_objects_unfound;
    const healthyPercentage = this.calcPercentage(healthy, objectCopies);
    const misplacedPercentage = this.calcPercentage(
      data.pg_info.object_stats.num_objects_misplaced,
      objectCopies
    );
    const degradedPercentage = this.calcPercentage(
      data.pg_info.object_stats.num_objects_degraded,
      objectCopies
    );
    const unfoundPercentage = this.calcPercentage(
      data.pg_info.object_stats.num_objects_unfound,
      objectCopies
    );

    chart.labels = [
      `${$localize`Healthy`}: ${healthyPercentage}%`,
      `${$localize`Misplaced`}: ${misplacedPercentage}%`,
      `${$localize`Degraded`}: ${degradedPercentage}%`,
      `${$localize`Unfound`}: ${unfoundPercentage}%`
    ];

    chart.dataset[0].data = [
      healthyPercentage,
      misplacedPercentage,
      degradedPercentage,
      unfoundPercentage
    ];

    chart.dataset[0].label = `${this.dimless.transform(
      data.pg_info.object_stats.num_objects
    )}\n${$localize`objects`}`;
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

    return Math.ceil((dividend / divisor) * 100 * 100) / 100;
  }
}
