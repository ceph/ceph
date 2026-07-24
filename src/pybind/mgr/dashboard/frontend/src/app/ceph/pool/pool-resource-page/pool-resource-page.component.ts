import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute, ParamMap } from '@angular/router';

import _ from 'lodash';
import { forkJoin, of, Subscription } from 'rxjs';
import { switchMap } from 'rxjs/operators';

import { PoolService } from '~/app/shared/api/pool.service';
import { ErasureCodeProfileService } from '~/app/shared/api/erasure-code-profile.service';
import { CdHelperClass } from '~/app/shared/classes/cd-helper.class';
import { OverviewField } from '~/app/shared/components/resource-overview-card/resource-overview-card.component';
import { ChartPoint } from '~/app/shared/models/area-chart-point';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { RbdConfigurationEntry } from '~/app/shared/models/configuration';
import { ErasureCodeProfile } from '~/app/shared/models/erasure-code-profile';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { getPoolDataProtection, mapPoolApplications, Pool, PoolType } from '../pool';
import { PoolStat, PoolStats } from '../pool-stat';
import { PoolOverviewModel } from '~/app/shared/models/pool-overview.model';

@Component({
  selector: 'cd-pool-resource-page',
  templateUrl: './pool-resource-page.component.html',
  styleUrls: ['./pool-resource-page.component.scss'],
  standalone: false
})
export class PoolResourcePageComponent implements OnInit, OnDestroy {
  private sub = new Subscription();
  poolName = '';
  section = '';
  poolOverviewFields: OverviewField[] = [];
  cacheTierColumns: Array<CdTableColumn> = [];
  poolDetails!: object;
  selectedPoolConfiguration: RbdConfigurationEntry[] = [];
  cacheTiers: Pool[] = [];
  overviewModel: PoolOverviewModel = {
    name: '',
    type: '',
    dataProtection: '',
    applications: [] as string[],
    pgStatus: '',
    crushRuleset: '',
    usageTotal: 0,
    usageUsed: null as number | null,
    usagePercent: '',
    usedCapacity: '',
    availableCapacity: '',
    totalCapacity: '',
    quotaLimit: '',
    isErasure: false,
    typeLabel: '',
    replicationSize: '',
    minSize: '',
    erasureK: '',
    erasureM: '',
    erasureTotal: '',
    erasurePlugin: '',
    failureDomain: '',
    readThroughput: '',
    readOps: '',
    readOpsChartData: [] as ChartPoint[],
    writeThroughput: '',
    writeOps: '',
    writeOpsChartData: [] as ChartPoint[]
  };

  omittedPoolAttributes = ['cdExecuting', 'cdIsBinary', 'stats'];
  private readonly binaryUnits = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB'];

  constructor(
    private route: ActivatedRoute,
    private poolService: PoolService,
    private erasureCodeProfileService: ErasureCodeProfileService,
    private formatter: FormatterService
  ) {
    this.cacheTierColumns = [
      { prop: 'pool_name', name: $localize`Name`, flexGrow: 3 },
      { prop: 'cache_mode', name: $localize`Cache Mode`, flexGrow: 2 },
      { prop: 'cache_min_evict_age', name: $localize`Min Evict Age`, flexGrow: 2 },
      { prop: 'cache_min_flush_age', name: $localize`Min Flush Age`, flexGrow: 2 },
      { prop: 'target_max_bytes', name: $localize`Target Max Bytes`, flexGrow: 2 },
      { prop: 'target_max_objects', name: $localize`Target Max Objects`, flexGrow: 2 }
    ];
  }

  ngOnInit(): void {
    this.section = this.route.snapshot.data['section'] ?? '';

    this.sub.add(
      this.route.parent?.paramMap
        .pipe(
          switchMap((pm: ParamMap) => {
            this.poolName = pm.get('name') ?? '';

            if (!this.poolName) {
              this.poolOverviewFields = [];
              return of(null);
            }

            return forkJoin([
              this.poolService.get(this.poolName, true),
              this.poolService.getConfiguration(this.poolName),
              this.erasureCodeProfileService.list(),
              /* Fetch a list of pools ONLY for cache tiers */
              this.poolService.list([
                'pool',
                'pool_name',
                'cache_mode',
                'cache_min_evict_age',
                'cache_min_flush_age',
                'target_max_bytes',
                'target_max_objects'
              ])
            ]);
          })
        )
        .subscribe((result: any[] | null) => {
          if (!result) return;

          const [poolData, poolConf, ecProfiles, lightweightPoolList] = result as [
            any,
            RbdConfigurationEntry[],
            ErasureCodeProfile[],
            Pool[]
          ];

          this.poolDetails = _.omit(poolData || {}, this.omittedPoolAttributes);
          this.selectedPoolConfiguration = poolConf || [];

          // Map the integer IDs from 'tiers' to the matching pools in the lightweight list
          const tierIds = Array.isArray(poolData?.tiers) ? poolData.tiers : [];
          this.cacheTiers = (lightweightPoolList || []).filter((item: Pool) =>
            tierIds.includes(item.pool)
          );

          const selectedErasureProfile = this.getErasureProfile(poolData, ecProfiles || []);
          this.poolOverviewFields = this.buildOverviewFields(poolData);
          this.overviewModel = this.buildOverviewModel(poolData, selectedErasureProfile);

          CdHelperClass.updateChanged(this, {
            poolDetails: this.poolDetails,
            selectedPoolConfiguration: this.selectedPoolConfiguration,
            cacheTiers: this.cacheTiers,
            poolOverviewFields: this.poolOverviewFields,
            overviewModel: this.overviewModel
          });
        })
    );
  }

  ngOnDestroy(): void {
    this.sub.unsubscribe();
  }

  private buildOverviewModel(pool?: Pool, erasureProfile?: ErasureCodeProfile): PoolOverviewModel {
    const poolData = (pool as Pool) || ({} as Pool);
    const stats = (poolData.stats || {}) as PoolStats;
    const usageUsed = this.getStatLatest(stats.bytes_used);
    const usageAvailable = this.getStatLatest(stats.avail_raw);
    const usageTotal =
      usageUsed !== null && usageAvailable !== null ? usageUsed + usageAvailable : null;
    const readOpsChartData = this.getRateChartData(stats.rd, $localize`Read Ops`);
    const writeOpsChartData = this.getRateChartData(stats.wr, $localize`Write Ops`);
    const isErasure = poolData.type === PoolType.ERASURE;
    const erasureK = _.isNumber(erasureProfile?.k) ? `${erasureProfile?.k}` : '';
    const erasureM = _.isNumber(erasureProfile?.m) ? `${erasureProfile?.m}` : '';
    const erasureTotal =
      _.isNumber(erasureProfile?.k) && _.isNumber(erasureProfile?.m)
        ? `${(erasureProfile?.k as number) + (erasureProfile?.m as number)}`
        : '';
    const typeLabel = isErasure ? $localize`Erasure Coded` : poolData.type || '';

    return {
      name: poolData.pool_name || this.poolName,
      type: poolData.type || '',
      typeLabel,
      dataProtection: getPoolDataProtection(poolData),
      applications: mapPoolApplications(poolData.application_metadata || []),
      pgStatus: this.transformPgStatus(poolData.pg_status),
      crushRuleset: poolData.crush_rule,
      usageTotal: usageTotal ?? 0,
      usageUsed,
      usagePercent:
        usageTotal && usageTotal > 0 && usageUsed !== null
          ? `${Math.round((usageUsed / usageTotal) * 1000) / 10}%`
          : '',
      usedCapacity: this.formatBytes(usageUsed),
      availableCapacity: this.formatBytes(usageAvailable),
      totalCapacity: this.formatBytes(usageTotal),
      quotaLimit:
        Number.isFinite(poolData.quota_max_bytes) && poolData.quota_max_bytes > 0
          ? this.formatBytes(poolData.quota_max_bytes)
          : '',
      isErasure,
      replicationSize: Number.isFinite(poolData.size) ? `${poolData.size}` : '',
      minSize: Number.isFinite(poolData.min_size) ? `${poolData.min_size}` : '',
      erasureK,
      erasureM,
      erasureTotal,
      erasurePlugin: erasureProfile?.plugin || '',
      failureDomain: erasureProfile?.['crush-failure-domain'] || '',
      readThroughput: this.formatRate(this.getStatRate(stats.rd_bytes), true) || '',
      readOps: this.formatRate(this.getStatRate(stats.rd), false) || '',
      readOpsChartData,
      writeThroughput: this.formatRate(this.getStatRate(stats.wr_bytes), true) || '',
      writeOps: this.formatRate(this.getStatRate(stats.wr), false) || '',
      writeOpsChartData
    };
  }

  private getErasureProfile(
    pool: Pool,
    profiles: ErasureCodeProfile[]
  ): ErasureCodeProfile | undefined {
    if (pool?.type !== PoolType.ERASURE || !pool?.erasure_code_profile) {
      return undefined;
    }

    return profiles.find((profile) => profile.name === pool.erasure_code_profile);
  }

  private buildOverviewFields(pool?: Pool): OverviewField[] {
    const poolData = (pool as Pool) || ({} as Pool);

    return [
      {
        label: $localize`Name`,
        value: poolData.pool_name || this.poolName
      },
      {
        label: $localize`Type`,
        value: poolData.type || ''
      },
      {
        label: $localize`Data Protection`,
        value: getPoolDataProtection(poolData)
      },
      {
        label: $localize`Applications`,
        values: mapPoolApplications(poolData.application_metadata || []),
        type: 'tags'
      },
      {
        label: $localize`PG Status`,
        value: this.transformPgStatus(poolData.pg_status)
      }
    ];
  }

  private transformPgStatus(pgStatus: any): string {
    if (!pgStatus || typeof pgStatus === 'string') {
      return pgStatus || '';
    }

    return (
      Object.entries(pgStatus)
        .map(([state, count]) => `${count} ${state}`)
        .join(', ') || ''
    );
  }

  private getStatLatest(stat?: PoolStat): number | null {
    return Number.isFinite(stat?.latest) ? (stat as PoolStat).latest : null;
  }

  private getStatRate(stat?: PoolStat): number | null {
    return Number.isFinite(stat?.rate) ? (stat as PoolStat).rate : null;
  }

  private getRateChartData(stat: PoolStat | undefined, groupLabel: string): ChartPoint[] {
    if (!Array.isArray(stat?.rates)) {
      return [];
    }

    const fallbackStart = Date.now() - Math.max(stat.rates.length - 1, 0) * 60000;

    return stat.rates.reduce((points: ChartPoint[], point: any, index: number) => {
      const fallbackTimestamp = new Date(fallbackStart + index * 60000);

      if (Array.isArray(point)) {
        const rawTimestamp = Number(point[0]);
        const rawValue = Number(point[1]);

        if (Number.isFinite(rawValue)) {
          points.push({
            timestamp: Number.isFinite(rawTimestamp)
              ? new Date(rawTimestamp * 1000)
              : fallbackTimestamp,
            values: { [groupLabel]: rawValue }
          });
        }
        return points;
      }

      const rawValue = Number(point);

      if (Number.isFinite(rawValue)) {
        points.push({
          timestamp: fallbackTimestamp,
          values: { [groupLabel]: rawValue }
        });
      }

      return points;
    }, []);
  }

  private formatBytes(value?: number): string {
    return this.formatter.format_number(value, 1024, this.binaryUnits, 1);
  }

  private formatRate(value?: number, binary?: boolean): string {
    if (binary) {
      return `${this.formatter.format_number(value, 1024, this.binaryUnits, 1)}/s`;
    }

    return `${Math.round(value * 10) / 10}/s`;
  }
}
