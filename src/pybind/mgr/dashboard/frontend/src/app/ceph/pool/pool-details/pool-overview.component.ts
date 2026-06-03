import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute, ParamMap } from '@angular/router';

import _ from 'lodash';
import { Subscription } from 'rxjs';

import { PoolService } from '~/app/shared/api/pool.service';
import { DimlessPipe } from '~/app/shared/pipes/dimless.pipe';
import { Pool, PoolType } from '../pool';
import { PoolStat, PoolStats } from '../pool-stat';

@Component({
  selector: 'cd-pool-overview',
  templateUrl: './pool-overview.component.html',
  styleUrls: ['./pool-overview.component.scss'],
  standalone: false
})
export class PoolOverviewComponent implements OnInit, OnDestroy {
  private sub = new Subscription();

  poolName = '';
  pool: Pool | null = null;

  name = '-';
  dataProtection = '-';
  applications: string[] = [];
  pgStatus = '-';
  crushRuleset = '-';

  // Usage bar
  usageTotal = 0;
  usageUsed = 0;

  // Sparkline data (rate series)
  readBytesRates: number[] = [];
  writeBytesRates: number[] = [];

  // Ops (rate values)
  readOpsRate = 0;
  writeOpsRate = 0;

  constructor(
    private route: ActivatedRoute,
    private poolService: PoolService,
    public dimlessPipe: DimlessPipe
  ) {}

  ngOnInit(): void {
    this.sub.add(
      this.route.parent?.paramMap.subscribe((pm: ParamMap) => {
        this.poolName = pm.get('name') ?? '';
        this.loadPool();
      })
    );
  }

  ngOnDestroy(): void {
    this.sub.unsubscribe();
  }

  private loadPool(): void {
    if (!this.poolName) {
      return;
    }

    this.poolService.getList().subscribe((pools: any) => {
      const pool = (pools as any[]).find((p: any) => p.pool_name === this.poolName);
      if (!pool) {
        return;
      }

      this.name = pool.pool_name ?? '-';
      this.crushRuleset = pool.crush_rule ?? '-';
      this.pgStatus = this.transformPgStatus(pool.pg_status);

      // Data protection
      if (pool.type === PoolType.ERASURE) {
        this.dataProtection = pool.erasure_code_profile
          ? `EC: ${pool.erasure_code_profile}`
          : '-';
      } else if (pool.type === PoolType.REPLICATED) {
        this.dataProtection = `replica: ×${pool.size}`;
      } else {
        this.dataProtection = '-';
      }

      // Applications
      const applicationLabels: Record<string, string> = {
        cephfs: $localize`File system`,
        rbd: $localize`Block`,
        rgw: $localize`Object`
      };
      this.applications = (pool.application_metadata || []).map(
        (app: string) => applicationLabels[app] || app
      );

      // Stats — getList() returns stats=true so rates[] are populated
      const emptyStat: PoolStat = { latest: 0, rate: 0, rates: [] };
      const stats: PoolStats = pool.stats || {};
      const bytesUsed = (stats.bytes_used ?? emptyStat).latest;
      const availRaw = (stats.avail_raw ?? emptyStat).latest;

      // Usage bar
      this.usageUsed = bytesUsed;
      this.usageTotal = bytesUsed + availRaw;

      // Sparkline — rates come as [[timestamp, value], ...], extract just the values
      const toRates = (stat: PoolStat): number[] =>
        (stat.rates || []).map((point: any) => (Array.isArray(point) ? point[1] : point));

      this.readBytesRates = toRates(stats.rd_bytes ?? emptyStat);
      this.writeBytesRates = toRates(stats.wr_bytes ?? emptyStat);

      // Ops rates
      this.readOpsRate = (stats.rd ?? emptyStat).rate;
      this.writeOpsRate = (stats.wr ?? emptyStat).rate;
    });
  }

  private transformPgStatus(pgStatus: any): string {
    if (!pgStatus || typeof pgStatus === 'string') {
      return pgStatus || '-';
    }
    const strings: string[] = [];
    _.forEach(pgStatus, (count: number, state: string) => {
      strings.push(`${count} ${state}`);
    });
    return strings.join(', ') || '-';
  }
}
