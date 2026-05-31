import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute, ParamMap } from '@angular/router';

import _ from 'lodash';
import { forkJoin, Subscription } from 'rxjs';

import { PoolService } from '~/app/shared/api/pool.service';
import { CdHelperClass } from '~/app/shared/classes/cd-helper.class';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { RbdConfigurationEntry } from '~/app/shared/models/configuration';
import { Pool } from '../pool';

@Component({
  selector: 'cd-pool-details-section',
  templateUrl: './pool-details-section.component.html',
  standalone: false
})
export class PoolDetailsSectionComponent implements OnInit, OnDestroy {
  private sub = new Subscription();
  poolName = '';
  section = '';
  cacheTierColumns: Array<CdTableColumn> = [];
  poolDetails!: object;
  selectedPoolConfiguration: RbdConfigurationEntry[] = [];
  cacheTiers: Pool[] = [];

  omittedPoolAttributes = ['cdExecuting', 'cdIsBinary', 'stats'];

  constructor(private route: ActivatedRoute, private poolService: PoolService) {
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
    this.sub.add(
      this.route.parent?.paramMap.subscribe((pm: ParamMap) => {
        this.poolName = pm.get('name') ?? '';
        this.loadPool();
      })
    );
    this.section = this.route.snapshot.data['section'] ?? '';
  }

  ngOnDestroy(): void {
    this.sub.unsubscribe();
  }

  private loadPool(): void {
    if (!this.poolName) {
      return;
    }

    forkJoin([
      this.poolService.get(this.poolName),
      this.poolService.getConfiguration(this.poolName),
      this.poolService.getList()
    ]).subscribe((result: any[]) => {
      const [pool, poolConf, poolList] = result as [any, RbdConfigurationEntry[], Pool[]];
        const poolData = pool || {};
        this.poolDetails = _.omit(poolData, this.omittedPoolAttributes);
        this.selectedPoolConfiguration = poolConf || [];
        const tierIds = Array.isArray(poolData['tiers']) ? poolData['tiers'] : [];
        this.cacheTiers = (poolList || []).filter((item: Pool) => tierIds.includes(item.pool));
        CdHelperClass.updateChanged(this, {
          poolDetails: this.poolDetails,
          selectedPoolConfiguration: this.selectedPoolConfiguration,
          cacheTiers: this.cacheTiers
        });
    });
  }
}