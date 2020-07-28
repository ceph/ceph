import { Component, Input, OnChanges } from '@angular/core';

import * as _ from 'lodash';

import { PoolService } from '../../../shared/api/pool.service';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { RbdConfigurationEntry } from '../../../shared/models/configuration';
import { Permissions } from '../../../shared/models/permissions';

@Component({
  selector: 'cd-pool-details',
  templateUrl: './pool-details.component.html',
  styleUrls: ['./pool-details.component.scss']
})
export class PoolDetailsComponent implements OnChanges {
  cacheTierColumns: Array<CdTableColumn> = [];

  @Input()
  selection: any;
  @Input()
  permissions: Permissions;
  @Input()
  cacheTiers: any[];
  selectedPoolConfiguration: RbdConfigurationEntry[];

  constructor(private poolService: PoolService) {
    this.cacheTierColumns = [
      {
        prop: 'pool_name',
        name: $localize`Name`,
        flexGrow: 3
      },
      {
        prop: 'cache_mode',
        name: $localize`Cache Mode`,
        flexGrow: 2
      },
      {
        prop: 'cache_min_evict_age',
        name: $localize`Min Evict Age`,
        flexGrow: 2
      },
      {
        prop: 'cache_min_flush_age',
        name: $localize`Min Flush Age`,
        flexGrow: 2
      },
      {
        prop: 'target_max_bytes',
        name: $localize`Target Max Bytes`,
        flexGrow: 2
      },
      {
        prop: 'target_max_objects',
        name: $localize`Target Max Objects`,
        flexGrow: 2
      }
    ];
  }

  ngOnChanges() {
    if (this.selection) {
      this.poolService.getConfiguration(this.selection.pool_name).subscribe((poolConf) => {
        this.selectedPoolConfiguration = poolConf;
      });
    }
  }

  filterNonPoolData(pool: object): object {
    return _.omit(pool, ['cdExecuting', 'cdIsBinary']);
  }
}
