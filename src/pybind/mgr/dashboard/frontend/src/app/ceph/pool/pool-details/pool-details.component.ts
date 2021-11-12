import { ChangeDetectionStrategy, Component, Input, OnChanges } from '@angular/core';

import _ from 'lodash';

import { PoolService } from '~/app/shared/api/pool.service';
import { CdHelperClass } from '~/app/shared/classes/cd-helper.class';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { RbdConfigurationEntry } from '~/app/shared/models/configuration';
import { Permissions } from '~/app/shared/models/permissions';

@Component({
  selector: 'cd-pool-details',
  templateUrl: './pool-details.component.html',
  styleUrls: ['./pool-details.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class PoolDetailsComponent implements OnChanges {
  @Input()
  cacheTiers: any[];
  @Input()
  permissions: Permissions;
  @Input()
  selection: any;

  cacheTierColumns: Array<CdTableColumn> = [];
  // 'stats' won't be shown as the pure stat numbers won't tell the user much,
  // if they are not converted or used in a chart (like the ones available in the pool listing)
  omittedPoolAttributes = ['cdExecuting', 'cdIsBinary', 'stats'];

  poolDetails: object;
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
      this.poolService
        .getConfiguration(this.selection.pool_name)
        .subscribe((poolConf: RbdConfigurationEntry[]) => {
          CdHelperClass.updateChanged(this, { selectedPoolConfiguration: poolConf });
        });
      CdHelperClass.updateChanged(this, {
        poolDetails: _.omit(this.selection, this.omittedPoolAttributes)
      });
    }
  }
}
