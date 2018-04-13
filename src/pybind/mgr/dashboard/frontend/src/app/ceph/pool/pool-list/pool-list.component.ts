import { Component } from '@angular/core';

import { PoolService } from '../../../shared/api/pool.service';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';

@Component({
  selector: 'cd-pool-list',
  templateUrl: './pool-list.component.html',
  styleUrls: ['./pool-list.component.scss']
})
export class PoolListComponent {
  pools = [];
  columns: CdTableColumn[];
  selection = new CdTableSelection();

  constructor(
    private poolService: PoolService,
  ) {
    this.columns = [
      {
        prop: 'pool_name',
        name: 'Name',
        flexGrow: 3
      },
      {
        prop: 'type',
        name: 'Type',
        flexGrow: 2
      },
      {
        prop: 'application_metadata',
        name: 'Applications',
        flexGrow: 3
      },
      {
        prop: 'pg_placement_num',
        name: 'Placement Groups',
        flexGrow: 1,
        cellClass: 'text-right'
      },
      {
        prop: 'size',
        name: 'Replica Size',
        flexGrow: 1,
        cellClass: 'text-right'
      },
      {
        prop: 'last_change',
        name: 'Last Change',
        flexGrow: 1,
        cellClass: 'text-right'
      },
      {
        prop: 'erasure_code_profile',
        name: 'Erasure Coded Profile',
        flexGrow: 2
      },
      {
        prop: 'crush_rule',
        name: 'Crush Ruleset',
        flexGrow: 2
      }
    ];
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  getPoolList() {
    this.poolService.getList().subscribe((pools: any[]) => {
      this.pools = pools;
    });
  }

}
