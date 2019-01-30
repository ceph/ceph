import { Component, Input, ViewChild } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';
import { TabsetComponent } from 'ngx-bootstrap/tabs';

import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { Permissions } from '../../../shared/models/permissions';

@Component({
  selector: 'cd-pool-details',
  templateUrl: './pool-details.component.html',
  styleUrls: ['./pool-details.component.scss']
})
export class PoolDetailsComponent {
  cacheTierColumns: Array<CdTableColumn> = [];

  @Input()
  selection: CdTableSelection;
  @Input()
  permissions: Permissions;
  @Input()
  cacheTiers: any[];
  @ViewChild(TabsetComponent)
  tabsetChild: TabsetComponent;

  constructor(private i18n: I18n) {
    this.cacheTierColumns = [
      {
        prop: 'pool_name',
        name: this.i18n('Name'),
        flexGrow: 3
      },
      {
        prop: 'cache_mode',
        name: this.i18n('Cache Mode'),
        flexGrow: 2
      },
      {
        prop: 'cache_min_evict_age',
        name: this.i18n('Min Evict Age'),
        flexGrow: 2
      },
      {
        prop: 'cache_min_flush_age',
        name: this.i18n('Min Flush Age'),
        flexGrow: 2
      },
      {
        prop: 'target_max_bytes',
        name: this.i18n('Target Max Bytes'),
        flexGrow: 2
      },
      {
        prop: 'target_max_objects',
        name: this.i18n('Target Max Objects'),
        flexGrow: 2
      }
    ];
  }
}
