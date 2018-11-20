import { Component, Input, OnChanges, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';
import * as _ from 'lodash';

import { CephfsService } from '../../../shared/api/cephfs.service';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { Permission } from '../../../shared/models/permissions';
import { DimlessBinaryPipe } from '../../../shared/pipes/dimless-binary.pipe';
import { DimlessPipe } from '../../../shared/pipes/dimless.pipe';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';

@Component({
  selector: 'cd-cephfs-detail',
  templateUrl: './cephfs-detail.component.html',
  styleUrls: ['./cephfs-detail.component.scss']
})
export class CephfsDetailComponent implements OnChanges, OnInit {
  @ViewChild('poolUsageTpl')
  poolUsageTpl: TemplateRef<any>;
  @ViewChild('activityTmpl')
  activityTmpl: TemplateRef<any>;

  @Input()
  selection: CdTableSelection;

  selectedItem: any;

  id: number;
  name: string;
  ranks: any;
  pools: any;
  standbys = [];
  clientCount: number;
  mdsCounters = {};
  grafanaId: any;
  grafanaPermission: Permission;

  objectValues = Object.values;
  clientsSelect = false;

  constructor(
    private authStorageService: AuthStorageService,
    private cephfsService: CephfsService,
    private dimlessBinary: DimlessBinaryPipe,
    private dimless: DimlessPipe,
    private i18n: I18n
  ) {
    this.grafanaPermission = this.authStorageService.getPermissions().grafana;
  }

  ngOnChanges() {
    if (this.selection.hasSelection) {
      this.selectedItem = this.selection.first();
      const mdsInfo: any[] = this.selectedItem.mdsmap.info;
      this.grafanaId = Object.values(mdsInfo)[0].name;

      if (this.id !== this.selectedItem.id) {
        this.id = this.selectedItem.id;
        this.ranks.data = [];
        this.pools.data = [];
        this.standbys = [];
        this.mdsCounters = {};
      }
    }
  }

  ngOnInit() {
    this.ranks = {
      columns: [
        { prop: 'rank', name: this.i18n('Rank') },
        { prop: 'state', name: this.i18n('State') },
        { prop: 'mds', name: this.i18n('Daemon') },
        { prop: 'activity', name: this.i18n('Activity'), cellTemplate: this.activityTmpl },
        { prop: 'dns', name: this.i18n('Dentries'), pipe: this.dimless },
        { prop: 'inos', name: this.i18n('Inodes'), pipe: this.dimless }
      ],
      data: []
    };

    this.pools = {
      columns: [
        { prop: 'pool', name: this.i18n('Pool') },
        { prop: 'type', name: this.i18n('Type') },
        { prop: 'size', name: this.i18n('Size'), pipe: this.dimlessBinary },
        {
          name: this.i18n('Usage'),
          cellTemplate: this.poolUsageTpl,
          comparator: (valueA, valueB, rowA, rowB, sortDirection) => {
            const valA = rowA.used / rowA.avail;
            const valB = rowB.used / rowB.avail;

            if (valA === valB) {
              return 0;
            }

            if (valA > valB) {
              return 1;
            } else {
              return -1;
            }
          }
        }
      ],
      data: []
    };
  }

  refresh() {
    this.cephfsService.getCephfs(this.id).subscribe((data: any) => {
      this.ranks.data = data.cephfs.ranks;
      this.pools.data = data.cephfs.pools;
      this.pools.data.forEach((pool) => {
        pool.size = pool.used + pool.avail;
      });
      this.standbys = [
        {
          key: this.i18n('Standby daemons'),
          value: data.standbys.map((value) => value.name).join(', ')
        }
      ];
      this.name = data.cephfs.name;
      this.clientCount = data.cephfs.client_count;
    });

    this.cephfsService.getMdsCounters(this.id).subscribe((data) => {
      _.each(this.mdsCounters, (value, key) => {
        if (data[key] === undefined) {
          delete this.mdsCounters[key];
        }
      });

      _.each(data, (mdsData: any, mdsName) => {
        mdsData.name = mdsName;
        this.mdsCounters[mdsName] = mdsData;
      });
    });
  }

  trackByFn(index, item) {
    return item.name;
  }
}
