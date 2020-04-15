import { Component, Input, OnChanges, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';
import * as _ from 'lodash';

import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { DimlessBinaryPipe } from '../../../shared/pipes/dimless-binary.pipe';
import { DimlessPipe } from '../../../shared/pipes/dimless.pipe';

@Component({
  selector: 'cd-cephfs-detail',
  templateUrl: './cephfs-detail.component.html',
  styleUrls: ['./cephfs-detail.component.scss']
})
export class CephfsDetailComponent implements OnChanges, OnInit {
  @ViewChild('poolUsageTpl', { static: true })
  poolUsageTpl: TemplateRef<any>;
  @ViewChild('activityTmpl', { static: true })
  activityTmpl: TemplateRef<any>;

  @Input()
  data: {
    standbys: string;
    pools: any[];
    ranks: any[];
    mdsCounters: object;
    name: string;
  };

  columns: {
    ranks: CdTableColumn[];
    pools: CdTableColumn[];
  };
  standbys: any[] = [];

  objectValues = Object.values;

  constructor(
    private dimlessBinary: DimlessBinaryPipe,
    private dimless: DimlessPipe,
    private i18n: I18n
  ) {}

  ngOnChanges() {
    this.setStandbys();
  }

  private setStandbys() {
    this.standbys = [
      {
        key: this.i18n('Standby daemons'),
        value: this.data.standbys
      }
    ];
  }

  ngOnInit() {
    this.columns = {
      ranks: [
        { prop: 'rank', name: this.i18n('Rank') },
        { prop: 'state', name: this.i18n('State') },
        { prop: 'mds', name: this.i18n('Daemon') },
        { prop: 'activity', name: this.i18n('Activity'), cellTemplate: this.activityTmpl },
        { prop: 'dns', name: this.i18n('Dentries'), pipe: this.dimless },
        { prop: 'inos', name: this.i18n('Inodes'), pipe: this.dimless }
      ],
      pools: [
        { prop: 'pool', name: this.i18n('Pool') },
        { prop: 'type', name: this.i18n('Type') },
        { prop: 'size', name: this.i18n('Size'), pipe: this.dimlessBinary },
        {
          name: this.i18n('Usage'),
          cellTemplate: this.poolUsageTpl,
          comparator: (_valueA: any, _valueB: any, rowA: any, rowB: any) => {
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
        } as CdTableColumn
      ]
    };
  }

  trackByFn(_index: any, item: any) {
    return item.name;
  }
}
