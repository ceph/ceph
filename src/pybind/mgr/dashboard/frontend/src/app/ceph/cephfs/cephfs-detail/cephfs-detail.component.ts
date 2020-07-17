import { Component, Input, OnChanges, OnInit, TemplateRef, ViewChild } from '@angular/core';

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

  constructor(private dimlessBinary: DimlessBinaryPipe, private dimless: DimlessPipe) {}

  ngOnChanges() {
    this.setStandbys();
  }

  private setStandbys() {
    this.standbys = [
      {
        key: $localize`Standby daemons`,
        value: this.data.standbys
      }
    ];
  }

  ngOnInit() {
    this.columns = {
      ranks: [
        { prop: 'rank', name: $localize`Rank` },
        { prop: 'state', name: $localize`State` },
        { prop: 'mds', name: $localize`Daemon` },
        { prop: 'activity', name: $localize`Activity`, cellTemplate: this.activityTmpl },
        { prop: 'dns', name: $localize`Dentries`, pipe: this.dimless },
        { prop: 'inos', name: $localize`Inodes`, pipe: this.dimless }
      ],
      pools: [
        { prop: 'pool', name: $localize`Pool` },
        { prop: 'type', name: $localize`Type` },
        { prop: 'size', name: $localize`Size`, pipe: this.dimlessBinary },
        {
          name: $localize`Usage`,
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
