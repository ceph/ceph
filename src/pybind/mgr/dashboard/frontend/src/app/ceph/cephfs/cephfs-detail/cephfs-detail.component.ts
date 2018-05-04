import { Component, Input, OnChanges, OnInit, TemplateRef, ViewChild } from '@angular/core';

import * as _ from 'lodash';
import { Subscription } from 'rxjs';

import { CephfsService } from '../../../shared/api/cephfs.service';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { DimlessBinaryPipe } from '../../../shared/pipes/dimless-binary.pipe';
import { DimlessPipe } from '../../../shared/pipes/dimless.pipe';

@Component({
  selector: 'cd-cephfs-detail',
  templateUrl: './cephfs-detail.component.html',
  styleUrls: ['./cephfs-detail.component.scss']
})
export class CephfsDetailComponent implements OnChanges, OnInit {
  @ViewChild('poolUsageTpl') poolUsageTpl: TemplateRef<any>;
  @ViewChild('activityTmpl') activityTmpl: TemplateRef<any>;

  @Input() selection: CdTableSelection;

  selectedItem: any;

  id: number;
  name: string;
  ranks: any;
  pools: any;
  standbys = [];
  clientCount: number;
  mdsCounters = {};

  objectValues = Object.values;
  clientsSelect = false;

  constructor(
    private cephfsService: CephfsService,
    private dimlessBinary: DimlessBinaryPipe,
    private dimless: DimlessPipe
  ) {}

  ngOnChanges() {
    if (this.selection.hasSelection) {
      this.selectedItem = this.selection.first();

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
        { prop: 'rank' },
        { prop: 'state' },
        { prop: 'mds', name: 'Daemon' },
        { prop: 'activity', cellTemplate: this.activityTmpl },
        { prop: 'dns', name: 'Dentries', pipe: this.dimless },
        { prop: 'inos', name: 'Inodes', pipe: this.dimless }
      ],
      data: []
    };

    this.pools = {
      columns: [
        { prop: 'pool' },
        { prop: 'type' },
        { prop: 'size', pipe: this.dimlessBinary },
        {
          name: 'Usage',
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
          key: 'Standby daemons',
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
