import { Component, OnDestroy, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { ActivatedRoute } from '@angular/router';

import * as _ from 'lodash';
import { Subscription } from 'rxjs/Subscription';

import { DimlessBinaryPipe } from '../../../shared/pipes/dimless-binary.pipe';
import { DimlessPipe } from '../../../shared/pipes/dimless.pipe';
import { CephfsService } from '../cephfs.service';

@Component({
  selector: 'cd-cephfs',
  templateUrl: './cephfs.component.html',
  styleUrls: ['./cephfs.component.scss']
})
export class CephfsComponent implements OnInit, OnDestroy {
  @ViewChild('poolProgressTmpl') poolProgressTmpl: TemplateRef<any>;
  @ViewChild('activityTmpl') activityTmpl: TemplateRef<any>;

  routeParamsSubscribe: Subscription;

  objectValues = Object.values;

  id: number;
  name: string;
  ranks: any;
  pools: any;
  standbys = [];
  clientCount: number;

  mdsCounters = {};

  constructor(
    private route: ActivatedRoute,
    private cephfsService: CephfsService,
    private dimlessBinary: DimlessBinaryPipe,
    private dimless: DimlessPipe
  ) {}

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
        { prop: 'used', pipe: this.dimlessBinary },
        { prop: 'avail', pipe: this.dimlessBinary },
        {
          name: 'Usage',
          cellTemplate: this.poolProgressTmpl,
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

    this.routeParamsSubscribe = this.route.params.subscribe((params: { id: number }) => {
      this.id = params.id;

      this.ranks.data = [];
      this.pools.data = [];
      this.standbys = [];
      this.mdsCounters = {};
    });
  }

  ngOnDestroy() {
    this.routeParamsSubscribe.unsubscribe();
  }

  refresh() {
    this.cephfsService.getCephfs(this.id).subscribe((data: any) => {
      this.ranks.data = data.cephfs.ranks;
      this.pools.data = data.cephfs.pools;
      this.standbys = [
        {
          key: 'Standby daemons',
          value: data.standbys.map(value => value.name).join(', ')
        }
      ];
      this.name = data.cephfs.name;
      this.clientCount = data.cephfs.client_count;
    });

    this.cephfsService.getMdsCounters(this.id).subscribe(data => {
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
