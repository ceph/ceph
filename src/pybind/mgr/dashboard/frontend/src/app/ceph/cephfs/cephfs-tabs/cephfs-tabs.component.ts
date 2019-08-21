import { Component, Input, NgZone, OnChanges, OnDestroy } from '@angular/core';

import * as _ from 'lodash';
import { timer } from 'rxjs';

import { CephfsService } from '../../../shared/api/cephfs.service';
import { ViewCacheStatus } from '../../../shared/enum/view-cache-status.enum';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { Permission } from '../../../shared/models/permissions';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';

@Component({
  selector: 'cd-cephfs-tabs',
  templateUrl: './cephfs-tabs.component.html',
  styleUrls: ['./cephfs-tabs.component.scss']
})
export class CephfsTabsComponent implements OnChanges, OnDestroy {
  @Input()
  selection: CdTableSelection;
  selectedItem: any;

  // Grafana tab
  grafanaId: any;
  grafanaPermission: Permission;

  // Client tab
  id: number;
  clients = {
    data: [],
    status: ViewCacheStatus.ValueNone
  };

  // Details tab
  details = {
    standbys: '',
    pools: [],
    ranks: [],
    mdsCounters: {},
    name: ''
  };

  private data: any;
  private reloadSubscriber;

  constructor(
    private ngZone: NgZone,
    private authStorageService: AuthStorageService,
    private cephfsService: CephfsService
  ) {
    this.grafanaPermission = this.authStorageService.getPermissions().grafana;
  }

  ngOnChanges() {
    this.selectedItem = this.selection.first();
    if (!this.selectedItem) {
      this.unsubscribeInterval();
      return;
    }
    if (this.selectedItem.id !== this.id) {
      this.setupSelected(this.selectedItem.id, this.selectedItem.mdsmap.info);
    }
  }

  private setupSelected(id, mdsInfo) {
    this.id = id;
    const firstMds = _.first(Object.values(mdsInfo));
    this.grafanaId = firstMds && firstMds['name'];
    this.details = {
      standbys: '',
      pools: [],
      ranks: [],
      mdsCounters: {},
      name: ''
    };
    this.clients = {
      data: [],
      status: ViewCacheStatus.ValueNone
    };
    this.updateInterval();
  }

  private updateInterval() {
    this.unsubscribeInterval();
    this.subscribeInterval();
  }

  private unsubscribeInterval() {
    if (this.reloadSubscriber) {
      this.reloadSubscriber.unsubscribe();
    }
  }

  private subscribeInterval() {
    this.ngZone.runOutsideAngular(
      () =>
        (this.reloadSubscriber = timer(0, 5000).subscribe(() =>
          this.ngZone.run(() => this.refresh())
        ))
    );
  }

  refresh() {
    this.cephfsService.getTabs(this.id).subscribe(
      (data: any) => {
        this.data = data;
        this.softRefresh();
      },
      () => {
        this.clients.status = ViewCacheStatus.ValueException;
      }
    );
  }

  softRefresh() {
    const data = _.cloneDeep(this.data); // Forces update of tab tables on tab switch
    // Clients tab
    this.clients = data.clients;
    // Details tab
    this.details = {
      standbys: data.standbys,
      pools: data.pools,
      ranks: data.ranks,
      mdsCounters: data.mds_counters,
      name: data.name
    };
  }

  ngOnDestroy() {
    this.unsubscribeInterval();
  }
}
