import { Component, OnDestroy, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { Subscription } from 'rxjs';

import { RbdMirroringService } from '~/app/shared/api/rbd-mirroring.service';
import { TableStatusViewCache } from '~/app/shared/classes/table-status-view-cache';
import { CephShortVersionPipe } from '~/app/shared/pipes/ceph-short-version.pipe';

@Component({
  selector: 'cd-mirroring-daemons',
  templateUrl: './daemon-list.component.html',
  styleUrls: ['./daemon-list.component.scss']
})
export class DaemonListComponent implements OnInit, OnDestroy {
  @ViewChild('healthTmpl', { static: true })
  healthTmpl: TemplateRef<any>;

  subs: Subscription;

  data: [];
  columns: {};

  tableStatus = new TableStatusViewCache();

  constructor(
    private rbdMirroringService: RbdMirroringService,
    private cephShortVersionPipe: CephShortVersionPipe
  ) {}

  ngOnInit() {
    this.columns = [
      { prop: 'instance_id', name: $localize`Instance`, flexGrow: 2 },
      { prop: 'id', name: $localize`ID`, flexGrow: 2 },
      { prop: 'server_hostname', name: $localize`Hostname`, flexGrow: 2 },
      {
        prop: 'version',
        name: $localize`Version`,
        pipe: this.cephShortVersionPipe,
        flexGrow: 2
      },
      {
        prop: 'health',
        name: $localize`Health`,
        cellTemplate: this.healthTmpl,
        flexGrow: 1
      }
    ];

    this.subs = this.rbdMirroringService.subscribeSummary((data) => {
      this.data = data.content_data.daemons;
      this.tableStatus = new TableStatusViewCache(data.status);
    });
  }

  ngOnDestroy(): void {
    this.subs.unsubscribe();
  }

  refresh() {
    this.rbdMirroringService.refresh();
  }
}
