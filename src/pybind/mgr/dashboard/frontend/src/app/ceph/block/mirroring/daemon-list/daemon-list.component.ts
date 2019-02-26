import { Component, OnDestroy, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';
import { Subscription } from 'rxjs';

import { RbdMirroringService } from '../../../../shared/api/rbd-mirroring.service';
import { CephShortVersionPipe } from '../../../../shared/pipes/ceph-short-version.pipe';

@Component({
  selector: 'cd-mirroring-daemons',
  templateUrl: './daemon-list.component.html',
  styleUrls: ['./daemon-list.component.scss']
})
export class DaemonListComponent implements OnInit, OnDestroy {
  @ViewChild('healthTmpl')
  healthTmpl: TemplateRef<any>;

  subs: Subscription;

  data: [];
  columns: {};

  constructor(
    private rbdMirroringService: RbdMirroringService,
    private cephShortVersionPipe: CephShortVersionPipe,
    private i18n: I18n
  ) {}

  ngOnInit() {
    this.columns = [
      { prop: 'instance_id', name: this.i18n('Instance'), flexGrow: 2 },
      { prop: 'id', name: this.i18n('ID'), flexGrow: 2 },
      { prop: 'server_hostname', name: this.i18n('Hostname'), flexGrow: 2 },
      {
        prop: 'version',
        name: this.i18n('Version'),
        pipe: this.cephShortVersionPipe,
        flexGrow: 2
      },
      {
        prop: 'health',
        name: this.i18n('Health'),
        cellTemplate: this.healthTmpl,
        flexGrow: 1
      }
    ];

    this.subs = this.rbdMirroringService.subscribeSummary((data: any) => {
      if (!data) {
        return;
      }
      this.data = data.content_data.daemons;
    });
  }

  ngOnDestroy(): void {
    this.subs.unsubscribe();
  }

  refresh() {
    this.rbdMirroringService.refresh();
  }
}
