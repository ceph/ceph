import { Component, OnDestroy, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';

import { Subscription } from 'rxjs';

import { RbdMirroringService } from '../../../../shared/api/rbd-mirroring.service';

@Component({
  selector: 'cd-mirroring-pools',
  templateUrl: './pool-list.component.html',
  styleUrls: ['./pool-list.component.scss']
})
export class PoolListComponent implements OnInit, OnDestroy {
  @ViewChild('healthTmpl')
  healthTmpl: TemplateRef<any>;

  subs: Subscription;

  data: [];
  columns: {};

  constructor(private rbdMirroringService: RbdMirroringService, private i18n: I18n) {}

  ngOnInit() {
    this.columns = [
      { prop: 'name', name: this.i18n('Name'), flexGrow: 2 },
      { prop: 'mirror_mode', name: this.i18n('Mode'), flexGrow: 2 },
      { prop: 'leader_id', name: this.i18n('Leader'), flexGrow: 2 },
      { prop: 'image_local_count', name: this.i18n('# Local'), flexGrow: 2 },
      { prop: 'image_remote_count', name: this.i18n('# Remote'), flexGrow: 2 },
      {
        prop: 'health',
        name: this.i18n('Health'),
        cellTemplate: this.healthTmpl,
        flexGrow: 1
      }
    ];

    this.subs = this.rbdMirroringService.subscribe((data: any) => {
      if (!data) {
        return;
      }
      this.data = data.content_data.pools;
    });
  }

  ngOnDestroy(): void {
    this.subs.unsubscribe();
  }

  refresh() {
    this.rbdMirroringService.refresh();
  }
}
