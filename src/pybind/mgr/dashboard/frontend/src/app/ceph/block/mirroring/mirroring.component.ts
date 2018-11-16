import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';

import { RbdMirroringService } from '../../../shared/api/rbd-mirroring.service';
import { ViewCacheStatus } from '../../../shared/enum/view-cache-status.enum';
import { CephShortVersionPipe } from '../../../shared/pipes/ceph-short-version.pipe';

@Component({
  selector: 'cd-mirroring',
  templateUrl: './mirroring.component.html',
  styleUrls: ['./mirroring.component.scss']
})
export class MirroringComponent implements OnInit {
  @ViewChild('healthTmpl')
  healthTmpl: TemplateRef<any>;
  @ViewChild('stateTmpl')
  stateTmpl: TemplateRef<any>;
  @ViewChild('syncTmpl')
  syncTmpl: TemplateRef<any>;
  @ViewChild('progressTmpl')
  progressTmpl: TemplateRef<any>;

  contentData: any;

  status: ViewCacheStatus;
  daemons = {
    data: [],
    columns: []
  };
  pools = {
    data: [],
    columns: {}
  };
  image_error = {
    data: [],
    columns: {}
  };
  image_syncing = {
    data: [],
    columns: {}
  };
  image_ready = {
    data: [],
    columns: {}
  };

  constructor(
    private rbdMirroringService: RbdMirroringService,
    private cephShortVersionPipe: CephShortVersionPipe,
    private i18n: I18n
  ) {}

  ngOnInit() {
    this.daemons.columns = [
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

    this.pools.columns = [
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

    this.image_error.columns = [
      { prop: 'pool_name', name: this.i18n('Pool'), flexGrow: 2 },
      { prop: 'name', name: this.i18n('Image'), flexGrow: 2 },
      { prop: 'description', name: this.i18n('Issue'), flexGrow: 4 },
      {
        prop: 'state',
        name: this.i18n('State'),
        cellTemplate: this.stateTmpl,
        flexGrow: 1
      }
    ];

    this.image_syncing.columns = [
      { prop: 'pool_name', name: this.i18n('Pool'), flexGrow: 2 },
      { prop: 'name', name: this.i18n('Image'), flexGrow: 2 },
      {
        prop: 'progress',
        name: this.i18n('Progress'),
        cellTemplate: this.progressTmpl,
        flexGrow: 2
      },
      {
        prop: 'state',
        name: this.i18n('State'),
        cellTemplate: this.syncTmpl,
        flexGrow: 1
      }
    ];

    this.image_ready.columns = [
      { prop: 'pool_name', name: this.i18n('Pool'), flexGrow: 2 },
      { prop: 'name', name: this.i18n('Image'), flexGrow: 2 },
      { prop: 'description', name: this.i18n('Description'), flexGrow: 4 },
      {
        prop: 'state',
        name: this.i18n('State'),
        cellTemplate: this.stateTmpl,
        flexGrow: 1
      }
    ];
  }

  refresh() {
    this.rbdMirroringService.get().subscribe((data: any) => {
      this.daemons.data = data.content_data.daemons;
      this.pools.data = data.content_data.pools;
      this.image_error.data = data.content_data.image_error;
      this.image_syncing.data = data.content_data.image_syncing;
      this.image_ready.data = data.content_data.image_ready;

      this.status = data.status;
    });
  }
}
