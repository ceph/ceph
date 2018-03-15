import { HttpClient } from '@angular/common/http';
import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';

import * as _ from 'lodash';

import { ViewCacheStatus } from '../../../shared/enum/view-cache-status.enum';
import { CephShortVersionPipe } from '../../../shared/pipes/ceph-short-version.pipe';
import { RbdMirroringService } from '../../../shared/services/rbd-mirroring.service';

@Component({
  selector: 'cd-mirroring',
  templateUrl: './mirroring.component.html',
  styleUrls: ['./mirroring.component.scss']
})
export class MirroringComponent implements OnInit {
  @ViewChild('healthTmpl') healthTmpl: TemplateRef<any>;
  @ViewChild('stateTmpl') stateTmpl: TemplateRef<any>;
  @ViewChild('syncTmpl') syncTmpl: TemplateRef<any>;
  @ViewChild('progressTmpl') progressTmpl: TemplateRef<any>;

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
    private http: HttpClient,
    private rbdMirroringService: RbdMirroringService,
    private cephShortVersionPipe: CephShortVersionPipe
  ) { }

  ngOnInit() {
    this.daemons.columns = [
      { prop: 'instance_id', name: 'Instance', flexGrow: 2 },
      { prop: 'id', name: 'ID', flexGrow: 2 },
      { prop: 'server_hostname', name: 'Hostname', flexGrow: 2 },
      {
        prop: 'server_hostname',
        name: 'Version',
        pipe: this.cephShortVersionPipe,
        flexGrow: 2
      },
      {
        prop: 'health',
        name: 'Health',
        cellTemplate: this.healthTmpl,
        flexGrow: 1
      }
    ];

    this.pools.columns = [
      { prop: 'name', name: 'Name', flexGrow: 2 },
      { prop: 'mirror_mode', name: 'Mode', flexGrow: 2 },
      { prop: 'leader_id', name: 'Leader', flexGrow: 2 },
      { prop: 'image_local_count', name: '# Local', flexGrow: 2 },
      { prop: 'image_remote_count', name: '# Remote', flexGrow: 2 },
      {
        prop: 'health',
        name: 'Health',
        cellTemplate: this.healthTmpl,
        flexGrow: 1
      }
    ];

    this.image_error.columns = [
      { prop: 'pool_name', name: 'Pool', flexGrow: 2 },
      { prop: 'name', name: 'Image', flexGrow: 2 },
      { prop: 'description', name: 'Issue', flexGrow: 4 },
      {
        prop: 'state',
        name: 'State',
        cellTemplate: this.stateTmpl,
        flexGrow: 1
      }
    ];

    this.image_syncing.columns = [
      { prop: 'pool_name', name: 'Pool', flexGrow: 2 },
      { prop: 'name', name: 'Image', flexGrow: 2 },
      {
        prop: 'progress',
        name: 'Progress',
        cellTemplate: this.progressTmpl,
        flexGrow: 2
      },
      {
        prop: 'state',
        name: 'State',
        cellTemplate: this.syncTmpl,
        flexGrow: 1
      }
    ];

    this.image_ready.columns = [
      { prop: 'pool_name', name: 'Pool', flexGrow: 2 },
      { prop: 'name', name: 'Image', flexGrow: 2 },
      { prop: 'description', name: 'Description', flexGrow: 4 },
      {
        prop: 'state',
        name: 'State',
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
