import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { HostService } from '../../../shared/api/host.service';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableFetchDataContext } from '../../../shared/models/cd-table-fetch-data-context';
import { Permissions } from '../../../shared/models/permissions';
import { CephShortVersionPipe } from '../../../shared/pipes/ceph-short-version.pipe';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';

@Component({
  selector: 'cd-hosts',
  templateUrl: './hosts.component.html',
  styleUrls: ['./hosts.component.scss']
})
export class HostsComponent implements OnInit {
  permissions: Permissions;
  columns: Array<CdTableColumn> = [];
  hosts: Array<object> = [];
  isLoadingHosts = false;

  @ViewChild('servicesTpl') public servicesTpl: TemplateRef<any>;

  constructor(
    private authStorageService: AuthStorageService,
    private hostService: HostService,
    private cephShortVersionPipe: CephShortVersionPipe
  ) {
    this.permissions = this.authStorageService.getPermissions();
  }

  ngOnInit() {
    this.columns = [
      {
        name: 'Hostname',
        prop: 'hostname',
        flexGrow: 1
      },
      {
        name: 'Services',
        prop: 'services',
        flexGrow: 3,
        cellTemplate: this.servicesTpl
      },
      {
        name: 'Version',
        prop: 'ceph_version',
        flexGrow: 1,
        pipe: this.cephShortVersionPipe
      }
    ];
  }

  getHosts(context: CdTableFetchDataContext) {
    if (this.isLoadingHosts) {
      return;
    }
    const typeToPermissionKey = {
      mds: 'cephfs',
      mon: 'monitor',
      osd: 'osd',
      rgw: 'rgw',
      'rbd-mirror': 'rbdMirroring',
      mgr: 'manager'
    };
    this.isLoadingHosts = true;
    this.hostService.list().then((resp) => {
      resp.map((host) => {
        host.services.map((service) => {
          service.cdLink = `/perf_counters/${service.type}/${service.id}`;
          const permissionKey = typeToPermissionKey[service.type];
          service.canRead = this.permissions[permissionKey].read;
          return service;
        });
        return host;
      });
      this.hosts = resp;
      this.isLoadingHosts = false;
    }).catch(() => {
      this.isLoadingHosts = false;
      context.error();
    });
  }
}
