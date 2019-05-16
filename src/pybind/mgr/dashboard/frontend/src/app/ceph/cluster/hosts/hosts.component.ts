import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';

import { HostService } from '../../../shared/api/host.service';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableFetchDataContext } from '../../../shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
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
  cdParams = { fromLink: '/hosts' };
  selection = new CdTableSelection();

  @ViewChild('servicesTpl')
  public servicesTpl: TemplateRef<any>;

  constructor(
    private authStorageService: AuthStorageService,
    private hostService: HostService,
    private cephShortVersionPipe: CephShortVersionPipe,
    private i18n: I18n
  ) {
    this.permissions = this.authStorageService.getPermissions();
  }

  ngOnInit() {
    this.columns = [
      {
        name: this.i18n('Hostname'),
        prop: 'hostname',
        flexGrow: 1
      },
      {
        name: this.i18n('Services'),
        prop: 'services',
        flexGrow: 3,
        cellTemplate: this.servicesTpl
      },
      {
        name: this.i18n('Version'),
        prop: 'ceph_version',
        flexGrow: 1,
        pipe: this.cephShortVersionPipe
      }
    ];
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
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
      mgr: 'manager',
      'tcmu-runner': 'iscsi'
    };
    this.isLoadingHosts = true;
    this.hostService
      .list()
      .then((resp) => {
        resp.map((host) => {
          host.services.map((service) => {
            service.cdLink = `/perf_counters/${service.type}/${encodeURIComponent(service.id)}`;
            const permission = this.permissions[typeToPermissionKey[service.type]];
            service.canRead = permission ? permission.read : false;
            return service;
          });
          return host;
        });
        this.hosts = resp;
        this.isLoadingHosts = false;
      })
      .catch(() => {
        this.isLoadingHosts = false;
        context.error();
      });
  }
}
