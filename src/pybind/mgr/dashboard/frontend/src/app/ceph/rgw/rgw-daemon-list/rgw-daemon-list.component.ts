import { Component, OnInit } from '@angular/core';

import { RgwDaemonService } from '../../../shared/api/rgw-daemon.service';
import { RgwSiteService } from '../../../shared/api/rgw-site.service';
import { ListWithDetails } from '../../../shared/classes/list-with-details.class';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableFetchDataContext } from '../../../shared/models/cd-table-fetch-data-context';
import { Permission } from '../../../shared/models/permissions';
import { CephShortVersionPipe } from '../../../shared/pipes/ceph-short-version.pipe';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';

@Component({
  selector: 'cd-rgw-daemon-list',
  templateUrl: './rgw-daemon-list.component.html',
  styleUrls: ['./rgw-daemon-list.component.scss']
})
export class RgwDaemonListComponent extends ListWithDetails implements OnInit {
  columns: CdTableColumn[] = [];
  daemons: object[] = [];
  grafanaPermission: Permission;
  isMultiSite: boolean;

  constructor(
    private rgwDaemonService: RgwDaemonService,
    private authStorageService: AuthStorageService,
    private cephShortVersionPipe: CephShortVersionPipe,
    private rgwSiteService: RgwSiteService
  ) {
    super();
  }

  ngOnInit(): void {
    this.grafanaPermission = this.authStorageService.getPermissions().grafana;
    this.columns = [
      {
        name: $localize`ID`,
        prop: 'id',
        flexGrow: 2
      },
      {
        name: $localize`Hostname`,
        prop: 'server_hostname',
        flexGrow: 2
      },
      {
        name: $localize`Version`,
        prop: 'version',
        flexGrow: 1,
        pipe: this.cephShortVersionPipe
      }
    ];
    this.rgwSiteService
      .get('realms')
      .subscribe((realms: string[]) => (this.isMultiSite = realms.length > 0));
  }

  getDaemonList(context: CdTableFetchDataContext) {
    this.rgwDaemonService.list().subscribe(
      (resp: object[]) => {
        this.daemons = resp;
      },
      () => {
        context.error();
      }
    );
  }
}
