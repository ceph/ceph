import { Component, OnInit } from '@angular/core';

import { RgwDaemon } from '~/app/ceph/rgw/models/rgw-daemon';
import { RgwDaemonService } from '~/app/shared/api/rgw-daemon.service';
import { RgwSiteService } from '~/app/shared/api/rgw-site.service';
import { ListWithDetails } from '~/app/shared/classes/list-with-details.class';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { Permission } from '~/app/shared/models/permissions';
import { CephShortVersionPipe } from '~/app/shared/pipes/ceph-short-version.pipe';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';

@Component({
  selector: 'cd-rgw-daemon-list',
  templateUrl: './rgw-daemon-list.component.html',
  styleUrls: ['./rgw-daemon-list.component.scss']
})
export class RgwDaemonListComponent extends ListWithDetails implements OnInit {
  columns: CdTableColumn[] = [];
  daemons: RgwDaemon[] = [];
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
        name: $localize`Port`,
        prop: 'port',
        flexGrow: 1
      },
      {
        name: $localize`Realm`,
        prop: 'realm_name',
        flexGrow: 2
      },
      {
        name: $localize`Zone Group`,
        prop: 'zonegroup_name',
        flexGrow: 2
      },
      {
        name: $localize`Zone`,
        prop: 'zone_name',
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
    this.rgwDaemonService.list().subscribe(this.updateDaemons, () => {
      context.error();
    });
  }

  private updateDaemons = (daemons: RgwDaemon[]) => {
    this.daemons = daemons;
  };
}
