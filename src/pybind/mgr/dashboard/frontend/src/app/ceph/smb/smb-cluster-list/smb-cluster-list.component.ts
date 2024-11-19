import { Component, OnInit, ViewChild } from '@angular/core';

import _ from 'lodash';
import { Subscription } from 'rxjs';

import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Permission } from '~/app/shared/models/permissions';
import { ListWithDetails } from '~/app/shared/classes/list-with-details.class';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { SmbService } from '~/app/shared/api/smb.service';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { URLBuilderService } from '~/app/shared/services/url-builder.service';


const BASE_URL = 'cephfs/smb';

@Component({
  selector: 'cd-smb-cluster-list',
  templateUrl: './smb-cluster-list.component.html',
  styleUrls: ['./smb-cluster-list.component.scss'],
  providers: [{ provide: URLBuilderService, useValue: new URLBuilderService(BASE_URL) }]
})
export class SmbClusterListComponent extends ListWithDetails implements OnInit {
  @ViewChild('table', { static: true })
  table: TableComponent;

  columns: CdTableColumn[];
  permission: Permission;
  selection = new CdTableSelection();
  summaryDataSubscription: Subscription;
  viewCacheStatus: any;
  smbClusters: any[];
  tableActions: CdTableAction[];

  constructor(
    private authStorageService: AuthStorageService,
    public actionLabels: ActionLabelsI18n,
    private smbService: SmbService,
    private urlBuilder: URLBuilderService,
  ) {
    super();

    this.permission = this.authStorageService.getPermissions().smb;
    this.tableActions = [
      {
        name: this.actionLabels.CREATE + ' Cluster ',
        permission: 'create',
        icon: Icons.add,
        routerLink: () => this.urlBuilder.getCreate(),
        
        canBePrimary: (selection: CdTableSelection) => !selection.hasSingleSelection
      },
      {
        name: this.actionLabels.CREATE + ' Share ',
        permission: 'create',
        icon: Icons.add,
        click: () => this.openModal(),
        canBePrimary: (selection: CdTableSelection) => !selection.hasSelection
      },
      {
        name: this.actionLabels.EDIT,
        permission: 'update',
        icon: Icons.edit,
        click: () => this.openModal()
      },
      {
        name: this.actionLabels.REMOVE,
        permission: 'read',
        icon: Icons.bars,
        click: () => this.openModal()
      }
    ];
  }

  ngOnInit() {
    this.columns = [
      {
        name: $localize`Cluster`,
        prop: 'cluster_id',
        flexGrow: 2
      },
      {
        name: $localize`Auth Mode`,
        prop: 'auth_mode',
        flexGrow: 2
      },
      {
        name: $localize`Intent`,
        prop: 'intent',
        flexGrow: 2
      }
    ];
  }

  openModal() {
    throw new Error('Method not implemented.');
  }

  loadSMBCluster(context: CdTableFetchDataContext) {
    this.smbService.list().subscribe(
      (resp: any[]) => {
        this.smbClusters = resp;
      },
      () => {
        context.error();
      }
    );
  }
}
