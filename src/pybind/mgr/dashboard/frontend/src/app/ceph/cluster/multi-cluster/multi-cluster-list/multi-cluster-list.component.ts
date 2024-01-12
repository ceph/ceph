import { Component, ViewChild } from '@angular/core';
import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import { MultiClusterService } from '~/app/shared/api/multi-cluster.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { ModalService } from '~/app/shared/services/modal.service';
import { MultiClusterFormComponent } from '../multi-cluster-form/multi-cluster-form.component';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { Permissions } from '~/app/shared/models/permissions';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';

@Component({
  selector: 'cd-multi-cluster-list',
  templateUrl: './multi-cluster-list.component.html',
  styleUrls: ['./multi-cluster-list.component.scss']
})
export class MultiClusterListComponent {

  @ViewChild(TableComponent)
  table: TableComponent;

  permissions: Permissions;
  tableActions: CdTableAction[];
  clusterTokenStatus: object = {};



  columns: Array<CdTableColumn> = [];
  data: any;
  selection = new CdTableSelection();
  bsModalRef: NgbModalRef;
  clustersTokenMap: Map<string, string> = new Map<string, string>();

  constructor(
    private multiClusterService: MultiClusterService,
    public actionLabels: ActionLabelsI18n,
    private authStorageService: AuthStorageService,
    private modalService: ModalService,
  ) {

    this.tableActions = [
      {
        permission: 'read',
        icon: Icons.add,
        name: this.actionLabels.ADD + ' Cluster',
        click: () => this.openRemoteClusterInfoModal()
      },
      {
        permission: 'read',
        icon: Icons.edit,
        name: this.actionLabels.EDIT + ' Cluster',
        click: () => this.openRemoteClusterInfoModal()
      },
      {
        permission: 'read',
        icon: Icons.destroy,
        name: this.actionLabels.REMOVE + ' Cluster',
        click: () => this.openRemoteClusterInfoModal()
      },
    ];
    this.permissions = this.authStorageService.getPermissions();
   }
  
  ngOnInit(): void {
  
    this.multiClusterService.subscribe((resp: string) => {
      resp['config']?.forEach((config: any) => {
        config['token'] ? this.clustersTokenMap.set(config['name'], config['token']) : '';
      });
      const uniqueObjects = resp['config'].filter((obj: { name: any; }, index: any, self: any[]) => index === self.findIndex((o: { name: any; }) => o.name === obj.name));
      this.data = uniqueObjects;
      this.data.forEach((cluster: any) => {
        cluster['cluster_connection_status'] = 2;
      })
      this.checkClusterConnectionStatus(this.clusterTokenStatus);
    })


    this.columns = [
      {
        prop: 'name',
        name: $localize`Cluster FSID`,
        flexGrow: 2
      },
      {
        prop: 'cluster_connection_status',
        name: $localize`Connection`,
        flexGrow: 2,
        cellTransformation: CellTemplate.badge,
        customTemplateConfig: {
          map: {
            1: { value: 'DISCONNECTED', class: 'badge-danger' },
            0: { value: 'CONNECTED', class: 'badge-success' },
            2: { value: 'CHECKING..', class: 'badge-info' }
          }
        }
      },
      {
        prop: 'helper_text',
        name: $localize`Cluster Alias`,
        flexGrow: 2
      },
      {
        prop: 'url',
        name: $localize`URL`,
        flexGrow: 2
      }
    ];

    this.multiClusterService.subscribeClusterTokenStatus((resp: object) => {
      this.clusterTokenStatus = resp;
      this.checkClusterConnectionStatus(this.clusterTokenStatus);
    })    
  }

  checkClusterConnectionStatus(clusterTokenStatus: any) {

    if (clusterTokenStatus && this.data) {
      this.data.forEach((cluster: any) => {
        if (clusterTokenStatus[cluster.name]) {
          cluster['cluster_connection_status'] = clusterTokenStatus[cluster.name];
        }
        if (cluster.helper_text === 'local-cluster') {
          cluster.cluster_connection_status = 0;
        }
      });
    }
  }

  openRemoteClusterInfoModal() {
    this.bsModalRef = this.modalService.show(MultiClusterFormComponent, {
      size: 'lg'
    });
    this.bsModalRef.componentInstance.submitAction.subscribe(() => {
      setTimeout(() => {
        window.location.reload();
      }, 4000)
    });
  }
}
