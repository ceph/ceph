import { Component, NgZone, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { NfsService } from '~/app/shared/api/nfs.service';
import { ListWithDetails } from '~/app/shared/classes/list-with-details.class';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { URLBuilderService } from '~/app/shared/services/url-builder.service';
import { NFSClusterListConfig, NFSClusterTableModel } from '../models/nfs-cluster-config';
import { OrchestratorStatus } from '~/app/shared/models/orchestrator.interface';
import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
const BASE_URL = 'cephfs/nfs';
@Component({
  selector: 'cd-nfs-cluster',
  templateUrl: './nfs-cluster.component.html',
  styleUrls: ['./nfs-cluster.component.scss'],
  providers: [{ provide: URLBuilderService, useValue: new URLBuilderService(BASE_URL) }]
})
export class NfsClusterComponent extends ListWithDetails implements OnInit {
  @ViewChild('test', { static: true })
  test: TemplateRef<any>;

  @ViewChild('ipPorts', { static: true })
  ipPorts: TemplateRef<any>;

  columns: CdTableColumn[] = [];
  selection: CdTableSelection = new CdTableSelection();
  tableActions: CdTableAction[] = [];
  data1: NFSClusterTableModel[];
  permission: Permission;
  orchStatus: OrchestratorStatus;

  constructor(
    public actionLabels: ActionLabelsI18n,
    protected ngZone: NgZone,
    private authStorageService: AuthStorageService,
    private nfsService: NfsService,
    private orchService: OrchestratorService
  ) {
    super();
    this.nfsService.nfsClusterList().subscribe((list: NFSClusterListConfig) => {
      this.data1 = this.mapDataInTable(list);
    });
  }
  ngOnInit(): void {
    this.orchService.status().subscribe((status: OrchestratorStatus) => {
      this.orchStatus = status;
    });
    this.permission = this.authStorageService.getPermissions().nfs;
    this.columns = [
      {
        name: $localize`Cluster Name`,
        prop: 'name',
        flexGrow: 1
      },
      {
        name: $localize`Hostnames`,
        prop: 'host',
        flexGrow: 2,
        cellTemplate: this.test
      },
      {
        name: $localize`IP Address`,
        prop: 'ip_port',
        flexGrow: 2,
        cellTemplate: this.ipPorts
      },
      {
        name: $localize`Virtual IP Address`,
        prop: 'virtual_ip_Port',
        flexGrow: 1
      }
    ];
  }
  mapDataInTable(list: NFSClusterListConfig): NFSClusterTableModel[] {
    return Object.keys(list).map((name) => {
      const item = list[name];
      const hosts = item.backend
        .filter((data: any) => !!data.hostname)
        .map((data: any) => data.hostname);
      const ipPorts = item.backend
        .filter((data: any) => !!data.ip && !!data.port)
        .map((data: any) => `${data.ip}:${data.port}`);
      const virtualIpPort =
        !!item.virtual_ip && !!item.port ? `${item.virtual_ip}:${item.port}` : '';
      return {
        name,
        host: hosts,
        ip_port: ipPorts,
        virtual_ip_Port: virtualIpPort
      };
    });
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  setExpandedRow(expandedRow: any) {
    super.setExpandedRow(expandedRow);
  }
}
