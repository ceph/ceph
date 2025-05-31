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
import { NFSCluster } from '../models/nfs-cluster-config';
import { OrchestratorStatus } from '~/app/shared/models/orchestrator.interface';
import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
import { Icons } from '~/app/shared/enum/icons.enum';
import { SUPPORTED_FSAL } from '../models/nfs.fsal';
import { getFsalFromRoute, getPathfromFsal } from '../utils';
import { Router } from '@angular/router';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { BehaviorSubject, Observable } from 'rxjs';
import { switchMap } from 'rxjs/operators';
const BASE_URL = 'cephfs/nfs';
@Component({
  selector: 'cd-nfs-cluster',
  templateUrl: './nfs-cluster.component.html',
  styleUrls: ['./nfs-cluster.component.scss'],
  providers: [{ provide: URLBuilderService, useValue: new URLBuilderService(BASE_URL) }]
})
export class NfsClusterComponent extends ListWithDetails implements OnInit {
  @ViewChild('hostnameTpl', { static: true })
  hostnameTpl: TemplateRef<any>;

  @ViewChild('ipAddrTpl', { static: true })
  ipAddrTpl: TemplateRef<any>;

  @ViewChild('virtualIpTpl', { static: true })
  virtualIpTpl: TemplateRef<any>;

  @ViewChild('table', { static: true })
  table: TableComponent;

  columns: CdTableColumn[] = [];
  selection: CdTableSelection = new CdTableSelection();
  tableActions: CdTableAction[] = [];
  permission: Permission;
  orchStatus: OrchestratorStatus;
  fsal: SUPPORTED_FSAL;
  viewCacheStatus: any;
  clusters$: Observable<NFSCluster[]>;
  subject = new BehaviorSubject<NFSCluster[]>([]);

  constructor(
    public actionLabels: ActionLabelsI18n,
    protected ngZone: NgZone,
    private authStorageService: AuthStorageService,
    private nfsService: NfsService,
    private orchService: OrchestratorService,
    private router: Router
  ) {
    super();
  }

  ngOnInit(): void {
    this.orchService.status().subscribe((status: OrchestratorStatus) => {
      this.orchStatus = status;
    });
    this.fsal = getFsalFromRoute(this.router.url);
    const prefix = getPathfromFsal(this.fsal);
    const getNfsUri = () => this.selection.first() && `${encodeURI(this.selection.first()?.name)}`;
    this.permission = this.authStorageService.getPermissions().nfs;
    this.clusters$ = this.subject.pipe(switchMap(() => this.nfsService.nfsClusterList()));
    this.columns = [
      {
        name: $localize`Name`,
        prop: 'name',
        flexGrow: 1
      },
      {
        name: $localize`Hostname`,
        prop: 'backend',
        flexGrow: 2,
        cellTemplate: this.hostnameTpl
      },
      {
        name: $localize`IP Address`,
        prop: 'backend',
        flexGrow: 2,
        cellTemplate: this.ipAddrTpl
      },
      {
        name: $localize`Virtual IP Address`,
        prop: 'virtual_ip',
        flexGrow: 1,
        cellTemplate: this.virtualIpTpl
      }
    ];

    const editAction: CdTableAction = {
      permission: 'update',
      icon: Icons.edit,
      routerLink: () => [`/${prefix}/nfs/edit/${getNfsUri()}`],
      name: $localize`Set QOS`,
      canBePrimary: () => true
    };

    this.tableActions = [editAction];
  }

  loadData() {
    this.subject.next([]);
  }
  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }
}
