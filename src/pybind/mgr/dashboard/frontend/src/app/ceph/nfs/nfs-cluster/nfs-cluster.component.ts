import { Component, NgZone, OnInit, TemplateRef, ViewChild, inject } from '@angular/core';
import { NfsService } from '~/app/shared/api/nfs.service';
import { ListWithDetails } from '~/app/shared/classes/list-with-details.class';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
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
import { BehaviorSubject, Observable } from 'rxjs';
import { Router } from '@angular/router';
import { switchMap } from 'rxjs/operators';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
const BASE_URL = 'cephfs/nfs/cluster';
@Component({
  selector: 'cd-nfs-cluster',
  templateUrl: './nfs-cluster.component.html',
  styleUrls: ['./nfs-cluster.component.scss'],
  providers: [{ provide: URLBuilderService, useValue: new URLBuilderService(BASE_URL) }],
  standalone: false
})
export class NfsClusterComponent extends ListWithDetails implements OnInit {
  @ViewChild('hostnameTpl', { static: true })
  hostnameTpl: TemplateRef<any>;

  @ViewChild('ipAddrTpl', { static: true })
  ipAddrTpl: TemplateRef<any>;

  @ViewChild('virtualIpTpl', { static: true })
  virtualIpTpl: TemplateRef<any>;

  CLUSTER_PATH = 'nfs/cluster';
  columns: CdTableColumn[] = [];
  selection: CdTableSelection = new CdTableSelection();
  tableActions: CdTableAction[] = [];
  permission: Permission;
  orchStatus: OrchestratorStatus;
  clusters$: Observable<NFSCluster[]>;
  subject = new BehaviorSubject<NFSCluster[]>([]);

  public actionLabels = inject(ActionLabelsI18n);
  protected ngZone = inject(NgZone);
  private authStorageService = inject(AuthStorageService);
  private nfsService = inject(NfsService);
  private orchService = inject(OrchestratorService);
  private urlBuilder = inject(URLBuilderService);
  private router = inject(Router);
  private modalService = inject(ModalCdsService);
  private taskWrapper = inject(TaskWrapperService);

  constructor() {
    super();
    this.permission = this.authStorageService.getPermissions().nfs;
  }

  ngOnInit(): void {
    this.orchService.status().subscribe((status: OrchestratorStatus) => {
      this.orchStatus = status;
    });
    this.permission = this.authStorageService.getPermissions().nfs;
    this.clusters$ = this.subject.pipe(switchMap(() => this.nfsService.nfsClusterList()));
    this.loadData();
    this.columns = [
      {
        name: $localize`Name`,
        prop: 'name',
        flexGrow: 1
      },
      {
        name: $localize`Hostnames`,
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

    this.tableActions = [
      {
        name: `${this.actionLabels.CREATE} cluster`,
        permission: 'create',
        icon: Icons.add,
        click: () => this.router.navigateByUrl(this.urlBuilder.getCreate()),
        canBePrimary: (selection: CdTableSelection) => !selection.hasSingleSelection
      },
      {
        name: this.actionLabels.DELETE,
        permission: 'delete',
        icon: Icons.destroy,
        click: () => this.removeNFSClusterModal(),
        canBePrimary: (selection: CdTableSelection) => selection.hasSingleSelection
      }
    ];
  }

  removeNFSClusterModal() {
    const cluster = this.selection.first();
    const clusterId = cluster?.name || cluster?.cluster_id;
    const modalRef = this.modalService.show(DeleteConfirmationModalComponent, {
      itemDescription: $localize`Cluster`,
      itemNames: [clusterId],
      submitActionObservable: () =>
        this.taskWrapper.wrapTaskAroundCall({
          task: new FinishedTask(`${this.CLUSTER_PATH}/${URLVerbs.DELETE}`, {
            cluster_id: clusterId
          }),
          call: this.nfsService.deleteCluster(clusterId)
        })
    });
    modalRef.closed.subscribe(() => {
      this.loadData();
    });
  }

  loadData() {
    this.subject.next([]);
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }
}
