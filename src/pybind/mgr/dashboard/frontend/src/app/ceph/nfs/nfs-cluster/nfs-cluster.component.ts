import { Component, DestroyRef, OnInit, TemplateRef, ViewChild, inject } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { Router } from '@angular/router';
import { Observable, Subject, forkJoin, of } from 'rxjs';
import { catchError, map, startWith, switchMap } from 'rxjs/operators';

import { NfsService } from '~/app/shared/api/nfs.service';
import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { OrchestratorStatus } from '~/app/shared/models/orchestrator.interface';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { URLBuilderService } from '~/app/shared/services/url-builder.service';

import { NFSBackend, NFSCluster, NFSShare } from '../models/nfs.model';

interface NFSClusterRow extends NFSCluster {
  status: string;
  eventsLink: string[];
  shares: number;
  protocol: string;
}

const BASE_URL = 'cephfs/nfs/cluster';

const toEmptyCluster = (name: string): NFSCluster => ({
  name,
  virtual_ip: null,
  backend: [],
  placement: {}
});

@Component({
  selector: 'cd-nfs-cluster',
  templateUrl: './nfs-cluster.component.html',
  styleUrls: ['./nfs-cluster.component.scss'],
  providers: [{ provide: URLBuilderService, useValue: new URLBuilderService(BASE_URL) }],
  standalone: false
})
export class NfsClusterComponent implements OnInit {
  @ViewChild('statusTpl', { static: true })
  statusTpl: TemplateRef<any>;

  columns: CdTableColumn[] = [];
  tableActions: CdTableAction[] = [];
  selection: CdTableSelection = new CdTableSelection();
  permission: Permission;
  orchStatus: OrchestratorStatus;
  clusters$: Observable<NFSClusterRow[]>;
  private readonly refresh$ = new Subject<void>();
  private destroyRef = inject(DestroyRef);
  public actionLabels = inject(ActionLabelsI18n);
  private authStorageService = inject(AuthStorageService);
  private nfsService = inject(NfsService);
  private orchService = inject(OrchestratorService);
  private urlBuilder = inject(URLBuilderService);
  private router = inject(Router);
  private modalService = inject(ModalCdsService);
  private taskWrapper = inject(TaskWrapperService);

  readonly CLUSTER_PATH = 'nfs/cluster';

  constructor() {
    this.permission = this.authStorageService.getPermissions().nfs;
  }

  ngOnInit(): void {
    this.orchService
      .status()
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe((status: OrchestratorStatus) => {
        this.orchStatus = status;
      });
    this.clusters$ = this.refresh$.pipe(
      startWith(undefined),
      switchMap(() => forkJoin({ clusters: this.loadClusters(), exports: this.loadShares() })),
      map(({ clusters, exports }) => this.mapClusterRows(clusters, exports))
    );
    this.columns = [
      {
        name: $localize`Name`,
        prop: 'name',
        flexGrow: 1
      },
      {
        name: $localize`Status`,
        prop: 'status',
        flexGrow: 1,
        cellTemplate: this.statusTpl,
        customTemplateConfig: {
          map: {
            Running: { class: 'tag-success' },
            Degraded: { class: 'tag-warning' },
            Stopped: { class: 'tag-danger' },
            Unknown: { class: 'tag-default' }
          }
        }
      },
      {
        name: $localize`Shares`,
        prop: 'shares',
        flexGrow: 1
      },
      {
        name: $localize`Protocol`,
        prop: 'protocol',
        flexGrow: 1
      }
    ];
    this.tableActions = [
      {
        name: this.actionLabels.CREATE,
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
    const clusterId = cluster?.name;
    this.modalService.show(DeleteConfirmationModalComponent, {
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
  }

  private loadClusters(): Observable<NFSCluster[]> {
    return this.nfsService.listClusters(true).pipe(
      map((clusters) => clusters as NFSCluster[]),
      catchError(() => this.loadClusterNames()),
      catchError(() => of([]))
    );
  }

  private loadClusterNames(): Observable<NFSCluster[]> {
    return this.nfsService
      .listClusters(false)
      .pipe(map((names) => (names as string[]).map(toEmptyCluster)));
  }

  private loadShares(): Observable<NFSShare[]> {
    return this.nfsService.list().pipe(
      map((shares) => (Array.isArray(shares) ? shares : []) as NFSShare[]),
      catchError(() => of([]))
    );
  }

  private mapClusterRows(clusters: NFSCluster[], exports: NFSShare[]): NFSClusterRow[] {
    const shareCounts = (exports || []).reduce((acc: Record<string, number>, exp: NFSShare) => {
      const clusterId = exp?.cluster_id;
      if (clusterId) {
        acc[clusterId] = (acc[clusterId] || 0) + 1;
      }
      return acc;
    }, {});

    return (clusters || []).map((cluster) => ({
      ...cluster,
      status: this.getClusterStatus(cluster.backend),
      eventsLink: ['/services', `nfs.${cluster.name}`, 'events'],
      shares: shareCounts[cluster.name] || 0,
      protocol: cluster.enable_nfsv3 ? $localize`NFSv3, NFSv4` : $localize`NFSv4`
    }));
  }

  private getClusterStatus(backends: NFSBackend[] = []): string {
    if (!backends?.length) {
      return '-';
    }
    const statuses = backends.map((backend) => (backend.status || '').toLowerCase());
    const running = statuses.filter((status) => status === 'running').length;
    if (running === statuses.length) {
      return 'Running';
    }
    if (running > 0) {
      return 'Degraded';
    }
    return '-';
  }

  loadData() {
    this.refresh$.next();
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }
}
