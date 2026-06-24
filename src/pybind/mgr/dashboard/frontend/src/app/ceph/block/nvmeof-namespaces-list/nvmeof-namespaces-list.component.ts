import { Component, Input, NgZone, OnDestroy, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { DeletionImpact } from '~/app/shared/enum/delete-confirmation-modal-impact.enum';
import { Icons } from '~/app/shared/enum/icons.enum';

import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { NvmeofSubsystemNamespace } from '~/app/shared/models/nvmeof';
import { Permission } from '~/app/shared/models/permissions';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';

import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { NvmeofStateService } from '../nvmeof-state.service';
import { GatewayGroupQueryHandlerService } from '../gateway-group-query-handler.service';
import { BehaviorSubject, Observable, forkJoin, of, Subject } from 'rxjs';
import { catchError, finalize, map, shareReplay, switchMap, takeUntil, tap } from 'rxjs/operators';
import { CephServiceSpec } from '~/app/shared/models/service.interface';

@Component({
  selector: 'cd-nvmeof-namespaces-list',
  templateUrl: './nvmeof-namespaces-list.component.html',
  styleUrls: ['./nvmeof-namespaces-list.component.scss'],
  standalone: false,
  providers: [GatewayGroupQueryHandlerService]
})
export class NvmeofNamespacesListComponent implements OnInit, OnDestroy {
  @Input()
  subsystemNQN: string;
  @ViewChild('deleteTpl', { static: true })
  deleteTpl: TemplateRef<any>;
  @ViewChild(TableComponent)
  table: TableComponent;
  namespacesColumns: any;
  tableActions: CdTableAction[];
  selection = new CdTableSelection();
  permission: Permission;
  namespaces$: Observable<NvmeofSubsystemNamespace[]>;
  private namespaceSubject = new BehaviorSubject<void>(undefined);

  private destroy$ = new Subject<void>();

  constructor(
    public actionLabels: ActionLabelsI18n,
    private router: Router,
    private route: ActivatedRoute,
    private ngZone: NgZone,
    private modalService: ModalCdsService,
    private authStorageService: AuthStorageService,
    private taskWrapper: TaskWrapperService,
    private nvmeofService: NvmeofService,
    private dimlessBinaryPipe: DimlessBinaryPipe,
    private nvmeofStateService: NvmeofStateService,
    public groupHandler: GatewayGroupQueryHandlerService
  ) {
    this.permission = this.authStorageService.getPermissions().nvmeof;
  }

  ngOnInit() {
    this.groupHandler.init();
    this.groupHandler.dataRefresh$
      .pipe(takeUntil(this.destroy$))
      .subscribe(() => this.listNamespaces());
    this.namespacesColumns = [
      {
        name: $localize`Namespace ID`,
        prop: 'nsid'
      },
      {
        name: $localize`Size`,
        prop: 'rbd_image_size',
        pipe: this.dimlessBinaryPipe
      },
      {
        name: $localize`Pool`,
        prop: 'rbd_pool_name'
      },
      {
        name: $localize`Image`,
        prop: 'rbd_image_name'
      },
      {
        name: $localize`Subsystem`,
        prop: 'ns_subsystem_nqn'
      }
    ];
    this.tableActions = [
      {
        name: this.actionLabels.CREATE,
        permission: 'create',
        icon: Icons.add,
        click: () => {
          this.router.navigate(['block/nvmeof/namespaces/create'], {
            queryParams: {
              group: this.groupHandler.group,
              subsystem_nqn: this.subsystemNQN
            }
          });
        },
        canBePrimary: (selection: CdTableSelection) => !selection.hasSelection,
        disable: () => !this.groupHandler.group
      },
      {
        name: $localize`Expand`,
        permission: 'update',
        icon: Icons.edit,
        click: (row: NvmeofSubsystemNamespace) => {
          const namespace = row || this.selection.first();
          this.ngZone.run(() => {
            this.router.navigate(
              [
                {
                  outlets: {
                    modal: [URLVerbs.EDIT, namespace.ns_subsystem_nqn, 'namespace', namespace.nsid]
                  }
                }
              ],
              {
                relativeTo: this.route,
                queryParams: { group: this.groupHandler.group },
                queryParamsHandling: 'merge'
              }
            );
          });
        }
      },
      {
        name: this.actionLabels.DELETE,
        permission: 'delete',
        icon: Icons.destroy,
        click: () => this.deleteNamespaceModal()
      }
    ];

    this.namespaces$ = this.namespaceSubject.pipe(
      switchMap(() => {
        if (!this.groupHandler.group) {
          if (this.groupHandler.groupSelectionCleared) {
            return of([]);
          }
          return this.fetchAllGroupsNamespaces();
        }
        return this.nvmeofService.listNamespaces(this.groupHandler.group).pipe(
          map((res: NvmeofSubsystemNamespace[] | { namespaces: NvmeofSubsystemNamespace[] }) =>
            this.normalizeAndDedup(res)
          ),
          catchError(() => of([]))
        );
      }),
      tap(() => this.setTableLoading(false)),
      finalize(() => this.setTableLoading(false)),
      shareReplay({ bufferSize: 1, refCount: true }),
      takeUntil(this.destroy$)
    );
    this.nvmeofStateService.refresh$
      .pipe(takeUntil(this.destroy$))
      .subscribe(() => this.fetchData());
  }

  private normalizeAndDedup(
    res: NvmeofSubsystemNamespace[] | { namespaces: NvmeofSubsystemNamespace[] }
  ): (NvmeofSubsystemNamespace & { unique_id: string })[] {
    const namespaces = Array.isArray(res) ? res : res.namespaces || [];
    const seen = new Set<string>();
    return namespaces
      .filter((ns) => {
        const key = `${ns.nsid}_${ns['ns_subsystem_nqn']}`;
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      })
      .map((ns) => ({ ...ns, unique_id: `${ns.nsid}_${ns['ns_subsystem_nqn']}` }));
  }

  private fetchAllGroupsNamespaces() {
    return this.nvmeofService.listGatewayGroups().pipe(
      map((gatewayGroups: CephServiceSpec[][]) => this.extractValidGroups(gatewayGroups)),
      switchMap((groups) => this.fetchNamespacesForGroups(groups)),
      catchError(() => of([]))
    );
  }

  private extractValidGroups(gatewayGroups: CephServiceSpec[][]): CephServiceSpec[] {
    const firstItem = gatewayGroups?.[0];
    const groups = Array.isArray(firstItem) ? firstItem : [];
    return groups.filter((g) => g?.spec?.group);
  }

  private fetchNamespacesForGroups(
    groups: CephServiceSpec[]
  ): Observable<(NvmeofSubsystemNamespace & { unique_id: string })[]> {
    if (groups.length === 0) return of([]);
    return forkJoin(groups.map((g) => this.fetchNamespacesForGroup(g.spec.group))).pipe(
      map((results) => this.deduplicateAcrossGroups(results.flat()))
    );
  }

  private fetchNamespacesForGroup(
    groupName: string
  ): Observable<(NvmeofSubsystemNamespace & { unique_id: string })[]> {
    return this.nvmeofService.listNamespaces(groupName).pipe(
      map((res: NvmeofSubsystemNamespace[] | { namespaces: NvmeofSubsystemNamespace[] }) =>
        this.normalizeAndDedup(res)
      ),
      catchError(() => of([]))
    );
  }

  private deduplicateAcrossGroups(
    namespaces: (NvmeofSubsystemNamespace & { unique_id: string })[]
  ): (NvmeofSubsystemNamespace & { unique_id: string })[] {
    const seen = new Set<string>();
    return namespaces.filter((ns) => {
      if (seen.has(ns.unique_id)) return false;
      seen.add(ns.unique_id);
      return true;
    });
  }

  get group(): string | null {
    return this.groupHandler.group;
  }

  set group(value: string | null) {
    this.groupHandler.group = value;
  }

  onGroupChange(group: string | null): void {
    this.groupHandler.onGroupChange(group);
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  listNamespaces() {
    this.setTableLoading(true);
    this.namespaceSubject.next();
  }

  fetchData() {
    this.listNamespaces();
  }

  private setTableLoading(loading: boolean): void {
    if (this.table) {
      this.table.loadingIndicator = loading;
    }
  }

  deleteNamespaceModal() {
    const namespace = this.selection.first();
    const subsystemNqn = namespace.ns_subsystem_nqn;
    this.modalService.show(DeleteConfirmationModalComponent, {
      itemDescription: $localize`Namespace`,
      impact: DeletionImpact.high,
      bodyTemplate: this.deleteTpl,
      itemNames: [namespace.nsid],
      actionDescription: 'delete',
      bodyContext: {
        deletionMessage: $localize`Deleting the namespace <strong>${namespace.nsid}</strong> will permanently remove all resources, services, and configurations within it. This action cannot be undone.`
      },
      submitActionObservable: () =>
        this.taskWrapper
          .wrapTaskAroundCall({
            task: new FinishedTask('nvmeof/namespace/delete', {
              nqn: subsystemNqn,
              nsid: namespace.nsid
            }),
            call: this.nvmeofService.deleteNamespace(
              subsystemNqn,
              namespace.nsid,
              this.groupHandler.group
            )
          })
          .pipe(tap({ complete: () => this.nvmeofStateService.requestRefresh() }))
    });
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
