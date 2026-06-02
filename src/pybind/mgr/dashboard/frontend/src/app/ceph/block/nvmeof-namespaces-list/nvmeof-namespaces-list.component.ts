import { Component, Input, NgZone, OnDestroy, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { NvmeofService, GroupsComboboxItem } from '~/app/shared/api/nvmeof.service';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { DeletionImpact } from '~/app/shared/enum/delete-confirmation-modal-impact.enum';
import { Icons } from '~/app/shared/enum/icons.enum';

import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { NvmeofSubsystemNamespace } from '~/app/shared/models/nvmeof';
import { Permission } from '~/app/shared/models/permissions';
import { CephServiceSpec } from '~/app/shared/models/service.interface';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';

import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { NvmeofStateService } from '../nvmeof-state.service';
import { BehaviorSubject, Observable, forkJoin, of, Subject } from 'rxjs';
import { catchError, map, shareReplay, switchMap, takeUntil, tap } from 'rxjs/operators';

const DEFAULT_PLACEHOLDER = $localize`Enter group name`;

@Component({
  selector: 'cd-nvmeof-namespaces-list',
  templateUrl: './nvmeof-namespaces-list.component.html',
  styleUrls: ['./nvmeof-namespaces-list.component.scss'],
  standalone: false
})
export class NvmeofNamespacesListComponent implements OnInit, OnDestroy {
  @Input()
  subsystemNQN: string;
  @Input()
  group: string;
  @ViewChild('deleteTpl', { static: true })
  deleteTpl: TemplateRef<any>;
  namespacesColumns: any;
  tableActions: CdTableAction[];
  selection = new CdTableSelection();
  permission: Permission;
  namespaces$: Observable<NvmeofSubsystemNamespace[]>;
  private namespaceSubject = new BehaviorSubject<void>(undefined);
  // Gateway group dropdown properties
  gwGroups: GroupsComboboxItem[] = [];
  groupSelectionCleared = false;
  gwGroupsEmpty: boolean = false;
  gwGroupPlaceholder: string = DEFAULT_PLACEHOLDER;

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
    private nvmeofStateService: NvmeofStateService
  ) {
    this.permission = this.authStorageService.getPermissions().nvmeof;
  }

  ngOnInit() {
    this.route.queryParams.pipe(takeUntil(this.destroy$)).subscribe((params) => {
      const hasGroupParam = Object.prototype.hasOwnProperty.call(params ?? {}, 'group');
      if (params?.['group']) {
        this.groupSelectionCleared = false;
        this.onGroupSelection({ content: params?.['group'] }, false);
      } else if (hasGroupParam) {
        this.groupSelectionCleared = true;
        if (this.group) {
          this.onGroupClear(false);
        }
      } else {
        this.groupSelectionCleared = false;
      }
    });
    this.setGatewayGroups();
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
              group: this.group,
              subsystem_nqn: this.subsystemNQN
            }
          });
        },
        canBePrimary: (selection: CdTableSelection) => !selection.hasSelection,
        disable: () => !this.group
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
                queryParams: { group: this.group },
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
        if (!this.group) {
          if (this.groupSelectionCleared) {
            return of([]);
          }
          return this.fetchAllGroupsNamespaces();
        }
        return this.nvmeofService.listNamespaces(this.group).pipe(
          map((res: any) => this.normalizeAndDedup(res)),
          catchError(() => of([]))
        );
      }),
      shareReplay({ bufferSize: 1, refCount: true }),
      takeUntil(this.destroy$)
    );
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
      switchMap((gatewayGroups: CephServiceSpec[][]) => {
        const firstItem = (gatewayGroups as any)?.[0];
        const groups: CephServiceSpec[] = Array.isArray(firstItem) ? firstItem : [];
        const validGroups = groups.filter((g) => g?.spec?.group);
        if (validGroups.length === 0) return of([]);
        return forkJoin(
          validGroups.map((g) =>
            this.nvmeofService.listNamespaces(g.spec.group).pipe(
              map((res: any) => this.normalizeAndDedup(res)),
              catchError(() => of([]))
            )
          )
        ).pipe(
          map((results) => {
            // Deduplicate again across groups
            const seen = new Set<string>();
            return (results as any[]).flat().filter((ns: any) => {
              if (seen.has(ns.unique_id)) return false;
              seen.add(ns.unique_id);
              return true;
            });
          })
        );
      }),
      catchError(() => of([]))
    );
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  listNamespaces() {
    this.namespaceSubject.next();
  }

  fetchData() {
    this.namespaceSubject.next();
  }

  // Gateway groups methods
  onGroupSelection(selected: GroupsComboboxItem, syncQueryParam = true) {
    selected.selected = true;
    this.group = selected.content;
    this.groupSelectionCleared = false;
    if (syncQueryParam) {
      this.syncGroupQueryParam(this.group);
    }
    this.listNamespaces();
  }

  onGroupClear(syncQueryParam = true) {
    this.group = null;
    this.groupSelectionCleared = true;
    if (syncQueryParam) {
      this.syncGroupQueryParam(null);
    }
    this.listNamespaces();
  }

  setGatewayGroups() {
    this.nvmeofService
      .listGatewayGroups()
      .pipe(takeUntil(this.destroy$))
      .subscribe({
        next: (response: CephServiceSpec[][]) => this.handleGatewayGroupsSuccess(response),
        error: (error) => this.handleGatewayGroupsError(error)
      });
  }

  handleGatewayGroupsSuccess(response: CephServiceSpec[][]) {
    if (response?.[0]?.length) {
      this.gwGroups = this.nvmeofService.formatGwGroupsList(response);
    } else {
      this.gwGroups = [];
    }
    this.updateGroupSelectionState();
  }

  updateGroupSelectionState() {
    if (this.gwGroups.length) {
      if (this.group) {
        this.gwGroups = this.gwGroups.map((g) => ({
          ...g,
          selected: g.content === this.group
        }));
      } else if (!this.groupSelectionCleared) {
        this.onGroupSelection(this.gwGroups[0]);
      } else {
        this.gwGroups = this.gwGroups.map((g) => ({
          ...g,
          selected: false
        }));
      }
      this.gwGroupsEmpty = false;
      this.gwGroupPlaceholder = DEFAULT_PLACEHOLDER;
    } else {
      this.gwGroupsEmpty = true;
      this.gwGroupPlaceholder = $localize`No groups available`;
    }
  }

  private syncGroupQueryParam(group: string | null) {
    this.router.navigate([], {
      relativeTo: this.route,
      queryParams: { group: group ?? '' },
      queryParamsHandling: 'merge'
    });
  }

  handleGatewayGroupsError(error: any) {
    this.gwGroups = [];
    this.gwGroupsEmpty = true;
    this.gwGroupPlaceholder = $localize`Unable to fetch Gateway groups`;
    if (error?.preventDefault) {
      error?.preventDefault?.();
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
            call: this.nvmeofService.deleteNamespace(subsystemNqn, namespace.nsid, this.group)
          })
          .pipe(tap({ complete: () => this.nvmeofStateService.requestRefresh() }))
    });
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
