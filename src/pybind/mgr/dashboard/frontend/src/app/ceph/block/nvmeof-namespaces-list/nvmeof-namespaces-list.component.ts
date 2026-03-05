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
import { BehaviorSubject, Observable, of, Subject } from 'rxjs';
import { catchError, map, switchMap, takeUntil } from 'rxjs/operators';

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
    private dimlessBinaryPipe: DimlessBinaryPipe
  ) {
    this.permission = this.authStorageService.getPermissions().nvmeof;
  }

  ngOnInit() {
    this.route.queryParams.pipe(takeUntil(this.destroy$)).subscribe((params) => {
      if (params?.['group']) this.onGroupSelection({ content: params?.['group'] });
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
          return of([]);
        }
        return this.nvmeofService.listNamespaces(this.group).pipe(
          map((res: NvmeofSubsystemNamespace[] | { namespaces: NvmeofSubsystemNamespace[] }) => {
            const namespaces = Array.isArray(res) ? res : res.namespaces || [];
            // Deduplicate by nsid + subsystem NQN (API with wildcard can return duplicates per gateway)
            const seen = new Set<string>();
            return namespaces
              .filter((ns) => {
                const key = `${ns.nsid}_${ns['ns_subsystem_nqn']}`;
                if (seen.has(key)) return false;
                seen.add(key);
                return true;
              })
              .map((ns) => ({
                ...ns,
                unique_id: `${ns.nsid}_${ns['ns_subsystem_nqn']}`
              }));
          }),
          catchError(() => of([]))
        );
      }),
      takeUntil(this.destroy$)
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
  onGroupSelection(selected: GroupsComboboxItem) {
    selected.selected = true;
    this.group = selected.content;
    this.listNamespaces();
  }

  onGroupClear() {
    this.group = null;
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
      if (!this.group) {
        this.onGroupSelection(this.gwGroups[0]);
      } else {
        this.gwGroups = this.gwGroups.map((g) => ({
          ...g,
          selected: g.content === this.group
        }));
      }
      this.gwGroupsEmpty = false;
      this.gwGroupPlaceholder = DEFAULT_PLACEHOLDER;
    } else {
      this.gwGroupsEmpty = true;
      this.gwGroupPlaceholder = $localize`No groups available`;
    }
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
        this.taskWrapper.wrapTaskAroundCall({
          task: new FinishedTask('nvmeof/namespace/delete', {
            nqn: subsystemNqn,
            nsid: namespace.nsid
          }),
          call: this.nvmeofService.deleteNamespace(subsystemNqn, namespace.nsid, this.group)
        })
    });
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
