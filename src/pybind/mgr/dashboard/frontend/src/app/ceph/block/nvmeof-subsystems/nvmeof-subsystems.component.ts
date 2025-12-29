import { Component, OnDestroy, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { NvmeofSubsystem, NvmeofSubsystemInitiator } from '~/app/shared/models/nvmeof';
import { Permissions } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ListWithDetails } from '~/app/shared/classes/list-with-details.class';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { CdTableAction } from '~/app/shared/models/cd-table-action';

import { Icons } from '~/app/shared/enum/icons.enum';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { NvmeofService, GroupsComboboxItem } from '~/app/shared/api/nvmeof.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { CephServiceSpec } from '~/app/shared/models/service.interface';
import { forkJoin, of, Subject } from 'rxjs';
import { catchError, map, switchMap, takeUntil } from 'rxjs/operators';

const BASE_URL = 'block/nvmeof/subsystems';
const DEFAULT_PLACEHOLDER = $localize`Enter group name`;

@Component({
  selector: 'cd-nvmeof-subsystems',
  templateUrl: './nvmeof-subsystems.component.html',
  styleUrls: ['./nvmeof-subsystems.component.scss'],
  standalone: false
})
export class NvmeofSubsystemsComponent extends ListWithDetails implements OnInit, OnDestroy {
  @ViewChild('authenticationTpl', { static: true })
  authenticationTpl: TemplateRef<any>;

  @ViewChild('encryptionTpl', { static: true })
  encryptionTpl: TemplateRef<any>;

  subsystems: (NvmeofSubsystem & { gw_group?: string; initiator_count?: number })[] = [];
  subsystemsColumns: any;
  permissions: Permissions;
  selection = new CdTableSelection();
  tableActions: CdTableAction[];
  subsystemDetails: any[];
  context: CdTableFetchDataContext;
  gwGroups: GroupsComboboxItem[] = [];
  group: string = null;
  gwGroupsEmpty: boolean = false;
  gwGroupPlaceholder: string = DEFAULT_PLACEHOLDER;

  private destroy$ = new Subject<void>();

  constructor(
    private nvmeofService: NvmeofService,
    private authStorageService: AuthStorageService,
    public actionLabels: ActionLabelsI18n,
    private router: Router,
    private modalService: ModalCdsService,
    private taskWrapper: TaskWrapperService,
    private route: ActivatedRoute,
    private notificationService: NotificationService
  ) {
    super();
    this.permissions = this.authStorageService.getPermissions();
  }

  ngOnInit() {
    this.route.queryParams.pipe(takeUntil(this.destroy$)).subscribe((params) => {
      if (params?.['group']) this.onGroupSelection({ content: params?.['group'] });
    });
    this.setGatewayGroups();
    this.subsystemsColumns = [
      {
        name: $localize`Subsystem NQN`,
        prop: 'nqn',
        flexGrow: 2
      },
      {
        name: $localize`Gateway group`,
        prop: 'gw_group'
      },
      {
        name: $localize`Initiators`,
        prop: 'initiator_count'
      },

      {
        name: $localize`Namespaces`,
        prop: 'namespace_count'
      },
      {
        name: $localize`Authentication`,
        prop: 'authentication',
        cellTemplate: this.authenticationTpl
      },
      {
        name: $localize`Traffic encryption`,
        prop: 'encryption',
        cellTemplate: this.encryptionTpl
      }
    ];
    this.tableActions = [
      {
        name: this.actionLabels.CREATE,
        permission: 'create',
        icon: Icons.add,
        click: () =>
          this.router.navigate([BASE_URL, { outlets: { modal: [URLVerbs.CREATE] } }], {
            queryParams: { group: this.group }
          }),
        canBePrimary: (selection: CdTableSelection) => !selection.hasSelection,
        disable: () => !this.group
      },
      {
        name: this.actionLabels.DELETE,
        permission: 'delete',
        icon: Icons.destroy,
        click: () => this.deleteSubsystemModal()
      }
    ];
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  getSubsystems() {
    if (this.group) {
      this.nvmeofService
        .listSubsystems(this.group)
        .pipe(
          switchMap((subsystems: NvmeofSubsystem[] | NvmeofSubsystem) => {
            const subs = Array.isArray(subsystems) ? subsystems : [subsystems];
            if (subs.length === 0) return of([]);

            return forkJoin(subs.map((sub) => this.enrichSubsystemWithInitiators(sub)));
          })
        )
        .pipe(takeUntil(this.destroy$))
        .subscribe({
          next: (subsystems: NvmeofSubsystem[]) => {
            this.subsystems = subsystems;
          },
          error: (error) => {
            this.subsystems = [];
            this.notificationService.show(
              NotificationType.error,
              $localize`Unable to fetch Gateway group`,
              $localize`Gateway group does not exist`
            );
            this.handleError(error);
          }
        });
    } else {
      this.subsystems = [];
    }
  }

  deleteSubsystemModal() {
    const subsystem = this.selection.first();
    this.modalService.show(DeleteConfirmationModalComponent, {
      itemDescription: 'Subsystem',
      itemNames: [subsystem.nqn],
      actionDescription: 'delete',
      submitActionObservable: () =>
        this.taskWrapper.wrapTaskAroundCall({
          task: new FinishedTask('nvmeof/subsystem/delete', { nqn: subsystem.nqn }),
          call: this.nvmeofService.deleteSubsystem(subsystem.nqn, this.group)
        })
    });
  }

  // Gateway groups
  onGroupSelection(selected: GroupsComboboxItem) {
    selected.selected = true;
    this.group = selected.content;
    this.getSubsystems();
  }

  onGroupClear() {
    this.group = null;
    this.getSubsystems();
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
    if (!this.group && this.gwGroups.length) {
      this.onGroupSelection(this.gwGroups[0]);
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
    this.handleError(error);
  }

  private handleError(error: any): void {
    if (error?.preventDefault) {
      error?.preventDefault?.();
    }
    this.context?.error?.(error);
  }

  private enrichSubsystemWithInitiators(sub: NvmeofSubsystem) {
    return this.nvmeofService.getInitiators(sub.nqn, this.group).pipe(
      catchError(() => of([])),
      map((initiators: NvmeofSubsystemInitiator[] | { hosts?: NvmeofSubsystemInitiator[] }) => {
        let count = 0;
        if (Array.isArray(initiators)) count = initiators.length;
        else if (initiators?.hosts && Array.isArray(initiators.hosts)) {
          count = initiators.hosts.length;
        }

        return {
          ...sub,
          gw_group: this.group,
          initiator_count: count
        } as NvmeofSubsystem & { initiator_count?: number };
      })
    );
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
