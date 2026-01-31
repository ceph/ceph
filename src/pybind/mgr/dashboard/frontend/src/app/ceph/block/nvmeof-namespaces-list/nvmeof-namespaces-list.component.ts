import { Component, Input, OnDestroy, OnInit, TemplateRef, ViewChild } from '@angular/core';
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
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

const BASE_URL = 'block/nvmeof/subsystems';
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
  namespaces: NvmeofSubsystemNamespace[];

  // Gateway group dropdown properties
  gwGroups: GroupsComboboxItem[] = [];
  gwGroupsEmpty: boolean = false;
  gwGroupPlaceholder: string = DEFAULT_PLACEHOLDER;

  private destroy$ = new Subject<void>();

  constructor(
    public actionLabels: ActionLabelsI18n,
    private router: Router,
    private route: ActivatedRoute,
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
        click: () =>
          this.router.navigate(
            [BASE_URL, { outlets: { modal: [URLVerbs.CREATE, this.subsystemNQN, 'namespace'] } }],
            { queryParams: { group: this.group } }
          ),
        canBePrimary: (selection: CdTableSelection) => !selection.hasSelection,
        disable: () => !this.group
      },
      {
        name: this.actionLabels.EDIT,
        permission: 'update',
        icon: Icons.edit,
        click: () =>
          this.router.navigate(
            [
              BASE_URL,
              {
                outlets: {
                  modal: [
                    URLVerbs.EDIT,
                    this.subsystemNQN,
                    'namespace',
                    this.selection.first().nsid
                  ]
                }
              }
            ],
            { queryParams: { group: this.group } }
          )
      },
      {
        name: this.actionLabels.DELETE,
        permission: 'delete',
        icon: Icons.destroy,
        click: () => this.deleteNamespaceModal()
      }
    ];
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  listNamespaces() {
    if (this.group) {
      this.nvmeofService
        .listNamespaces(this.group)
        .subscribe((res: { namespaces: NvmeofSubsystemNamespace[] }) => {
          this.namespaces = res.namespaces || [];
        });
    } else {
      this.namespaces = [];
    }
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
    if (!this.group && this.gwGroups.length) {
      this.onGroupSelection(this.gwGroups[0]);
      this.gwGroupsEmpty = false;
      this.gwGroupPlaceholder = DEFAULT_PLACEHOLDER;
    } else if (!this.gwGroups.length) {
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
