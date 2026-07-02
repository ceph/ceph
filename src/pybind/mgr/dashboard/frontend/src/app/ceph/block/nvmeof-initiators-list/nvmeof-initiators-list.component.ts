import { Component, Input, OnDestroy, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { ActivatedRoute, NavigationEnd, Router } from '@angular/router';
import { Subscription } from 'rxjs';
import { filter } from 'rxjs/operators';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { DeletionImpact } from '~/app/shared/enum/delete-confirmation-modal-impact.enum';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { FinishedTask } from '~/app/shared/models/finished-task';
import {
  NvmeofSubsystem,
  NvmeofSubsystemInitiator,
  ALLOW_ALL_HOST,
  NvmeofSubsystemAuthType,
  getSubsystemAuthStatus
} from '~/app/shared/models/nvmeof';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { NvmeofEditHostKeyModalComponent } from '../nvmeof-edit-host-key-modal/nvmeof-edit-host-key-modal.component';

@Component({
  selector: 'cd-nvmeof-initiators-list',
  templateUrl: './nvmeof-initiators-list.component.html',
  styleUrls: ['./nvmeof-initiators-list.component.scss'],
  standalone: false
})
export class NvmeofInitiatorsListComponent implements OnInit, OnDestroy {
  @Input()
  subsystemNQN: string;
  @Input()
  group: string;

  @ViewChild('dhchapTpl', { static: true })
  dhchapTpl: TemplateRef<any>;
  @ViewChild('hostNqnTpl', { static: true })
  hostNqnTpl: TemplateRef<any>;

  initiatorColumns: CdTableColumn[];
  tableActions: CdTableAction[];
  selection = new CdTableSelection();
  permission: Permission;
  initiators: NvmeofSubsystemInitiator[] = [];
  subsystem: NvmeofSubsystem;
  authStatus: string;
  authType = NvmeofSubsystemAuthType;
  allowAllHost = ALLOW_ALL_HOST;
  yesLabel = $localize`Yes`;
  noLabel = $localize`No`;
  allowAllHosts = false;

  private subscriptions = new Subscription();

  constructor(
    public actionLabels: ActionLabelsI18n,
    private authStorageService: AuthStorageService,
    private nvmeofService: NvmeofService,
    private modalService: ModalCdsService,
    private router: Router,
    private taskWrapper: TaskWrapperService,
    private route: ActivatedRoute
  ) {
    this.permission = this.authStorageService.getPermissions().nvmeof;
  }

  ngOnInit() {
    if (!this.subsystemNQN || !this.group) {
      this.route.parent?.params.subscribe((params) => {
        if (params['subsystem_nqn']) {
          this.subsystemNQN = params['subsystem_nqn'];
        }
        this.fetchIfReady();
      });
      this.route.queryParams.subscribe((qp) => {
        if (qp['group']) {
          this.group = qp['group'];
        }
        this.fetchIfReady();
      });
    } else {
      this.listInitiators();
      this.getSubsystem();
    }

    this.subscriptions.add(
      this.router.events
        .pipe(
          filter(
            (event): event is NavigationEnd =>
              event instanceof NavigationEnd && !event.urlAfterRedirects.includes('(modal:')
          )
        )
        .subscribe(() => {
          this.fetchIfReady();
        })
    );

    this.initiatorColumns = [
      {
        name: $localize`Host NQN`,
        prop: 'nqn',
        cellTemplate: this.hostNqnTpl
      },
      {
        name: $localize`DHCHAP key`,
        prop: 'dhchap_key',
        cellTemplate: this.dhchapTpl
      }
    ];
    this.tableActions = [
      {
        name: this.actionLabels.ADD,
        permission: 'create',
        icon: Icons.add,
        click: () => this.openAddInitiatorForm(),
        canBePrimary: (selection: CdTableSelection) => !selection.hasSelection,
        disable: () => this.hasAllHostsAllowed()
      },
      {
        name: $localize`Edit host key`,
        permission: 'update',
        icon: Icons.edit,
        click: () => this.editHostKeyModal(),
        disable: () => this.selection.selected.length !== 1,
        canBePrimary: (selection: CdTableSelection) => selection.selected.length === 1
      },
      {
        name: this.actionLabels.REMOVE,
        permission: 'delete',
        icon: Icons.destroy,
        click: () => this.removeInitiatorModal(),
        disable: () => !this.selection.hasSelection,
        canBePrimary: (selection: CdTableSelection) => selection.hasSelection
      }
    ];
  }

  ngOnDestroy() {
    this.subscriptions.unsubscribe();
  }

  private fetchIfReady() {
    if (this.subsystemNQN && this.group) {
      this.listInitiators();
      this.getSubsystem();
    }
  }

  openAddInitiatorForm(disableAllowAll = false) {
    this.router.navigate([{ outlets: { modal: [URLVerbs.ADD, 'initiator'] } }], {
      queryParams: { group: this.group },
      state: { disableAllowAll },
      relativeTo: this.route.parent
    });
  }

  editHostKeyModal() {
    const selected = this.selection.selected[0];
    if (!selected) return;
    const modalRef = this.modalService.show(NvmeofEditHostKeyModalComponent, {
      subsystemNQN: this.subsystemNQN,
      hostNQN: selected.nqn,
      group: this.group,
      dhchapKey: selected.dhchap_key || ''
    });
    if (modalRef?.closeChange) {
      this.subscriptions.add(
        modalRef.closeChange.subscribe(() => {
          this.listInitiators();
          this.getSubsystem();
        })
      );
    }
  }

  getAllowAllHostIndex() {
    return this.selection.selected.findIndex((selected) => selected.nqn === ALLOW_ALL_HOST);
  }

  hasAllHostsAllowed(): boolean {
    return (
      !!this.subsystem?.allow_any_host &&
      (this.initiators.length === 0 ||
        this.initiators.some((initiator) => initiator.nqn === ALLOW_ALL_HOST))
    );
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  listInitiators() {
    this.nvmeofService
      .getInitiators(this.subsystemNQN, this.group)
      .subscribe((response: NvmeofSubsystemInitiator[] | { hosts: NvmeofSubsystemInitiator[] }) => {
        const initiators = Array.isArray(response) ? response : response?.hosts || [];
        this.initiators = initiators;
        this.updateAuthStatus();
      });
  }

  getSubsystem() {
    this.nvmeofService
      .getSubsystem(this.subsystemNQN, this.group)
      .subscribe((subsystem: NvmeofSubsystem) => {
        this.subsystem = subsystem;
        this.updateAuthStatus();
      });
  }

  updateAuthStatus() {
    this.allowAllHosts = this.hasAllHostsAllowed();
    if (this.subsystem && this.initiators) {
      this.authStatus = getSubsystemAuthStatus(this.subsystem, this.initiators);
    }
  }

  getSelectedNQNs() {
    return this.selection.selected.map((selected) => selected.nqn);
  }

  getDisplayedHostNqn(hostNqn: string): string {
    return hostNqn === ALLOW_ALL_HOST ? $localize`Any` : hostNqn;
  }

  removeInitiatorModal() {
    const hostNQNs = this.getSelectedNQNs();
    const allowAllHostIndex = this.getAllowAllHostIndex();
    const host_nqn = hostNQNs.join(',');
    let itemNames = hostNQNs;
    if (allowAllHostIndex !== -1) {
      hostNQNs.splice(allowAllHostIndex, 1);
      itemNames = [...hostNQNs, $localize`Allow any host(*)`];
    }
    const hostName = itemNames[0];
    const deleteModalRef = this.modalService.show(DeleteConfirmationModalComponent, {
      itemDescription: $localize`host`,
      impact: DeletionImpact.high,
      itemNames,
      actionDescription: 'remove',
      bodyContext: {
        deletionMessage: $localize`Removing <strong>${hostName}</strong> will disconnect it and revoke its permissions for the <strong>${this.subsystemNQN}</strong> subsystem.`
      },
      submitActionObservable: () =>
        this.taskWrapper.wrapTaskAroundCall({
          task: new FinishedTask('nvmeof/initiator/remove', {
            nqn: this.subsystemNQN,
            plural: itemNames.length > 1
          }),
          call: this.nvmeofService.removeInitiators(this.subsystemNQN, {
            host_nqn,
            gw_group: this.group
          })
        })
    });
    if (deleteModalRef?.closeChange) {
      this.subscriptions.add(
        deleteModalRef.closeChange.subscribe(() => {
          this.listInitiators();
          this.getSubsystem();
        })
      );
    }
  }
}
