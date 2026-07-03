import { Component, OnDestroy, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { Router } from '@angular/router';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import {
  NvmeofSubsystem,
  NvmeofSubsystemInitiator,
  NvmeofSubsystemAuthType,
  getSubsystemAuthStatus
} from '~/app/shared/models/nvmeof';
import { Permissions } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ListWithDetails } from '~/app/shared/classes/list-with-details.class';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { TableComponent } from '~/app/shared/datatable/table/table.component';

import { Icons } from '~/app/shared/enum/icons.enum';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { NvmeofStateService } from '../nvmeof-state.service';
import { GatewayGroupQueryHandlerService } from '../gateway-group-query-handler.service';
import { BehaviorSubject, forkJoin, Observable, of, Subject } from 'rxjs';
import { catchError, finalize, map, shareReplay, switchMap, takeUntil, tap } from 'rxjs/operators';
import { DeletionImpact } from '~/app/shared/enum/delete-confirmation-modal-impact.enum';
import { CephServiceSpec } from '~/app/shared/models/service.interface';

const BASE_URL = 'block/nvmeof/subsystems';

@Component({
  selector: 'cd-nvmeof-subsystems',
  templateUrl: './nvmeof-subsystems.component.html',
  styleUrls: ['./nvmeof-subsystems.component.scss'],
  standalone: false,
  providers: [GatewayGroupQueryHandlerService]
})
export class NvmeofSubsystemsComponent extends ListWithDetails implements OnInit, OnDestroy {
  @ViewChild('authenticationTpl', { static: true })
  authenticationTpl: TemplateRef<any>;

  @ViewChild('encryptionTpl', { static: true })
  encryptionTpl: TemplateRef<any>;

  @ViewChild('deleteTpl', { static: true })
  deleteTpl: TemplateRef<any>;

  @ViewChild('customTableItemTemplate', { static: true })
  customTableItemTemplate: TemplateRef<any>;

  @ViewChild(TableComponent)
  table: TableComponent;

  subsystems: (NvmeofSubsystem & { gw_group?: string; initiator_count?: number })[] = [];
  pendingNqn: string = null;
  subsystemsColumns: any;
  permissions: Permissions;
  selection = new CdTableSelection();
  tableActions: CdTableAction[];
  subsystemDetails: any[];
  context: CdTableFetchDataContext;
  authType = NvmeofSubsystemAuthType;
  subsystems$: Observable<(NvmeofSubsystem & { gw_group?: string; initiator_count?: number })[]>;

  private subsystemSubject = new BehaviorSubject<void>(undefined);

  private destroy$ = new Subject<void>();

  constructor(
    private nvmeofService: NvmeofService,
    private authStorageService: AuthStorageService,
    public actionLabels: ActionLabelsI18n,
    private router: Router,
    private modalService: ModalCdsService,
    private taskWrapper: TaskWrapperService,
    private nvmeofStateService: NvmeofStateService,
    public groupHandler: GatewayGroupQueryHandlerService
  ) {
    super();
    this.permissions = this.authStorageService.getPermissions();
  }

  ngOnInit() {
    this.groupHandler.init();
    this.groupHandler.dataRefresh$
      .pipe(takeUntil(this.destroy$))
      .subscribe(() => this.getSubsystems());
    this.subsystemsColumns = [
      {
        name: $localize`Subsystem NQN`,
        prop: 'nqn',
        flexGrow: 2,
        cellTemplate: this.customTableItemTemplate
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
      }
    ];

    this.tableActions = [
      {
        name: this.actionLabels.CREATE,
        permission: 'create',
        icon: Icons.add,
        click: () =>
          this.router.navigate([BASE_URL, { outlets: { modal: [URLVerbs.CREATE] } }], {
            queryParams: { group: this.groupHandler.group }
          }),
        canBePrimary: (selection: CdTableSelection) => !selection.hasSelection,
        disable: () => !this.groupHandler.group
      },
      {
        name: this.actionLabels.DELETE,
        permission: 'delete',
        icon: Icons.destroy,
        click: () => this.deleteSubsystemModal()
      }
    ];

    this.subsystems$ = this.subsystemSubject.pipe(
      switchMap(() => {
        if (!this.groupHandler.group) {
          if (this.groupHandler.groupSelectionCleared) {
            return of([]);
          }
          return this.fetchAllGroupsSubsystems();
        }
        return this.nvmeofService.listSubsystems(this.groupHandler.group).pipe(
          switchMap((subsystems: any) => {
            const subs: NvmeofSubsystem[] = Array.isArray(subsystems) ? subsystems : [subsystems];
            if (subs.length === 0) return of([]);
            return forkJoin(
              subs.map((sub) => this.enrichSubsystemForGroup(sub, this.groupHandler.group))
            );
          }),
          catchError((error) => {
            this.handleError(error);
            return of([]);
          })
        );
      }),
      tap((subs) => {
        this.subsystems = subs;
        this.setTableLoading(false);
      }),
      finalize(() => this.setTableLoading(false)),
      shareReplay({ bufferSize: 1, refCount: true }),
      takeUntil(this.destroy$)
    );
    this.nvmeofStateService.refresh$
      .pipe(takeUntil(this.destroy$))
      .subscribe(() => this.fetchData());
  }

  getSubsystems() {
    this.setTableLoading(true);
    this.subsystemSubject.next();
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  fetchData() {
    this.getSubsystems();
  }

  private setTableLoading(loading: boolean): void {
    if (this.table) {
      this.table.loadingIndicator = loading;
    }
  }

  deleteSubsystemModal() {
    const subsystem = this.selection.first();
    this.modalService.show(DeleteConfirmationModalComponent, {
      itemDescription: $localize`Subsystem`,
      impact: DeletionImpact.high,
      bodyTemplate: this.deleteTpl,
      itemNames: [subsystem.nqn],
      actionDescription: 'delete',
      bodyContext: {
        deletionMessage: $localize`Deleting <strong>${subsystem.nqn}</strong> will remove all associated configurations and resources. Dependent services may stop working. This action cannot be undone.`,
        forceDeleteAcknowledgementMessage: $localize`I understand this may remove resources still attached to this subsystem.`
      },
      submitActionObservable: () =>
        this.taskWrapper
          .wrapTaskAroundCall({
            task: new FinishedTask('nvmeof/subsystem/delete', { nqn: subsystem.nqn }),
            call: this.nvmeofService.deleteSubsystem(subsystem.nqn, this.groupHandler.group)
          })
          .pipe(tap({ complete: () => this.nvmeofStateService.requestRefresh() }))
    });
  }

  private handleError(error: any): void {
    if (error?.preventDefault) {
      error?.preventDefault?.();
    }
    this.context?.error?.(error);
  }

  private enrichSubsystemForGroup(sub: NvmeofSubsystem, group: string) {
    return this.nvmeofService.getInitiators(sub.nqn, group).pipe(
      catchError(() => of([])),
      map((initiators: NvmeofSubsystemInitiator[] | { hosts?: NvmeofSubsystemInitiator[] }) => {
        let count = 0;
        if (Array.isArray(initiators)) count = initiators.length;
        else if (initiators?.hosts && Array.isArray(initiators.hosts)) {
          count = initiators.hosts.length;
        }
        return {
          ...sub,
          gw_group: group,
          initiator_count: count,
          auth: getSubsystemAuthStatus(sub, initiators)
        } as NvmeofSubsystem & { initiator_count?: number; auth?: string };
      })
    );
  }

  private fetchAllGroupsSubsystems() {
    return this.nvmeofService.listGatewayGroups().pipe(
      map((gatewayGroups) => this.extractValidGroups(gatewayGroups)),
      switchMap((groups) => this.fetchSubsystemsForGroups(groups)),
      catchError(() => of([]))
    );
  }

  private extractValidGroups(gatewayGroups: CephServiceSpec[][]): CephServiceSpec[] {
    const firstItem = gatewayGroups?.[0];
    const groups = Array.isArray(firstItem) ? firstItem : [];
    return groups.filter((g) => g?.spec?.group);
  }

  private fetchSubsystemsForGroups(groups: CephServiceSpec[]): Observable<NvmeofSubsystem[]> {
    if (groups.length === 0) return of([]);
    return forkJoin(groups.map((g) => this.fetchSubsystemsForGroup(g.spec.group))).pipe(
      map((results) => results.flat())
    );
  }

  private fetchSubsystemsForGroup(groupName: string): Observable<NvmeofSubsystem[]> {
    return this.nvmeofService.listSubsystems(groupName).pipe(
      switchMap((subsystems) => this.enrichSubsystems(subsystems, groupName)),
      catchError(() => of([]))
    );
  }

  private enrichSubsystems(subsystems: any, groupName: string): Observable<NvmeofSubsystem[]> {
    const subs = Array.isArray(subsystems) ? subsystems : [subsystems];
    if (subs.length === 0) return of([]);
    return forkJoin(subs.map((sub) => this.enrichSubsystemForGroup(sub, groupName)));
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
