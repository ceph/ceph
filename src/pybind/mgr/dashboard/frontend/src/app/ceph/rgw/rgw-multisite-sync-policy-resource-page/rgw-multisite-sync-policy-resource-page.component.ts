import { TitleCasePipe } from '@angular/common';
import {
  Component,
  OnDestroy,
  OnInit,
  TemplateRef,
  ViewChild,
  ViewEncapsulation
} from '@angular/core';
import { ActivatedRoute, ParamMap } from '@angular/router';
import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import {
  Observable,
  Subscriber,
  Subscription,
  combineLatest,
  forkJoin as observableForkJoin
} from 'rxjs';
import { FlowType } from '~/app/ceph/rgw/models/rgw-multisite';
import { RgwMultisiteSyncFlowModalComponent } from '~/app/ceph/rgw/rgw-multisite-sync-flow-modal/rgw-multisite-sync-flow-modal.component';
import { RgwMultisiteSyncPipeModalComponent } from '~/app/ceph/rgw/rgw-multisite-sync-pipe-modal/rgw-multisite-sync-pipe-modal.component';
import { RgwMultisiteService } from '~/app/shared/api/rgw-multisite.service';
import { OverviewField } from '~/app/shared/components/resource-overview-card/resource-overview-card.component';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { Icons } from '~/app/shared/enum/icons.enum';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';

enum MultisiteResourceType {
  flow = 'flow',
  pipe = 'pipe'
}

@Component({
  selector: 'cd-rgw-multisite-sync-policy-resource-page',
  templateUrl: './rgw-multisite-sync-policy-resource-page.component.html',
  styleUrls: ['./rgw-multisite-sync-policy-resource-page.component.scss'],
  encapsulation: ViewEncapsulation.None,
  standalone: false
})
export class RgwMultisiteSyncPolicyResourcePageComponent implements OnInit, OnDestroy {
  private sub = new Subscription();

  @ViewChild(TableComponent)
  table: TableComponent;
  @ViewChild('deleteTpl', { static: true })
  deleteTpl: TemplateRef<any>;

  section = 'overview';
  flowType = FlowType;
  permission: Permission;
  resourceType: MultisiteResourceType = MultisiteResourceType.flow;
  modalRef: NgbModalRef;

  groupName = '';
  bucketName = '';
  notFound = false;
  hasPolicyGroup = false;
  overviewFields: OverviewField[] = [];

  symmetricalFlowData: any[] = [];
  directionalFlowData: any[] = [];
  pipeData: any[] = [];

  symmetricalFlowCols: CdTableColumn[];
  directionalFlowCols: CdTableColumn[];
  pipeCols: CdTableColumn[];

  symFlowTableActions: CdTableAction[];
  dirFlowTableActions: CdTableAction[];
  pipeTableActions: CdTableAction[];

  symFlowSelection = new CdTableSelection();
  dirFlowSelection = new CdTableSelection();
  pipeSelection = new CdTableSelection();

  constructor(
    private route: ActivatedRoute,
    private actionLabels: ActionLabelsI18n,
    private modalService: ModalCdsService,
    private rgwMultisiteService: RgwMultisiteService,
    private taskWrapper: TaskWrapperService,
    private cdsModalService: ModalCdsService,
    private authStorageService: AuthStorageService,
    private titleCasePipe: TitleCasePipe
  ) {
    this.permission = this.authStorageService.getPermissions().rgw;

    this.symmetricalFlowCols = [
      {
        name: $localize`Name`,
        prop: 'id',
        flexGrow: 1
      },
      {
        name: $localize`Zones`,
        prop: 'zones',
        flexGrow: 1
      }
    ];
    this.directionalFlowCols = [
      {
        name: $localize`Source Zone`,
        prop: 'source_zone',
        flexGrow: 1
      },
      {
        name: $localize`Destination Zone`,
        prop: 'dest_zone',
        flexGrow: 1
      }
    ];
    this.pipeCols = [
      {
        name: $localize`Name`,
        prop: 'id',
        flexGrow: 1
      },
      {
        name: $localize`Source Zone`,
        prop: 'source.zones',
        flexGrow: 1
      },
      {
        name: $localize`Destination Zone`,
        prop: 'dest.zones',
        flexGrow: 1
      },
      {
        name: $localize`Source Bucket`,
        prop: 'source.bucket',
        flexGrow: 1
      },
      {
        name: $localize`Destination Bucket`,
        prop: 'dest.bucket',
        flexGrow: 1
      }
    ];

    const symAddAction: CdTableAction = {
      permission: 'create',
      icon: Icons.add,
      name: this.actionLabels.CREATE,
      click: () => this.openModal(FlowType.symmetrical),
      canBePrimary: (selection: CdTableSelection) => !selection.hasSelection
    };
    const symEditAction: CdTableAction = {
      permission: 'update',
      icon: Icons.edit,
      name: this.actionLabels.EDIT,
      click: () => this.openModal(FlowType.symmetrical, true)
    };
    const symDeleteAction: CdTableAction = {
      permission: 'delete',
      icon: Icons.destroy,
      disable: () => !this.symFlowSelection.hasSelection,
      name: this.actionLabels.DELETE,
      click: () => this.deleteFlow(FlowType.symmetrical),
      canBePrimary: (selection: CdTableSelection) => selection.hasMultiSelection
    };
    this.symFlowTableActions = [symAddAction, symEditAction, symDeleteAction];

    const dirAddAction: CdTableAction = {
      permission: 'create',
      icon: Icons.add,
      name: this.actionLabels.CREATE,
      click: () => this.openModal(FlowType.directional),
      canBePrimary: (selection: CdTableSelection) => !selection.hasSelection
    };
    const dirDeleteAction: CdTableAction = {
      permission: 'delete',
      icon: Icons.destroy,
      // TODO: disabling 'delete' as we are not getting flow_id from backend which is needed for deletion
      disable: () =>
        'Deleting the directional flow is disabled in the UI. Please use CLI to delete the directional flow',
      name: this.actionLabels.DELETE,
      click: () => this.deleteFlow(FlowType.directional),
      canBePrimary: (selection: CdTableSelection) => selection.hasSelection
    };
    this.dirFlowTableActions = [dirAddAction, dirDeleteAction];

    const pipeAddAction: CdTableAction = {
      permission: 'create',
      icon: Icons.add,
      name: this.actionLabels.CREATE,
      click: () => this.openPipeModal(),
      canBePrimary: (selection: CdTableSelection) => !selection.hasSelection
    };
    const pipeEditAction: CdTableAction = {
      permission: 'update',
      icon: Icons.edit,
      name: this.actionLabels.EDIT,
      click: () => this.openPipeModal(true)
    };
    const pipeDeleteAction: CdTableAction = {
      permission: 'delete',
      icon: Icons.destroy,
      disable: () => !this.pipeSelection.hasSelection,
      name: this.actionLabels.DELETE,
      click: () => this.deletePipe(),
      canBePrimary: (selection: CdTableSelection) => selection.hasMultiSelection
    };
    this.pipeTableActions = [pipeAddAction, pipeEditAction, pipeDeleteAction];
  }

  ngOnInit(): void {
    this.sub.add(
      this.route.data.subscribe((data) => {
        this.section = data['section'] ?? 'overview';
      })
    );

    this.sub.add(
      combineLatest([
        this.route.parent?.paramMap ?? this.route.paramMap,
        this.route.parent?.queryParamMap ?? this.route.queryParamMap
      ]).subscribe(([pm, queryPm]: [ParamMap, ParamMap]) => {
        this.groupName = pm.get('groupName') || '';
        this.bucketName = queryPm.get('bucketName') || '';
        this.loadData();
      })
    );
  }

  ngOnDestroy(): void {
    this.sub.unsubscribe();
  }

  private getCurrentGroupContext() {
    return {
      groupName: this.groupName,
      bucket: this.bucketName || undefined
    };
  }

  private buildOverviewFields(policy: any): OverviewField[] {
    const symmetricalCount = this.symmetricalFlowData.length;
    const directionalCount = this.directionalFlowData.length;
    const pipesCount = this.pipeData.length;

    const status = policy?.status;
    const zonegroup = policy?.zonegroup;

    return [
      {
        label: $localize`Group Name`,
        value: this.groupName
      },
      {
        label: $localize`Status`,
        value: status ? this.titleCasePipe.transform(status) : '-'
      },
      {
        label: $localize`Scope`,
        value: this.bucketName ? $localize`Bucket-level` : $localize`Zonegroup-level`
      },
      {
        label: $localize`Associated Zonegroup`,
        value: zonegroup || '-'
      },
      {
        label: $localize`Associated Bucket`,
        value: this.bucketName || '-'
      },
      {
        label: $localize`Total Flows`,
        value: `${symmetricalCount} ${$localize`Symmetrical`}, ${directionalCount} ${$localize`Directional`}`
      },
      {
        label: $localize`Total Pipes`,
        value: pipesCount
      }
    ];
  }

  loadData(context?: CdTableFetchDataContext): void {
    if (!this.groupName) {
      this.hasPolicyGroup = false;
      this.notFound = true;
      this.overviewFields = [];
      this.symmetricalFlowData = [];
      this.directionalFlowData = [];
      this.pipeData = [];
      return;
    }

    this.rgwMultisiteService.getSyncPolicyGroup(this.groupName, this.bucketName).subscribe({
      next: (policy: any) => {
        this.notFound = false;
        this.hasPolicyGroup = true;
        this.symmetricalFlowData = policy?.data_flow?.[FlowType.symmetrical] || [];
        this.directionalFlowData = policy?.data_flow?.[FlowType.directional] || [];
        this.pipeData = policy?.pipes || [];
        this.overviewFields = this.buildOverviewFields(policy || {});
      },
      error: () => {
        this.hasPolicyGroup = false;
        this.notFound = true;
        this.overviewFields = [];
        if (context) {
          context.error();
        }
      }
    });
  }

  updateSelection(selection: CdTableSelection, type: FlowType): void {
    if (type === FlowType.directional) {
      this.dirFlowSelection = selection;
      return;
    }
    this.symFlowSelection = selection;
  }

  async openModal(flowType: FlowType, edit = false): Promise<void> {
    const action = edit ? 'edit' : 'create';
    const initialState = {
      groupType: flowType,
      groupExpandedRow: this.getCurrentGroupContext(),
      flowSelectedRow:
        flowType === FlowType.symmetrical
          ? this.symFlowSelection.first()
          : this.dirFlowSelection.first(),
      action: action
    };

    this.modalRef = this.modalService.show(RgwMultisiteSyncFlowModalComponent, initialState);

    try {
      const res = await this.modalRef.result;
      if (res === NotificationType.success) {
        this.loadData();
      }
    } catch {
      // Modal dismissed.
    }
  }

  deleteFlow(flowType: FlowType): void {
    this.resourceType = MultisiteResourceType.flow;
    let selection = this.symFlowSelection;
    if (flowType === FlowType.directional) {
      selection = this.dirFlowSelection;
    }

    const flowIds = selection.selected.map((flow: any) => flow.id);
    const groupContext = this.getCurrentGroupContext();

    this.cdsModalService.show(DeleteConfirmationModalComponent, {
      itemDescription: selection.hasSingleSelection ? $localize`Flow` : $localize`Flows`,
      itemNames: flowIds,
      bodyTemplate: this.deleteTpl,
      submitActionObservable: () => {
        return new Observable((observer: Subscriber<any>) => {
          this.taskWrapper
            .wrapTaskAroundCall({
              task: new FinishedTask('rgw/multisite/sync-flow/delete', {
                flow_ids: flowIds
              }),
              call: observableForkJoin(
                selection.selected.map((flow: any) => {
                  return this.rgwMultisiteService.removeSyncFlow(
                    flow.id,
                    flowType,
                    groupContext.groupName,
                    groupContext.bucket
                  );
                })
              )
            })
            .subscribe({
              error: (error: any) => {
                observer.error(error);
                this.table?.refreshBtn();
              },
              complete: () => {
                observer.complete();
                this.table?.refreshBtn();
              }
            });
        });
      }
    });
  }

  async openPipeModal(edit = false): Promise<void> {
    const action = edit ? 'edit' : 'create';
    const initialState = {
      groupExpandedRow: this.getCurrentGroupContext(),
      pipeSelectedRow: this.pipeSelection.first(),
      action: action
    };

    this.modalRef = this.modalService.show(RgwMultisiteSyncPipeModalComponent, initialState);

    try {
      const res = await this.modalRef.result;
      if (res === NotificationType.success) {
        this.loadData();
      }
    } catch {
      // Modal dismissed.
    }
  }

  deletePipe(): void {
    this.resourceType = MultisiteResourceType.pipe;
    const pipeIds = this.pipeSelection.selected.map((pipe: any) => pipe.id);
    const groupContext = this.getCurrentGroupContext();

    this.cdsModalService.show(DeleteConfirmationModalComponent, {
      itemDescription: this.pipeSelection.hasSingleSelection ? $localize`Pipe` : $localize`Pipes`,
      itemNames: pipeIds,
      bodyTemplate: this.deleteTpl,
      submitActionObservable: () => {
        return new Observable((observer: Subscriber<any>) => {
          this.taskWrapper
            .wrapTaskAroundCall({
              task: new FinishedTask('rgw/multisite/sync-pipe/delete', {
                pipe_ids: pipeIds
              }),
              call: observableForkJoin(
                this.pipeSelection.selected.map((pipe: any) => {
                  return this.rgwMultisiteService.removeSyncPipe(
                    pipe.id,
                    groupContext.groupName,
                    groupContext.bucket
                  );
                })
              )
            })
            .subscribe({
              error: (error: any) => {
                observer.error(error);
                this.table?.refreshBtn();
              },
              complete: () => {
                observer.complete();
                this.table?.refreshBtn();
              }
            });
        });
      }
    });
  }
}
