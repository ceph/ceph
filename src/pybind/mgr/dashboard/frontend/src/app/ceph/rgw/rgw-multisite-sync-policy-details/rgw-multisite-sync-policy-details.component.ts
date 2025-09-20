import { Component, Input, OnChanges, SimpleChanges, TemplateRef, ViewChild } from '@angular/core';
import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Permission } from '~/app/shared/models/permissions';
import { ModalService } from '~/app/shared/services/modal.service';
import { RgwMultisiteService } from '~/app/shared/api/rgw-multisite.service';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { Observable, Subscriber, forkJoin as observableForkJoin } from 'rxjs';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { RgwMultisiteSyncFlowModalComponent } from '../rgw-multisite-sync-flow-modal/rgw-multisite-sync-flow-modal.component';
import { FlowType } from '../models/rgw-multisite';
import { RgwMultisiteSyncPipeModalComponent } from '../rgw-multisite-sync-pipe-modal/rgw-multisite-sync-pipe-modal.component';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';

enum MultisiteResourceType {
  flow = 'flow',
  pipe = 'pipe'
}

@Component({
  selector: 'cd-rgw-multisite-sync-policy-details',
  templateUrl: './rgw-multisite-sync-policy-details.component.html',
  styleUrls: ['./rgw-multisite-sync-policy-details.component.scss']
})
export class RgwMultisiteSyncPolicyDetailsComponent implements OnChanges {
  @Input()
  expandedRow: any;
  @Input()
  permission: Permission;

  @ViewChild(TableComponent)
  table: TableComponent;
  @ViewChild('deleteTpl', { static: true })
  deleteTpl: TemplateRef<any>;

  resourceType: MultisiteResourceType = MultisiteResourceType.flow;
  flowType = FlowType;
  modalRef: NgbModalRef;
  symmetricalFlowData: any = [];
  directionalFlowData: any = [];
  pipeData: any = [];
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
    private actionLabels: ActionLabelsI18n,
    private modalService: ModalService,
    private rgwMultisiteService: RgwMultisiteService,
    private taskWrapper: TaskWrapperService,
    private cdsModalService: ModalCdsService
  ) {
    this.symmetricalFlowCols = [
      {
        name: 'Name',
        prop: 'id',
        flexGrow: 1
      },
      {
        name: 'Zones',
        prop: 'zones',
        flexGrow: 1
      }
    ];
    this.directionalFlowCols = [
      {
        name: 'Source Zone',
        prop: 'source_zone',
        flexGrow: 1
      },
      {
        name: 'Destination Zone',
        prop: 'dest_zone',
        flexGrow: 1
      }
    ];
    this.pipeCols = [
      {
        name: 'Name',
        prop: 'id',
        flexGrow: 1
      },
      {
        name: 'Source Zone',
        prop: 'source.zones',
        flexGrow: 1
      },
      {
        name: 'Destination Zone',
        prop: 'dest.zones',
        flexGrow: 1
      },
      {
        name: 'Source Bucket',
        prop: 'source.bucket',
        flexGrow: 1
      },
      {
        name: 'Destination Bucket',
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

  ngOnChanges(changes: SimpleChanges): void {
    if (changes.expandedRow.currentValue && changes.expandedRow.currentValue.groupName) {
      this.symmetricalFlowData = [];
      this.directionalFlowData = [];
      this.loadData();
    }
  }

  loadData(context?: any) {
    if (this.expandedRow) {
      this.rgwMultisiteService
        .getSyncPolicyGroup(this.expandedRow.groupName, this.expandedRow.bucket)
        .subscribe(
          (policy: any) => {
            this.symmetricalFlowData = policy.data_flow[FlowType.symmetrical] || [];
            this.directionalFlowData = policy.data_flow[FlowType.directional] || [];
            this.pipeData = policy.pipes || [];
          },
          () => {
            if (context) {
              context.error();
            }
          }
        );
    }
  }

  updateSelection(selection: any, type: FlowType) {
    if (type === FlowType.directional) {
      this.dirFlowSelection = selection;
    } else {
      this.symFlowSelection = selection;
    }
  }

  async openModal(flowType: FlowType, edit = false) {
    const action = edit ? 'edit' : 'create';
    const initialState = {
      groupType: flowType,
      groupExpandedRow: this.expandedRow,
      flowSelectedRow:
        flowType === FlowType.symmetrical
          ? this.symFlowSelection.first()
          : this.dirFlowSelection.first(),
      action: action
    };

    this.modalRef = this.modalService.show(RgwMultisiteSyncFlowModalComponent, initialState, {
      size: 'lg'
    });

    try {
      const res = await this.modalRef.result;
      if (res === NotificationType.success) {
        this.loadData();
      }
    } catch (err) {}
  }

  deleteFlow(flowType: FlowType) {
    this.resourceType = MultisiteResourceType.flow;
    let selection = this.symFlowSelection;
    if (flowType === FlowType.directional) {
      selection = this.dirFlowSelection;
    }
    const flowIds = selection.selected.map((flow: any) => flow.id);
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
                    this.expandedRow.groupName,
                    this.expandedRow.bucket
                  );
                })
              )
            })
            .subscribe({
              error: (error: any) => {
                // Forward the error to the observer.
                observer.error(error);
                // Reload the data table content because some deletions might
                // have been executed successfully in the meanwhile.
                this.table.refreshBtn();
              },
              complete: () => {
                // Notify the observer that we are done.
                observer.complete();
                // Reload the data table content.
                this.table.refreshBtn();
              }
            });
        });
      }
    });
  }

  async openPipeModal(edit = false) {
    const action = edit ? 'edit' : 'create';
    const initialState = {
      groupExpandedRow: this.expandedRow,
      pipeSelectedRow: this.pipeSelection.first(),
      action: action
    };

    this.modalRef = this.modalService.show(RgwMultisiteSyncPipeModalComponent, initialState, {
      size: 'lg'
    });

    try {
      const res = await this.modalRef.result;
      if (res === NotificationType.success) {
        this.loadData();
      }
    } catch (err) {}
  }

  deletePipe() {
    this.resourceType = MultisiteResourceType.pipe;
    const pipeIds = this.pipeSelection.selected.map((pipe: any) => pipe.id);
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
                    this.expandedRow.groupName,
                    this.expandedRow.bucket
                  );
                })
              )
            })
            .subscribe({
              error: (error: any) => {
                // Forward the error to the observer.
                observer.error(error);
                // Reload the data table content because some deletions might
                // have been executed successfully in the meanwhile.
                this.table.refreshBtn();
              },
              complete: () => {
                // Notify the observer that we are done.
                observer.complete();
                // Reload the data table content.
                this.table.refreshBtn();
              }
            });
        });
      }
    });
  }
}
