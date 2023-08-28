import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { UntypedFormControl } from '@angular/forms';
import { Router } from '@angular/router';

import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import _ from 'lodash';
import { forkJoin as observableForkJoin, Observable } from 'rxjs';
import { take } from 'rxjs/operators';

import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
import { OsdService } from '~/app/shared/api/osd.service';
import { ListWithDetails } from '~/app/shared/classes/list-with-details.class';
import { ConfirmationModalComponent } from '~/app/shared/components/confirmation-modal/confirmation-modal.component';
import { CriticalConfirmationModalComponent } from '~/app/shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { FormModalComponent } from '~/app/shared/components/form-modal/form-modal.component';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { Icons } from '~/app/shared/enum/icons.enum';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { OrchestratorFeature } from '~/app/shared/models/orchestrator.enum';
import { OrchestratorStatus } from '~/app/shared/models/orchestrator.interface';
import { OsdSettings } from '~/app/shared/models/osd-settings';
import { Permissions } from '~/app/shared/models/permissions';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalService } from '~/app/shared/services/modal.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { URLBuilderService } from '~/app/shared/services/url-builder.service';
import { OsdFlagsIndivModalComponent } from '../osd-flags-indiv-modal/osd-flags-indiv-modal.component';
import { OsdFlagsModalComponent } from '../osd-flags-modal/osd-flags-modal.component';
import { OsdPgScrubModalComponent } from '../osd-pg-scrub-modal/osd-pg-scrub-modal.component';
import { OsdRecvSpeedModalComponent } from '../osd-recv-speed-modal/osd-recv-speed-modal.component';
import { OsdReweightModalComponent } from '../osd-reweight-modal/osd-reweight-modal.component';
import { OsdScrubModalComponent } from '../osd-scrub-modal/osd-scrub-modal.component';

const BASE_URL = 'osd';

@Component({
  selector: 'cd-osd-list',
  templateUrl: './osd-list.component.html',
  styleUrls: ['./osd-list.component.scss'],
  providers: [{ provide: URLBuilderService, useValue: new URLBuilderService(BASE_URL) }]
})
export class OsdListComponent extends ListWithDetails implements OnInit {
  @ViewChild('osdUsageTpl', { static: true })
  osdUsageTpl: TemplateRef<any>;
  @ViewChild('markOsdConfirmationTpl', { static: true })
  markOsdConfirmationTpl: TemplateRef<any>;
  @ViewChild('criticalConfirmationTpl', { static: true })
  criticalConfirmationTpl: TemplateRef<any>;
  @ViewChild('reweightBodyTpl')
  reweightBodyTpl: TemplateRef<any>;
  @ViewChild('safeToDestroyBodyTpl')
  safeToDestroyBodyTpl: TemplateRef<any>;
  @ViewChild('deleteOsdExtraTpl')
  deleteOsdExtraTpl: TemplateRef<any>;
  @ViewChild('flagsTpl', { static: true })
  flagsTpl: TemplateRef<any>;

  permissions: Permissions;
  tableActions: CdTableAction[];
  bsModalRef: NgbModalRef;
  columns: CdTableColumn[];
  clusterWideActions: CdTableAction[];
  icons = Icons;
  osdSettings = new OsdSettings();

  selection = new CdTableSelection();
  osds: any[] = [];
  disabledFlags: string[] = [
    'sortbitwise',
    'purged_snapdirs',
    'recovery_deletes',
    'pglog_hardlimit'
  ];
  indivFlagNames: string[] = ['noup', 'nodown', 'noin', 'noout'];

  orchStatus: OrchestratorStatus;
  actionOrchFeatures = {
    create: [OrchestratorFeature.OSD_CREATE],
    delete: [OrchestratorFeature.OSD_DELETE]
  };

  protected static collectStates(osd: any) {
    const states = [osd['in'] ? 'in' : 'out'];
    if (osd['up']) {
      states.push('up');
    } else if (osd.state.includes('destroyed')) {
      states.push('destroyed');
    } else {
      states.push('down');
    }
    return states;
  }

  constructor(
    private authStorageService: AuthStorageService,
    private osdService: OsdService,
    private dimlessBinaryPipe: DimlessBinaryPipe,
    private modalService: ModalService,
    private urlBuilder: URLBuilderService,
    private router: Router,
    private taskWrapper: TaskWrapperService,
    public actionLabels: ActionLabelsI18n,
    public notificationService: NotificationService,
    private orchService: OrchestratorService
  ) {
    super();
    this.permissions = this.authStorageService.getPermissions();
    this.tableActions = [
      {
        name: this.actionLabels.CREATE,
        permission: 'create',
        icon: Icons.add,
        click: () => this.router.navigate([this.urlBuilder.getCreate()]),
        disable: (selection: CdTableSelection) => this.getDisable('create', selection),
        canBePrimary: (selection: CdTableSelection) => !selection.hasSelection
      },
      {
        name: this.actionLabels.EDIT,
        permission: 'update',
        icon: Icons.edit,
        click: () => this.editAction()
      },
      {
        name: this.actionLabels.FLAGS,
        permission: 'update',
        icon: Icons.flag,
        click: () => this.configureFlagsIndivAction(),
        disable: () => !this.hasOsdSelected
      },
      {
        name: this.actionLabels.SCRUB,
        permission: 'update',
        icon: Icons.analyse,
        click: () => this.scrubAction(false),
        disable: () => !this.hasOsdSelected,
        canBePrimary: (selection: CdTableSelection) => selection.hasSelection
      },
      {
        name: this.actionLabels.DEEP_SCRUB,
        permission: 'update',
        icon: Icons.deepCheck,
        click: () => this.scrubAction(true),
        disable: () => !this.hasOsdSelected
      },
      {
        name: this.actionLabels.REWEIGHT,
        permission: 'update',
        click: () => this.reweight(),
        disable: () => !this.hasOsdSelected || !this.selection.hasSingleSelection,
        icon: Icons.reweight
      },
      {
        name: this.actionLabels.MARK_OUT,
        permission: 'update',
        click: () => this.showConfirmationModal($localize`out`, this.osdService.markOut),
        disable: () => this.isNotSelectedOrInState('out'),
        icon: Icons.left
      },
      {
        name: this.actionLabels.MARK_IN,
        permission: 'update',
        click: () => this.showConfirmationModal($localize`in`, this.osdService.markIn),
        disable: () => this.isNotSelectedOrInState('in'),
        icon: Icons.right
      },
      {
        name: this.actionLabels.MARK_DOWN,
        permission: 'update',
        click: () => this.showConfirmationModal($localize`down`, this.osdService.markDown),
        disable: () => this.isNotSelectedOrInState('down'),
        icon: Icons.down
      },
      {
        name: this.actionLabels.MARK_LOST,
        permission: 'delete',
        click: () =>
          this.showCriticalConfirmationModal(
            $localize`Mark`,
            $localize`OSD lost`,
            $localize`marked lost`,
            (ids: number[]) => {
              return this.osdService.safeToDestroy(JSON.stringify(ids));
            },
            'is_safe_to_destroy',
            this.osdService.markLost
          ),
        disable: () => this.isNotSelectedOrInState('up'),
        icon: Icons.flatten
      },
      {
        name: this.actionLabels.PURGE,
        permission: 'delete',
        click: () =>
          this.showCriticalConfirmationModal(
            $localize`Purge`,
            $localize`OSD`,
            $localize`purged`,
            (ids: number[]) => {
              return this.osdService.safeToDestroy(JSON.stringify(ids));
            },
            'is_safe_to_destroy',
            (id: number) => {
              this.selection = new CdTableSelection();
              return this.osdService.purge(id);
            }
          ),
        disable: () => this.isNotSelectedOrInState('up'),
        icon: Icons.erase
      },
      {
        name: this.actionLabels.DESTROY,
        permission: 'delete',
        click: () =>
          this.showCriticalConfirmationModal(
            $localize`destroy`,
            $localize`OSD`,
            $localize`destroyed`,
            (ids: number[]) => {
              return this.osdService.safeToDestroy(JSON.stringify(ids));
            },
            'is_safe_to_destroy',
            (id: number) => {
              this.selection = new CdTableSelection();
              return this.osdService.destroy(id);
            }
          ),
        disable: () => this.isNotSelectedOrInState('up'),
        icon: Icons.destroyCircle
      },
      {
        name: this.actionLabels.DELETE,
        permission: 'delete',
        click: () => this.delete(),
        disable: (selection: CdTableSelection) => this.getDisable('delete', selection),
        icon: Icons.destroy
      }
    ];
  }

  ngOnInit() {
    this.clusterWideActions = [
      {
        name: $localize`Flags`,
        icon: Icons.flag,
        click: () => this.configureFlagsAction(),
        permission: 'read',
        visible: () => this.permissions.osd.read
      },
      {
        name: $localize`Recovery Priority`,
        icon: Icons.deepCheck,
        click: () => this.configureQosParamsAction(),
        permission: 'read',
        visible: () => this.permissions.configOpt.read
      },
      {
        name: $localize`PG scrub`,
        icon: Icons.analyse,
        click: () => this.configurePgScrubAction(),
        permission: 'read',
        visible: () => this.permissions.configOpt.read
      }
    ];
    this.columns = [
      {
        prop: 'id',
        name: $localize`ID`,
        flexGrow: 1,
        cellTransformation: CellTemplate.executing,
        customTemplateConfig: {
          valueClass: 'bold'
        }
      },
      { prop: 'host.name', name: $localize`Host` },
      {
        prop: 'collectedStates',
        name: $localize`Status`,
        flexGrow: 1,
        cellTransformation: CellTemplate.badge,
        customTemplateConfig: {
          map: {
            in: { class: 'badge-success' },
            up: { class: 'badge-success' },
            down: { class: 'badge-danger' },
            out: { class: 'badge-danger' },
            destroyed: { class: 'badge-danger' }
          }
        }
      },
      {
        prop: 'tree.device_class',
        name: $localize`Device class`,
        flexGrow: 1.2,
        cellTransformation: CellTemplate.badge,
        customTemplateConfig: {
          map: {
            hdd: { class: 'badge-hdd' },
            ssd: { class: 'badge-ssd' }
          }
        }
      },
      {
        prop: 'stats.numpg',
        name: $localize`PGs`,
        flexGrow: 1
      },
      {
        prop: 'stats.stat_bytes',
        name: $localize`Size`,
        flexGrow: 1,
        pipe: this.dimlessBinaryPipe
      },
      {
        prop: 'state',
        name: $localize`Flags`,
        cellTemplate: this.flagsTpl
      },
      { prop: 'stats.usage', name: $localize`Usage`, cellTemplate: this.osdUsageTpl },
      {
        prop: 'stats_history.out_bytes',
        name: $localize`Read bytes`,
        cellTransformation: CellTemplate.sparkline
      },
      {
        prop: 'stats_history.in_bytes',
        name: $localize`Write bytes`,
        cellTransformation: CellTemplate.sparkline
      },
      {
        prop: 'stats.op_r',
        name: $localize`Read ops`,
        cellTransformation: CellTemplate.perSecond
      },
      {
        prop: 'stats.op_w',
        name: $localize`Write ops`,
        cellTransformation: CellTemplate.perSecond
      }
    ];

    this.orchService.status().subscribe((status: OrchestratorStatus) => (this.orchStatus = status));

    this.osdService
      .getOsdSettings()
      .pipe(take(1))
      .subscribe((data: any) => {
        this.osdSettings = data;
      });
  }

  getDisable(action: 'create' | 'delete', selection: CdTableSelection): boolean | string {
    if (action === 'delete') {
      if (!selection.hasSelection) {
        return true;
      } else {
        // Disable delete action if any selected OSDs are under deleting or unmanaged.
        const deletingOSDs = _.some(this.getSelectedOsds(), (osd) => {
          const status = _.get(osd, 'operational_status');
          return status === 'deleting' || status === 'unmanaged';
        });
        if (deletingOSDs) {
          return true;
        }
      }
    }
    return this.orchService.getTableActionDisableDesc(
      this.orchStatus,
      this.actionOrchFeatures[action]
    );
  }

  /**
   * Only returns valid IDs, e.g. if an OSD is falsely still selected after being deleted, it won't
   * get returned.
   */
  getSelectedOsdIds(): number[] {
    const osdIds = this.osds.map((osd) => osd.id);
    return this.selection.selected
      .map((row) => row.id)
      .filter((id) => osdIds.includes(id))
      .sort();
  }

  getSelectedOsds(): any[] {
    return this.osds.filter(
      (osd) => !_.isUndefined(osd) && this.getSelectedOsdIds().includes(osd.id)
    );
  }

  get hasOsdSelected(): boolean {
    return this.getSelectedOsdIds().length > 0;
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  /**
   * Returns true if no rows are selected or if *any* of the selected rows are in the given
   * state. Useful for deactivating the corresponding menu entry.
   */
  isNotSelectedOrInState(state: 'in' | 'up' | 'down' | 'out'): boolean {
    const selectedOsds = this.getSelectedOsds();
    if (selectedOsds.length === 0) {
      return true;
    }
    switch (state) {
      case 'in':
        return selectedOsds.some((osd) => osd.in === 1);
      case 'out':
        return selectedOsds.some((osd) => osd.in !== 1);
      case 'down':
        return selectedOsds.some((osd) => osd.up !== 1);
      case 'up':
        return selectedOsds.some((osd) => osd.up === 1);
    }
  }

  getOsdList() {
    const observables = [this.osdService.getList(), this.osdService.getFlags()];
    observableForkJoin(observables).subscribe((resp: [any[], string[]]) => {
      this.osds = resp[0].map((osd) => {
        osd.collectedStates = OsdListComponent.collectStates(osd);
        osd.stats_history.out_bytes = osd.stats_history.op_out_bytes.map((i: string) => i[1]);
        osd.stats_history.in_bytes = osd.stats_history.op_in_bytes.map((i: string) => i[1]);
        osd.stats.usage = osd.stats.stat_bytes_used / osd.stats.stat_bytes;
        osd.cdIsBinary = true;
        osd.cdIndivFlags = osd.state.filter((f: string) => this.indivFlagNames.includes(f));
        osd.cdClusterFlags = resp[1].filter((f: string) => !this.disabledFlags.includes(f));
        const deploy_state = _.get(osd, 'operational_status', 'unmanaged');
        if (deploy_state !== 'unmanaged' && deploy_state !== 'working') {
          osd.cdExecuting = deploy_state;
        }
        return osd;
      });
    });
  }

  editAction() {
    const selectedOsd = _.filter(this.osds, ['id', this.selection.first().id]).pop();

    this.modalService.show(FormModalComponent, {
      titleText: $localize`Edit OSD: ${selectedOsd.id}`,
      fields: [
        {
          type: 'text',
          name: 'deviceClass',
          value: selectedOsd.tree.device_class,
          label: $localize`Device class`,
          required: true
        }
      ],
      submitButtonText: $localize`Edit OSD`,
      onSubmit: (values: any) => {
        this.osdService.update(selectedOsd.id, values.deviceClass).subscribe(() => {
          this.notificationService.show(
            NotificationType.success,
            $localize`Updated OSD '${selectedOsd.id}'`
          );
          this.getOsdList();
        });
      }
    });
  }

  scrubAction(deep: boolean) {
    if (!this.hasOsdSelected) {
      return;
    }

    const initialState = {
      selected: this.getSelectedOsdIds(),
      deep: deep
    };

    this.bsModalRef = this.modalService.show(OsdScrubModalComponent, initialState);
  }

  configureFlagsAction() {
    this.bsModalRef = this.modalService.show(OsdFlagsModalComponent);
  }

  configureFlagsIndivAction() {
    const initialState = {
      selected: this.getSelectedOsds()
    };
    this.bsModalRef = this.modalService.show(OsdFlagsIndivModalComponent, initialState);
  }

  showConfirmationModal(markAction: string, onSubmit: (id: number) => Observable<any>) {
    const osdIds = this.getSelectedOsdIds();
    this.bsModalRef = this.modalService.show(ConfirmationModalComponent, {
      titleText: $localize`Mark OSD ${markAction}`,
      buttonText: $localize`Mark ${markAction}`,
      bodyTpl: this.markOsdConfirmationTpl,
      bodyContext: {
        markActionDescription: markAction,
        osdIds
      },
      onSubmit: () => {
        observableForkJoin(
          this.getSelectedOsdIds().map((osd: any) => onSubmit.call(this.osdService, osd))
        ).subscribe(() => this.bsModalRef.close());
      }
    });
  }

  reweight() {
    const selectedOsd = this.osds.filter((o) => o.id === this.selection.first().id).pop();
    this.bsModalRef = this.modalService.show(OsdReweightModalComponent, {
      currentWeight: selectedOsd.weight,
      osdId: selectedOsd.id
    });
  }

  delete() {
    const deleteFormGroup = new CdFormGroup({
      preserve: new UntypedFormControl(false)
    });

    this.showCriticalConfirmationModal(
      $localize`delete`,
      $localize`OSD`,
      $localize`deleted`,
      (ids: number[]) => {
        return this.osdService.safeToDelete(JSON.stringify(ids));
      },
      'is_safe_to_delete',
      (id: number) => {
        this.selection = new CdTableSelection();
        return this.taskWrapper.wrapTaskAroundCall({
          task: new FinishedTask('osd/' + URLVerbs.DELETE, {
            svc_id: id
          }),
          call: this.osdService.delete(id, deleteFormGroup.value.preserve, true)
        });
      },
      true,
      deleteFormGroup,
      this.deleteOsdExtraTpl
    );
  }

  /**
   * Perform check first and display a critical confirmation modal.
   * @param {string} actionDescription name of the action.
   * @param {string} itemDescription the item's name that the action operates on.
   * @param {string} templateItemDescription the action name to be displayed in modal template.
   * @param {Function} check the function is called to check if the action is safe.
   * @param {string} checkKey the safe indicator's key in the check response.
   * @param {Function} action the action function.
   * @param {boolean} taskWrapped if true, hide confirmation modal after action
   * @param {CdFormGroup} childFormGroup additional child form group to be passed to confirmation modal
   * @param {TemplateRef<any>} childFormGroupTemplate template for additional child form group
   */
  showCriticalConfirmationModal(
    actionDescription: string,
    itemDescription: string,
    templateItemDescription: string,
    check: (ids: number[]) => Observable<any>,
    checkKey: string,
    action: (id: number | number[]) => Observable<any>,
    taskWrapped: boolean = false,
    childFormGroup?: CdFormGroup,
    childFormGroupTemplate?: TemplateRef<any>
  ): void {
    check(this.getSelectedOsdIds()).subscribe((result) => {
      const modalRef = this.modalService.show(CriticalConfirmationModalComponent, {
        actionDescription: actionDescription,
        itemDescription: itemDescription,
        bodyTemplate: this.criticalConfirmationTpl,
        bodyContext: {
          safeToPerform: result[checkKey],
          message: result.message,
          active: result.active,
          missingStats: result.missing_stats,
          storedPgs: result.stored_pgs,
          actionDescription: templateItemDescription,
          osdIds: this.getSelectedOsdIds()
        },
        childFormGroup: childFormGroup,
        childFormGroupTemplate: childFormGroupTemplate,
        submitAction: () => {
          const observable = observableForkJoin(
            this.getSelectedOsdIds().map((osd: any) => action.call(this.osdService, osd))
          );
          if (taskWrapped) {
            observable.subscribe({
              error: () => {
                this.getOsdList();
                modalRef.close();
              },
              complete: () => modalRef.close()
            });
          } else {
            observable.subscribe(
              () => {
                this.getOsdList();
                modalRef.close();
              },
              () => modalRef.close()
            );
          }
        }
      });
    });
  }

  configureQosParamsAction() {
    this.bsModalRef = this.modalService.show(OsdRecvSpeedModalComponent);
  }

  configurePgScrubAction() {
    this.bsModalRef = this.modalService.show(OsdPgScrubModalComponent, undefined, { size: 'lg' });
  }
}
