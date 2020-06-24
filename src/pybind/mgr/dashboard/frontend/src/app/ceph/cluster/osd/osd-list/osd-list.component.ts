import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { FormControl } from '@angular/forms';
import { Router } from '@angular/router';

import { I18n } from '@ngx-translate/i18n-polyfill';
import * as _ from 'lodash';
import { BsModalRef, BsModalService } from 'ngx-bootstrap/modal';
import { forkJoin as observableForkJoin, Observable } from 'rxjs';

import { OsdService } from '../../../../shared/api/osd.service';
import { ListWithDetails } from '../../../../shared/classes/list-with-details.class';
import { ConfirmationModalComponent } from '../../../../shared/components/confirmation-modal/confirmation-modal.component';
import { CriticalConfirmationModalComponent } from '../../../../shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { FormModalComponent } from '../../../../shared/components/form-modal/form-modal.component';
import { ActionLabelsI18n, URLVerbs } from '../../../../shared/constants/app.constants';
import { TableComponent } from '../../../../shared/datatable/table/table.component';
import { CellTemplate } from '../../../../shared/enum/cell-template.enum';
import { Icons } from '../../../../shared/enum/icons.enum';
import { NotificationType } from '../../../../shared/enum/notification-type.enum';
import { CdFormGroup } from '../../../../shared/forms/cd-form-group';
import { CdTableAction } from '../../../../shared/models/cd-table-action';
import { CdTableColumn } from '../../../../shared/models/cd-table-column';
import { CdTableSelection } from '../../../../shared/models/cd-table-selection';
import { FinishedTask } from '../../../../shared/models/finished-task';
import { Permissions } from '../../../../shared/models/permissions';
import { DimlessBinaryPipe } from '../../../../shared/pipes/dimless-binary.pipe';
import { AuthStorageService } from '../../../../shared/services/auth-storage.service';
import { DepCheckerService } from '../../../../shared/services/dep-checker.service';
import { NotificationService } from '../../../../shared/services/notification.service';
import { TaskWrapperService } from '../../../../shared/services/task-wrapper.service';
import { URLBuilderService } from '../../../../shared/services/url-builder.service';
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
  @ViewChild(TableComponent, { static: true })
  tableComponent: TableComponent;
  @ViewChild('reweightBodyTpl')
  reweightBodyTpl: TemplateRef<any>;
  @ViewChild('safeToDestroyBodyTpl')
  safeToDestroyBodyTpl: TemplateRef<any>;
  @ViewChild('deleteOsdExtraTpl')
  deleteOsdExtraTpl: TemplateRef<any>;

  permissions: Permissions;
  tableActions: CdTableAction[];
  bsModalRef: BsModalRef;
  columns: CdTableColumn[];
  clusterWideActions: CdTableAction[];
  icons = Icons;

  selection = new CdTableSelection();
  osds: any[] = [];

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
    private modalService: BsModalService,
    private i18n: I18n,
    private urlBuilder: URLBuilderService,
    private router: Router,
    private depCheckerService: DepCheckerService,
    private taskWrapper: TaskWrapperService,
    public actionLabels: ActionLabelsI18n,
    public notificationService: NotificationService
  ) {
    super();
    this.permissions = this.authStorageService.getPermissions();
    this.tableActions = [
      {
        name: this.actionLabels.CREATE,
        permission: 'create',
        icon: Icons.add,
        click: () => {
          this.depCheckerService.checkOrchestratorOrModal(
            this.actionLabels.CREATE,
            this.i18n('OSD'),
            () => {
              this.router.navigate([this.urlBuilder.getCreate()]);
            }
          );
        },
        canBePrimary: (selection: CdTableSelection) => !selection.hasSelection
      },
      {
        name: this.actionLabels.EDIT,
        permission: 'update',
        icon: Icons.edit,
        click: () => this.editAction()
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
        click: () => this.showConfirmationModal(this.i18n('out'), this.osdService.markOut),
        disable: () => this.isNotSelectedOrInState('out'),
        icon: Icons.left
      },
      {
        name: this.actionLabels.MARK_IN,
        permission: 'update',
        click: () => this.showConfirmationModal(this.i18n('in'), this.osdService.markIn),
        disable: () => this.isNotSelectedOrInState('in'),
        icon: Icons.right
      },
      {
        name: this.actionLabels.MARK_DOWN,
        permission: 'update',
        click: () => this.showConfirmationModal(this.i18n('down'), this.osdService.markDown),
        disable: () => this.isNotSelectedOrInState('down'),
        icon: Icons.down
      },
      {
        name: this.actionLabels.MARK_LOST,
        permission: 'delete',
        click: () =>
          this.showCriticalConfirmationModal(
            this.i18n('Mark'),
            this.i18n('OSD lost'),
            this.i18n('marked lost'),
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
            this.i18n('Purge'),
            this.i18n('OSD'),
            this.i18n('purged'),
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
            this.i18n('destroy'),
            this.i18n('OSD'),
            this.i18n('destroyed'),
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
        disable: () => !this.hasOsdSelected,
        icon: Icons.destroy
      }
    ];
  }

  ngOnInit() {
    this.clusterWideActions = [
      {
        name: this.i18n('Flags'),
        icon: Icons.flag,
        click: () => this.configureFlagsAction(),
        permission: 'read',
        visible: () => this.permissions.osd.read
      },
      {
        name: this.i18n('Recovery Priority'),
        icon: Icons.deepCheck,
        click: () => this.configureQosParamsAction(),
        permission: 'read',
        visible: () => this.permissions.configOpt.read
      },
      {
        name: this.i18n('PG scrub'),
        icon: Icons.analyse,
        click: () => this.configurePgScrubAction(),
        permission: 'read',
        visible: () => this.permissions.configOpt.read
      }
    ];
    this.columns = [
      { prop: 'host.name', name: this.i18n('Host') },
      { prop: 'id', name: this.i18n('ID'), flexGrow: 1, cellTransformation: CellTemplate.bold },
      {
        prop: 'collectedStates',
        name: this.i18n('Status'),
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
        name: this.i18n('Device class'),
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
        name: this.i18n('PGs'),
        flexGrow: 1
      },
      {
        prop: 'stats.stat_bytes',
        name: this.i18n('Size'),
        flexGrow: 1,
        pipe: this.dimlessBinaryPipe
      },
      { prop: 'stats.usage', name: this.i18n('Usage'), cellTemplate: this.osdUsageTpl },
      {
        prop: 'stats_history.out_bytes',
        name: this.i18n('Read bytes'),
        cellTransformation: CellTemplate.sparkline
      },
      {
        prop: 'stats_history.in_bytes',
        name: this.i18n('Write bytes'),
        cellTransformation: CellTemplate.sparkline
      },
      {
        prop: 'stats.op_r',
        name: this.i18n('Read ops'),
        cellTransformation: CellTemplate.perSecond
      },
      {
        prop: 'stats.op_w',
        name: this.i18n('Write ops'),
        cellTransformation: CellTemplate.perSecond
      }
    ];
  }

  /**
   * Only returns valid IDs, e.g. if an OSD is falsely still selected after being deleted, it won't
   * get returned.
   */
  getSelectedOsdIds(): number[] {
    const osdIds = this.osds.map((osd) => osd.id);
    return this.selection.selected.map((row) => row.id).filter((id) => osdIds.includes(id));
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
    this.osdService.getList().subscribe((data: any[]) => {
      this.osds = data.map((osd) => {
        osd.collectedStates = OsdListComponent.collectStates(osd);
        osd.stats_history.out_bytes = osd.stats_history.op_out_bytes.map((i: string) => i[1]);
        osd.stats_history.in_bytes = osd.stats_history.op_in_bytes.map((i: string) => i[1]);
        osd.stats.usage = osd.stats.stat_bytes_used / osd.stats.stat_bytes;
        osd.cdIsBinary = true;
        return osd;
      });
    });
  }

  editAction() {
    const selectedOsd = _.filter(this.osds, ['id', this.selection.first().id]).pop();

    this.modalService.show(FormModalComponent, {
      initialState: {
        titleText: this.i18n('Edit OSD: {{id}}', {
          id: selectedOsd.id
        }),
        fields: [
          {
            type: 'text',
            name: 'deviceClass',
            value: selectedOsd.tree.device_class,
            label: this.i18n('Device class'),
            required: true
          }
        ],
        submitButtonText: this.i18n('Edit OSD'),
        onSubmit: (values: any) => {
          this.osdService.update(selectedOsd.id, values.deviceClass).subscribe(() => {
            this.notificationService.show(
              NotificationType.success,
              this.i18n(`Updated OSD '{{id}}'`, {
                id: selectedOsd.id
              })
            );
            this.getOsdList();
          });
        }
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

    this.bsModalRef = this.modalService.show(OsdScrubModalComponent, { initialState });
  }

  configureFlagsAction() {
    this.bsModalRef = this.modalService.show(OsdFlagsModalComponent, {});
  }

  showConfirmationModal(markAction: string, onSubmit: (id: number) => Observable<any>) {
    this.bsModalRef = this.modalService.show(ConfirmationModalComponent, {
      initialState: {
        titleText: this.i18n('Mark OSD {{markAction}}', { markAction: markAction }),
        buttonText: this.i18n('Mark {{markAction}}', { markAction: markAction }),
        bodyTpl: this.markOsdConfirmationTpl,
        bodyContext: {
          markActionDescription: markAction
        },
        onSubmit: () => {
          observableForkJoin(
            this.getSelectedOsdIds().map((osd: any) => onSubmit.call(this.osdService, osd))
          ).subscribe(() => this.bsModalRef.hide());
        }
      }
    });
  }

  reweight() {
    const selectedOsd = this.osds.filter((o) => o.id === this.selection.first().id).pop();
    this.modalService.show(OsdReweightModalComponent, {
      initialState: {
        currentWeight: selectedOsd.weight,
        osdId: selectedOsd.id
      }
    });
  }

  delete() {
    const deleteFormGroup = new CdFormGroup({
      preserve: new FormControl(false)
    });

    this.depCheckerService.checkOrchestratorOrModal(
      this.actionLabels.DELETE,
      this.i18n('OSD'),
      () => {
        this.showCriticalConfirmationModal(
          this.i18n('delete'),
          this.i18n('OSD'),
          this.i18n('deleted'),
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
        initialState: {
          actionDescription: actionDescription,
          itemDescription: itemDescription,
          bodyTemplate: this.criticalConfirmationTpl,
          bodyContext: {
            safeToPerform: result[checkKey],
            message: result.message,
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
                  modalRef.hide();
                },
                complete: () => modalRef.hide()
              });
            } else {
              observable.subscribe(
                () => {
                  this.getOsdList();
                  modalRef.hide();
                },
                () => modalRef.hide()
              );
            }
          }
        }
      });
    });
  }

  configureQosParamsAction() {
    this.bsModalRef = this.modalService.show(OsdRecvSpeedModalComponent, {});
  }

  configurePgScrubAction() {
    this.bsModalRef = this.modalService.show(OsdPgScrubModalComponent, { class: 'modal-lg' });
  }
}
