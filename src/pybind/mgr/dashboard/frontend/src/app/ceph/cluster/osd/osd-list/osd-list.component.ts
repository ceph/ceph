import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { BsModalRef, BsModalService } from 'ngx-bootstrap/modal';
import { Observable } from 'rxjs';

import { OsdService } from '../../../../shared/api/osd.service';
import { ConfirmationModalComponent } from '../../../../shared/components/confirmation-modal/confirmation-modal.component';
import { CriticalConfirmationModalComponent } from '../../../../shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { TableComponent } from '../../../../shared/datatable/table/table.component';
import { CellTemplate } from '../../../../shared/enum/cell-template.enum';
import { CdTableAction } from '../../../../shared/models/cd-table-action';
import { CdTableColumn } from '../../../../shared/models/cd-table-column';
import { CdTableSelection } from '../../../../shared/models/cd-table-selection';
import { Permission } from '../../../../shared/models/permissions';
import { DimlessBinaryPipe } from '../../../../shared/pipes/dimless-binary.pipe';
import { AuthStorageService } from '../../../../shared/services/auth-storage.service';
import { OsdFlagsModalComponent } from '../osd-flags-modal/osd-flags-modal.component';
import { OsdReweightModalComponent } from '../osd-reweight-modal/osd-reweight-modal.component';
import { OsdScrubModalComponent } from '../osd-scrub-modal/osd-scrub-modal.component';

@Component({
  selector: 'cd-osd-list',
  templateUrl: './osd-list.component.html',
  styleUrls: ['./osd-list.component.scss']
})
export class OsdListComponent implements OnInit {
  @ViewChild('statusColor')
  statusColor: TemplateRef<any>;
  @ViewChild('osdUsageTpl')
  osdUsageTpl: TemplateRef<any>;
  @ViewChild('markOsdConfirmationTpl')
  markOsdConfirmationTpl: TemplateRef<any>;
  @ViewChild('criticalConfirmationTpl')
  criticalConfirmationTpl: TemplateRef<any>;
  @ViewChild(TableComponent)
  tableComponent: TableComponent;
  @ViewChild('reweightBodyTpl')
  reweightBodyTpl: TemplateRef<any>;
  @ViewChild('safeToDestroyBodyTpl')
  safeToDestroyBodyTpl: TemplateRef<any>;

  permission: Permission;
  tableActions: CdTableAction[];
  bsModalRef: BsModalRef;
  columns: CdTableColumn[];

  osds = [];
  selection = new CdTableSelection();

  protected static collectStates(osd) {
    return [osd['in'] ? 'in' : 'out', osd['up'] ? 'up' : 'down'];
  }

  constructor(
    private authStorageService: AuthStorageService,
    private osdService: OsdService,
    private dimlessBinaryPipe: DimlessBinaryPipe,
    private modalService: BsModalService
  ) {
    this.permission = this.authStorageService.getPermissions().osd;
    this.tableActions = [
      {
        name: 'Scrub',
        permission: 'update',
        icon: 'fa-stethoscope',
        click: () => this.scrubAction(false),
        disable: () => !this.hasOsdSelected
      },
      {
        name: 'Deep Scrub',
        permission: 'update',
        icon: 'fa-cog',
        click: () => this.scrubAction(true),
        disable: () => !this.hasOsdSelected
      },
      {
        name: 'Reweight',
        permission: 'update',
        click: () => this.reweight(),
        disable: () => !this.hasOsdSelected,
        icon: 'fa-balance-scale'
      },
      {
        name: 'Mark Out',
        permission: 'update',
        click: () => this.markOut(),
        disable: () => this.isNotSelectedOrInState('out'),
        icon: 'fa-arrow-left'
      },
      {
        name: 'Mark In',
        permission: 'update',
        click: () => this.markIn(),
        disable: () => this.isNotSelectedOrInState('in'),
        icon: 'fa-arrow-right'
      },
      {
        name: 'Mark Down',
        permission: 'update',
        click: () => this.markDown(),
        disable: () => this.isNotSelectedOrInState('down'),
        icon: 'fa-arrow-down'
      },
      {
        name: 'Mark lost',
        permission: 'delete',
        click: () => this.markLost(),
        disable: () => this.isNotSelectedOrInState('up'),
        icon: 'fa-unlink'
      },
      {
        name: 'Remove',
        permission: 'delete',
        click: () => this.remove(),
        disable: () => this.isNotSelectedOrInState('up'),
        icon: 'fa-remove'
      },
      {
        name: 'Destroy',
        permission: 'delete',
        click: () => this.destroy(),
        disable: () => this.isNotSelectedOrInState('up'),
        icon: 'fa-eraser'
      }
    ];
  }

  ngOnInit() {
    this.columns = [
      { prop: 'host.name', name: 'Host' },
      { prop: 'id', name: 'ID', cellTransformation: CellTemplate.bold },
      { prop: 'collectedStates', name: 'Status', cellTemplate: this.statusColor },
      { prop: 'stats.numpg', name: 'PGs' },
      { prop: 'stats.stat_bytes', name: 'Size', pipe: this.dimlessBinaryPipe },
      { name: 'Usage', cellTemplate: this.osdUsageTpl },
      {
        prop: 'stats_history.out_bytes',
        name: 'Read bytes',
        cellTransformation: CellTemplate.sparkline
      },
      {
        prop: 'stats_history.in_bytes',
        name: 'Writes bytes',
        cellTransformation: CellTemplate.sparkline
      },
      { prop: 'stats.op_r', name: 'Read ops', cellTransformation: CellTemplate.perSecond },
      { prop: 'stats.op_w', name: 'Write ops', cellTransformation: CellTemplate.perSecond }
    ];
  }

  get hasOsdSelected() {
    if (this.selection.hasSelection) {
      const osdId = this.selection.first().id;
      const osd = this.osds.filter((o) => o.id === osdId).pop();
      return !!osd;
    }
    return false;
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  /**
   * Returns true if no row is selected or if the selected row is in the given
   * state. Useful for deactivating the corresponding menu entry.
   */
  isNotSelectedOrInState(state: 'in' | 'up' | 'down' | 'out'): boolean {
    if (!this.hasOsdSelected) {
      return true;
    }

    const osdId = this.selection.first().id;
    const osd = this.osds.filter((o) => o.id === osdId).pop();

    if (!osd) {
      // `osd` is undefined if the selected OSD has been removed.
      return true;
    }

    switch (state) {
      case 'in':
        return osd.in === 1;
      case 'out':
        return osd.in !== 1;
      case 'down':
        return osd.up !== 1;
      case 'up':
        return osd.up === 1;
    }
  }

  getOsdList() {
    this.osdService.getList().subscribe((data: any[]) => {
      this.osds = data;
      data.map((osd) => {
        osd.collectedStates = OsdListComponent.collectStates(osd);
        osd.stats_history.out_bytes = osd.stats_history.op_out_bytes.map((i) => i[1]);
        osd.stats_history.in_bytes = osd.stats_history.op_in_bytes.map((i) => i[1]);
        osd.cdIsBinary = true;
        return osd;
      });
    });
  }

  scrubAction(deep) {
    if (!this.hasOsdSelected) {
      return;
    }

    const initialState = {
      selected: this.tableComponent.selection.selected,
      deep: deep
    };

    this.bsModalRef = this.modalService.show(OsdScrubModalComponent, { initialState });
  }

  configureClusterAction() {
    this.bsModalRef = this.modalService.show(OsdFlagsModalComponent, {});
  }

  showConfirmationModal(markAction: string, onSubmit: (id: number) => Observable<any>) {
    this.bsModalRef = this.modalService.show(ConfirmationModalComponent, {
      initialState: {
        titleText: `Mark OSD ${markAction}`,
        buttonText: `Mark ${markAction}`,
        bodyTpl: this.markOsdConfirmationTpl,
        bodyContext: {
          markActionDescription: markAction
        },
        onSubmit: () => {
          onSubmit
            .call(this.osdService, this.selection.first().id)
            .subscribe(() => this.bsModalRef.hide());
        }
      }
    });
  }

  markOut() {
    this.showConfirmationModal('out', this.osdService.markOut);
  }

  markIn() {
    this.showConfirmationModal('in', this.osdService.markIn);
  }

  markDown() {
    this.showConfirmationModal('down', this.osdService.markDown);
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

  showCriticalConfirmationModal(
    actionDescription: string,
    itemDescription: string,
    templateItemDescription: string,
    action: (id: number) => Observable<any>
  ): void {
    this.osdService.safeToDestroy(this.selection.first().id).subscribe((result) => {
      const modalRef = this.modalService.show(CriticalConfirmationModalComponent, {
        initialState: {
          actionDescription: actionDescription,
          itemDescription: itemDescription,
          bodyTemplate: this.criticalConfirmationTpl,
          bodyContext: {
            result: result,
            actionDescription: templateItemDescription
          },
          submitAction: () => {
            action
              .call(this.osdService, this.selection.first().id)
              .subscribe(() => modalRef.hide());
          }
        }
      });
    });
  }

  markLost() {
    this.showCriticalConfirmationModal('Mark', 'OSD lost', 'marked lost', this.osdService.markLost);
  }

  remove() {
    this.showCriticalConfirmationModal('Remove', 'OSD', 'removed', this.osdService.remove);
  }

  destroy() {
    this.showCriticalConfirmationModal('destroy', 'OSD', 'destroyed', this.osdService.destroy);
  }
}
