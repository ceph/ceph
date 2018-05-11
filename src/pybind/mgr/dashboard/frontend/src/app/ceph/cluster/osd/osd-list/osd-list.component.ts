import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { BsModalRef, BsModalService } from 'ngx-bootstrap';

import { OsdService } from '../../../../shared/api/osd.service';
import { TableComponent } from '../../../../shared/datatable/table/table.component';
import { CellTemplate } from '../../../../shared/enum/cell-template.enum';
import { CdTableColumn } from '../../../../shared/models/cd-table-column';
import { CdTableSelection } from '../../../../shared/models/cd-table-selection';
import { Permission } from '../../../../shared/models/permissions';
import { DimlessBinaryPipe } from '../../../../shared/pipes/dimless-binary.pipe';
import { AuthStorageService } from '../../../../shared/services/auth-storage.service';
import { OsdFlagsModalComponent } from '../osd-flags-modal/osd-flags-modal.component';
import { OsdScrubModalComponent } from '../osd-scrub-modal/osd-scrub-modal.component';

@Component({
  selector: 'cd-osd-list',
  templateUrl: './osd-list.component.html',
  styleUrls: ['./osd-list.component.scss']
})
export class OsdListComponent implements OnInit {
  @ViewChild('statusColor') statusColor: TemplateRef<any>;
  @ViewChild('osdUsageTpl') osdUsageTpl: TemplateRef<any>;
  @ViewChild(TableComponent) tableComponent: TableComponent;

  permission: Permission;
  bsModalRef: BsModalRef;
  osds = [];
  columns: CdTableColumn[];
  selection = new CdTableSelection();

  constructor(
    private authStorageService: AuthStorageService,
    private osdService: OsdService,
    private dimlessBinaryPipe: DimlessBinaryPipe,
    private modalService: BsModalService
  ) {
    this.permission = this.authStorageService.getPermissions().osd;
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

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  getOsdList() {
    this.osdService.getList().subscribe((data: any[]) => {
      this.osds = data;
      data.map((osd) => {
        osd.collectedStates = this.collectStates(osd);
        osd.stats_history.out_bytes = osd.stats_history.op_out_bytes.map((i) => i[1]);
        osd.stats_history.in_bytes = osd.stats_history.op_in_bytes.map((i) => i[1]);
        return osd;
      });
    });
  }

  collectStates(osd) {
    const select = (onState, offState) => (osd[onState] ? onState : offState);
    return [select('up', 'down'), select('in', 'out')];
  }

  beforeShowDetails(selection: CdTableSelection) {
    return selection.hasSingleSelection;
  }

  scrubAction(deep) {
    if (!this.tableComponent.selection.hasSelection) {
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
}
