import { AfterViewInit, Component, EventEmitter, Output, ViewChild } from '@angular/core';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { TableColumnProp } from '@swimlane/ngx-datatable';
import * as _ from 'lodash';

import { ActionLabelsI18n } from '../../../../shared/constants/app.constants';
import { Icons } from '../../../../shared/enum/icons.enum';
import { CdFormBuilder } from '../../../../shared/forms/cd-form-builder';
import { CdFormGroup } from '../../../../shared/forms/cd-form-group';
import { CdTableColumnFiltersChange } from '../../../../shared/models/cd-table-column-filters-change';
import { InventoryDevice } from '../../inventory/inventory-devices/inventory-device.model';
import { InventoryDevicesComponent } from '../../inventory/inventory-devices/inventory-devices.component';

@Component({
  selector: 'cd-osd-devices-selection-modal',
  templateUrl: './osd-devices-selection-modal.component.html',
  styleUrls: ['./osd-devices-selection-modal.component.scss']
})
export class OsdDevicesSelectionModalComponent implements AfterViewInit {
  @ViewChild('inventoryDevices')
  inventoryDevices: InventoryDevicesComponent;

  @Output()
  submitAction = new EventEmitter<CdTableColumnFiltersChange>();

  icons = Icons;
  filterColumns: TableColumnProp[] = [];

  hostname: string;
  deviceType: string;
  formGroup: CdFormGroup;
  action: string;

  devices: InventoryDevice[] = [];
  filteredDevices: InventoryDevice[] = [];
  capacity = 0;
  event: CdTableColumnFiltersChange;
  canSubmit = false;
  requiredFilters: string[] = [];

  constructor(
    private formBuilder: CdFormBuilder,
    public activeModal: NgbActiveModal,
    public actionLabels: ActionLabelsI18n
  ) {
    this.action = actionLabels.ADD;
    this.createForm();
  }

  ngAfterViewInit() {
    // At least one filter other than hostname is required
    // Extract the name from table columns for i18n strings
    const cols = _.filter(this.inventoryDevices.columns, (col) => {
      return this.filterColumns.includes(col.prop) && col.prop !== 'hostname';
    });
    // Fixes 'ExpressionChangedAfterItHasBeenCheckedError'
    setTimeout(() => {
      this.requiredFilters = _.map(cols, 'name');
    }, 0);
  }

  createForm() {
    this.formGroup = this.formBuilder.group({});
  }

  onFilterChange(event: CdTableColumnFiltersChange) {
    this.capacity = 0;
    this.canSubmit = false;
    if (_.isEmpty(event.filters)) {
      // filters are cleared
      this.filteredDevices = [];
      this.event = undefined;
    } else {
      // at least one filter is required (except hostname)
      const filters = event.filters.filter((filter) => {
        return filter.prop !== 'hostname';
      });
      this.canSubmit = !_.isEmpty(filters);
      this.filteredDevices = event.data;
      this.capacity = _.sumBy(this.filteredDevices, 'sys_api.size');
      this.event = event;
    }
  }

  onSubmit() {
    this.submitAction.emit(this.event);
    this.activeModal.close();
  }
}
