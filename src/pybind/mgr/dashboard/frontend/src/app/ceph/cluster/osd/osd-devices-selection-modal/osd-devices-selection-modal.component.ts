import { Component, EventEmitter, Output } from '@angular/core';

import * as _ from 'lodash';
import { BsModalRef } from 'ngx-bootstrap/modal';

import { ActionLabelsI18n } from '../../../../shared/constants/app.constants';
import { Icons } from '../../../../shared/enum/icons.enum';
import { CdFormBuilder } from '../../../../shared/forms/cd-form-builder';
import { CdFormGroup } from '../../../../shared/forms/cd-form-group';
import { CdTableColumnFiltersChange } from '../../../../shared/models/cd-table-column-filters-change';
import { InventoryDevice } from '../../inventory/inventory-devices/inventory-device.model';

@Component({
  selector: 'cd-osd-devices-selection-modal',
  templateUrl: './osd-devices-selection-modal.component.html',
  styleUrls: ['./osd-devices-selection-modal.component.scss']
})
export class OsdDevicesSelectionModalComponent {
  @Output()
  submitAction = new EventEmitter<CdTableColumnFiltersChange>();

  icons = Icons;
  filterColumns: string[] = [];

  hostname: string;
  deviceType: string;
  formGroup: CdFormGroup;
  action: string;

  devices: InventoryDevice[] = [];
  filteredDevices: InventoryDevice[] = [];
  event: CdTableColumnFiltersChange;
  canSubmit = false;

  constructor(
    private formBuilder: CdFormBuilder,
    public bsModalRef: BsModalRef,
    public actionLabels: ActionLabelsI18n
  ) {
    this.action = actionLabels.ADD;
    this.createForm();
  }

  createForm() {
    this.formGroup = this.formBuilder.group({});
  }

  onFilterChange(event: CdTableColumnFiltersChange) {
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
      this.event = event;
    }
  }

  onSubmit() {
    this.submitAction.emit(this.event);
    this.bsModalRef.hide();
  }
}
