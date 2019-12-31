import { Component, EventEmitter, Input, Output } from '@angular/core';

import * as _ from 'lodash';

import { BsModalService, ModalOptions } from 'ngx-bootstrap/modal';
import { Icons } from '../../../../shared/enum/icons.enum';
import { InventoryDeviceFiltersChangeEvent } from '../../inventory/inventory-devices/inventory-device-filters-change-event.interface';
import { InventoryDevice } from '../../inventory/inventory-devices/inventory-device.model';
import { OsdDevicesSelectionModalComponent } from '../osd-devices-selection-modal/osd-devices-selection-modal.component';
import { DevicesSelectionChangeEvent } from './devices-selection-change-event.interface';
import { DevicesSelectionClearEvent } from './devices-selection-clear-event.interface';

@Component({
  selector: 'cd-osd-devices-selection-groups',
  templateUrl: './osd-devices-selection-groups.component.html',
  styleUrls: ['./osd-devices-selection-groups.component.scss']
})
export class OsdDevicesSelectionGroupsComponent {
  // data, wal, db
  @Input() type: string;

  // Data, WAL, DB
  @Input() name: string;

  @Input() hostname: string;

  @Input() availDevices: InventoryDevice[];

  @Input() canSelect: boolean;

  @Output()
  selected = new EventEmitter<DevicesSelectionChangeEvent>();

  @Output()
  cleared = new EventEmitter<DevicesSelectionClearEvent>();

  icons = Icons;
  devices: InventoryDevice[] = [];
  appliedFilters = [];

  constructor(private bsModalService: BsModalService) {}

  showSelectionModal() {
    let filterColumns = ['human_readable_type', 'sys_api.vendor', 'sys_api.model', 'sys_api.size'];
    if (this.type === 'data') {
      filterColumns = ['hostname', ...filterColumns];
    }
    const options: ModalOptions = {
      class: 'modal-xl',
      initialState: {
        hostname: this.hostname,
        deviceType: this.name,
        devices: this.availDevices,
        filterColumns: filterColumns
      }
    };
    const modalRef = this.bsModalService.show(OsdDevicesSelectionModalComponent, options);
    modalRef.content.submitAction.subscribe((result: InventoryDeviceFiltersChangeEvent) => {
      this.devices = result.filterInDevices;
      this.appliedFilters = result.filters;
      const event = _.assign({ type: this.type }, result);
      this.selected.emit(event);
    });
  }

  clearDevices() {
    const event = {
      type: this.type,
      clearedDevices: [...this.devices]
    };
    this.devices = [];
    this.cleared.emit(event);
  }
}
