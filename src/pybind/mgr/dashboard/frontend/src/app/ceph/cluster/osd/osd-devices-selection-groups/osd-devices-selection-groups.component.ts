import { Component, EventEmitter, Input, OnChanges, OnInit, Output } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';
import * as _ from 'lodash';
import { BsModalService, ModalOptions } from 'ngx-bootstrap/modal';

import { Icons } from '../../../../shared/enum/icons.enum';
import { CdTableColumnFiltersChange } from '../../../../shared/models/cd-table-column-filters-change';
import { InventoryDevice } from '../../inventory/inventory-devices/inventory-device.model';
import { OsdDevicesSelectionModalComponent } from '../osd-devices-selection-modal/osd-devices-selection-modal.component';
import { DevicesSelectionChangeEvent } from './devices-selection-change-event.interface';
import { DevicesSelectionClearEvent } from './devices-selection-clear-event.interface';

@Component({
  selector: 'cd-osd-devices-selection-groups',
  templateUrl: './osd-devices-selection-groups.component.html',
  styleUrls: ['./osd-devices-selection-groups.component.scss']
})
export class OsdDevicesSelectionGroupsComponent implements OnInit, OnChanges {
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
  capacity = 0;
  appliedFilters: any[] = [];

  addButtonTooltip: String;
  tooltips = {
    noAvailDevices: this.i18n('No available devices'),
    addPrimaryFirst: this.i18n('Please add primary devices first'),
    addByFilters: this.i18n('Add devices by using filters')
  };

  constructor(private bsModalService: BsModalService, private i18n: I18n) {}

  ngOnInit() {
    this.updateAddButtonTooltip();
  }

  ngOnChanges() {
    this.updateAddButtonTooltip();
  }

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
    modalRef.content.submitAction.subscribe((result: CdTableColumnFiltersChange) => {
      this.devices = result.data;
      this.capacity = _.sumBy(this.devices, 'sys_api.size');
      this.appliedFilters = result.filters;
      const event = _.assign({ type: this.type }, result);
      this.selected.emit(event);
    });
  }

  private updateAddButtonTooltip() {
    if (this.type === 'data' && this.availDevices.length === 0) {
      this.addButtonTooltip = this.tooltips.noAvailDevices;
    } else {
      if (!this.canSelect) {
        // No primary devices added yet.
        this.addButtonTooltip = this.tooltips.addPrimaryFirst;
      } else if (this.availDevices.length === 0) {
        this.addButtonTooltip = this.tooltips.noAvailDevices;
      } else {
        this.addButtonTooltip = this.tooltips.addByFilters;
      }
    }
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
