import { Component, EventEmitter, Input, OnChanges, OnInit, Output } from '@angular/core';
import { Router } from '@angular/router';

import _ from 'lodash';

import { InventoryDevice } from '~/app/ceph/cluster/inventory/inventory-devices/inventory-device.model';
import { OsdService } from '~/app/shared/api/osd.service';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableColumnFiltersChange } from '~/app/shared/models/cd-table-column-filters-change';
import { ModalService } from '~/app/shared/services/modal.service';
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
  appliedFilters = new Array();
  expansionCanSelect = false;
  isOsdPage: boolean;

  addButtonTooltip: String;
  tooltips = {
    noAvailDevices: $localize`No available devices`,
    addPrimaryFirst: $localize`Please add primary devices first`,
    addByFilters: $localize`Add devices by using filters`
  };

  constructor(
    private modalService: ModalService,
    public osdService: OsdService,
    private router: Router
  ) {
    this.isOsdPage = this.router.url.includes('/osd');
  }

  ngOnInit() {
    if (!this.isOsdPage) {
      this.osdService?.osdDevices[this.type]
        ? (this.devices = this.osdService.osdDevices[this.type])
        : (this.devices = []);
      this.capacity = _.sumBy(this.devices, 'sys_api.size');
      this.osdService?.osdDevices
        ? (this.expansionCanSelect = this.osdService?.osdDevices['disableSelect'])
        : (this.expansionCanSelect = false);
    }
    this.updateAddButtonTooltip();
  }

  ngOnChanges() {
    this.updateAddButtonTooltip();
  }

  showSelectionModal() {
    const filterColumns = [
      'hostname',
      'human_readable_type',
      'sys_api.vendor',
      'sys_api.model',
      'sys_api.size'
    ];
    const diskType = this.name === 'Primary' ? 'hdd' : 'ssd';
    const initialState = {
      hostname: this.hostname,
      deviceType: this.name,
      diskType: diskType,
      devices: this.availDevices,
      filterColumns: filterColumns
    };
    const modalRef = this.modalService.show(OsdDevicesSelectionModalComponent, initialState, {
      size: 'xl'
    });
    modalRef.componentInstance.submitAction.subscribe((result: CdTableColumnFiltersChange) => {
      this.devices = result.data;
      this.capacity = _.sumBy(this.devices, 'sys_api.size');
      this.appliedFilters = result.filters;
      const event = _.assign({ type: this.type }, result);
      if (!this.isOsdPage) {
        this.osdService.osdDevices[this.type] = this.devices;
        this.osdService.osdDevices['disableSelect'] =
          this.canSelect || this.devices.length === this.availDevices.length;
        this.osdService.osdDevices[this.type]['capacity'] = this.capacity;
      }
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
    if (!this.isOsdPage) {
      this.expansionCanSelect = false;
      this.osdService.osdDevices['disableSelect'] = false;
      this.osdService.osdDevices = [];
    }
    const event = {
      type: this.type,
      clearedDevices: [...this.devices]
    };
    this.devices = [];
    this.cleared.emit(event);
  }
}
