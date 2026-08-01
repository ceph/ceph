import {
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  Component,
  EventEmitter,
  Input,
  OnChanges,
  OnInit,
  Output,
  PipeTransform
} from '@angular/core';
import { Router } from '@angular/router';

import _ from 'lodash';

import { InventoryDevice } from '~/app/ceph/cluster/inventory/inventory-devices/inventory-device.model';
import { OsdService } from '~/app/shared/api/osd.service';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableColumnFiltersChange } from '~/app/shared/models/cd-table-column-filters-change';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';
import { ModalService } from '~/app/shared/services/modal.service';
import { OsdDevicesSelectionModalComponent } from '../osd-devices-selection-modal/osd-devices-selection-modal.component';
import { DevicesSelectionChangeEvent } from './devices-selection-change-event.interface';
import { DevicesSelectionClearEvent } from './devices-selection-clear-event.interface';

interface DeviceFilterOption {
  raw: string;
  formatted: string;
}

interface DeviceFilterField {
  prop: string;
  name: string;
  pipe?: PipeTransform;
  options: DeviceFilterOption[];
}

@Component({
  selector: 'cd-osd-devices-selection-groups',
  templateUrl: './osd-devices-selection-groups.component.html',
  styleUrls: ['./osd-devices-selection-groups.component.scss'],
  standalone: false,
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class OsdDevicesSelectionGroupsComponent implements OnInit, OnChanges {
  // data, wal, db
  @Input() type: string;

  // Data, WAL, DB
  @Input() name: string;

  @Input() hostname: string;

  @Input() availDevices: InventoryDevice[];

  @Input() canSelect: boolean;

  @Input() inlineSelection = false;

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
  filterColumns = [
    'hostname',
    'human_readable_type',
    'sys_api.vendor',
    'sys_api.model',
    'sys_api.size'
  ];
  requiredFilters: string[] = [
    $localize`Type`,
    $localize`Vendor`,
    $localize`Model`,
    $localize`Size`
  ];
  filterFields: DeviceFilterField[] = [];
  selectedFilters: Record<string, string | undefined> = {};
  filteredDevices: InventoryDevice[] = [];
  inlineCapacity = 0;
  canInlineSubmit = false;
  hasAnyFilter = false;
  inlineFilterEvent?: CdTableColumnFiltersChange;
  tooltips = {
    noAvailDevices: $localize`No available devices`,
    addPrimaryFirst: $localize`Please add primary devices first`,
    addByFilters: $localize`Add devices by using filters`
  };

  private filterFieldConfig: Array<{
    prop: string;
    name: string;
    pipe?: PipeTransform;
  }>;

  constructor(
    private modalService: ModalService,
    public osdService: OsdService,
    private router: Router,
    private dimlessBinary: DimlessBinaryPipe,
    private cdr: ChangeDetectorRef
  ) {
    this.isOsdPage = this.router.url.includes('/osd');
    this.filterFieldConfig = [
      { prop: 'human_readable_type', name: $localize`Type` },
      { prop: 'hostname', name: $localize`Hostname` },
      { prop: 'sys_api.vendor', name: $localize`Vendor` },
      { prop: 'sys_api.model', name: $localize`Model` },
      { prop: 'sys_api.size', name: $localize`Size`, pipe: this.dimlessBinary }
    ];
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
      if (this.inlineSelection) {
        this.restoreSelectedFiltersFromApplied();
      }
    }
    if (this.inlineSelection) {
      this.initDefaultFilterValues();
      this.updateFilterFields();
      this.updateFilteredDevices();
    } else {
      this.updateAddButtonTooltip();
    }
    this.cdr.markForCheck();
  }

  ngOnChanges() {
    if (this.inlineSelection) {
      this.initDefaultFilterValues();
      this.updateFilterFields();
      this.updateFilteredDevices();
    } else {
      this.updateAddButtonTooltip();
    }
    this.cdr.markForCheck();
  }

  get showAddPrimaryFirstAlert(): boolean {
    return this.type !== 'data' && !this.canSelect && this.devices.length === 0;
  }

  get showFilterSection(): boolean {
    if (this.type === 'data') {
      return this.availDevices?.length > 0 || this.devices?.length > 0;
    }

    if (this.devices?.length > 0) {
      return true;
    }

    return this.canSelect && this.availDevices?.length > 0;
  }

  get showNoAvailDevicesAlert(): boolean {
    if (this.type !== 'data' && !this.canSelect) {
      return false;
    }

    return !this.showFilterSection;
  }

  get tableDevices(): InventoryDevice[] {
    return this.canInlineSubmit ? this.filteredDevices : [];
  }

  get displayedFilters(): CdTableColumnFiltersChange['filters'] {
    return this.inlineFilterEvent?.filters ?? this.appliedFilters ?? [];
  }

  get showSelectionSummary(): boolean {
    return (
      (this.canInlineSubmit && this.tableDevices.length > 0) ||
      (this.devices.length > 0 && this.appliedFilters.length > 0)
    );
  }

  onFilterFieldChange(prop: string, value: string) {
    const normalizedValue = value === '' ? undefined : value;
    const hadDevices = this.devices.length > 0;
    const clearedDevices = [...this.devices];
    this.selectedFilters[prop] = normalizedValue;
    this.updateFilteredDevices();
    this.syncSelectionState(hadDevices, clearedDevices);
    this.cdr.markForCheck();
  }

  showSelectionModal() {
    const diskType = this.name === 'Primary' ? 'hdd' : 'ssd';
    const initialState = {
      hostname: this.hostname,
      deviceType: this.name,
      diskType: diskType,
      devices: this.availDevices,
      filterColumns: this.filterColumns
    };
    const modalRef = this.modalService.show(OsdDevicesSelectionModalComponent, initialState, {
      size: 'xl'
    });
    modalRef.componentInstance.submitAction.subscribe((result: CdTableColumnFiltersChange) => {
      this.applySelectionResult(result);
    });
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
    this.appliedFilters = [];
    this.capacity = 0;
    this.selectedFilters = {};
    this.inlineCapacity = 0;
    this.canInlineSubmit = false;
    this.inlineFilterEvent = undefined;
    if (this.inlineSelection) {
      this.initDefaultFilterValues();
      this.updateFilterFields();
      this.updateFilteredDevices();
    }
    this.cleared.emit(event);
    this.cdr.markForCheck();
  }

  private get devicesForFiltering(): InventoryDevice[] {
    return _.uniqBy([...(this.availDevices || []), ...(this.devices || [])], 'uid');
  }

  private get canApplySelection(): boolean {
    return this.type === 'data' || this.canSelect;
  }

  private initDefaultFilterValues() {
    if (this.hostname && !this.selectedFilters['hostname']) {
      this.selectedFilters['hostname'] = this.hostname;
    }
  }

  private restoreSelectedFiltersFromApplied() {
    if (_.isEmpty(this.appliedFilters)) {
      return;
    }
    this.appliedFilters.forEach((filter) => {
      this.selectedFilters[filter.prop as string] = filter.value?.raw;
    });
  }

  private updateFilterFields() {
    this.filterFields = this.filterFieldConfig
      .filter((field) => this.filterColumns.includes(field.prop))
      .map((field) => ({
        ...field,
        options: this.buildFilterOptions(field.prop, field.pipe)
      }));
  }

  private buildFilterOptions(prop: string, pipe?: PipeTransform): DeviceFilterOption[] {
    const values = _.sortedUniq(
      _.filter(_.map(this.devicesForFiltering, prop), (value) => {
        return (_.isString(value) && value !== '') || _.isBoolean(value) || _.isFinite(value);
      }).sort()
    );

    return values.map((value) => this.createFilterOption(value, pipe));
  }

  private createFilterOption(value: unknown, pipe?: PipeTransform): DeviceFilterOption {
    const raw = _.toString(value);
    return {
      raw,
      formatted: pipe ? pipe.transform(value) : raw
    };
  }

  private syncSelectionState(hadDevices: boolean, clearedDevices: InventoryDevice[]) {
    if (this.canApplySelection && this.canInlineSubmit && this.filteredDevices.length > 0) {
      this.applySelectionResult(this.inlineFilterEvent);
      return;
    }

    if (hadDevices) {
      this.resetSelectionState();
      this.cleared.emit({
        type: this.type,
        clearedDevices
      });
    }
  }

  private resetSelectionState() {
    this.devices = [];
    this.capacity = 0;
    this.appliedFilters = [];
  }

  private updateFilteredDevices() {
    let data = [...this.devicesForFiltering];
    const appliedFilters: CdTableColumnFiltersChange['filters'] = [];

    this.filterFields.forEach((field) => {
      const selectedValue = this.selectedFilters[field.prop];
      if (_.isUndefined(selectedValue)) {
        return;
      }

      const option = field.options.find((entry) => entry.raw === selectedValue);
      if (!option) {
        delete this.selectedFilters[field.prop];
        return;
      }

      appliedFilters.push({
        name: field.name,
        prop: field.prop,
        value: option
      });
      data = data.filter((row) => `${_.get(row, field.prop)}` === selectedValue);
    });

    this.hasAnyFilter = !_.isEmpty(appliedFilters);
    const filtersWithoutHostname = appliedFilters.filter((filter) => filter.prop !== 'hostname');
    this.canInlineSubmit = !_.isEmpty(filtersWithoutHostname);
    this.filteredDevices = this.hasAnyFilter ? data : [];
    this.inlineCapacity = _.sumBy(this.filteredDevices, 'sys_api.size');

    if (this.hasAnyFilter) {
      const filteredUids = new Set(this.filteredDevices.map((device) => device.uid));
      this.inlineFilterEvent = {
        filters: appliedFilters,
        data: this.filteredDevices,
        dataOut: this.devicesForFiltering.filter((device) => !filteredUids.has(device.uid))
      };
    } else {
      this.inlineFilterEvent = undefined;
    }
  }

  private applySelectionResult(result: CdTableColumnFiltersChange) {
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
  }

  private updateAddButtonTooltip() {
    if (this.type === 'data' && this.availDevices.length === 0) {
      this.addButtonTooltip = this.tooltips.noAvailDevices;
    } else {
      if (!this.canSelect) {
        this.addButtonTooltip = this.tooltips.addPrimaryFirst;
      } else if (this.availDevices.length === 0) {
        this.addButtonTooltip = this.tooltips.noAvailDevices;
      } else {
        this.addButtonTooltip = this.tooltips.addByFilters;
      }
    }
  }
}
