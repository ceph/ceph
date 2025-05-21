import {
  AfterViewInit,
  ChangeDetectorRef,
  Component,
  EventEmitter,
  Inject,
  Optional,
  Output,
  ViewChild
} from '@angular/core';

import { BaseModal } from 'carbon-components-angular';
import _ from 'lodash';

import { InventoryDevice } from '~/app/ceph/cluster/inventory/inventory-devices/inventory-device.model';
import { InventoryDevicesComponent } from '~/app/ceph/cluster/inventory/inventory-devices/inventory-devices.component';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdTableColumnFiltersChange } from '~/app/shared/models/cd-table-column-filters-change';
import { WizardStepsService } from '~/app/shared/services/wizard-steps.service';

@Component({
  selector: 'cd-osd-devices-selection-modal',
  templateUrl: './osd-devices-selection-modal.component.html',
  styleUrls: ['./osd-devices-selection-modal.component.scss']
})
export class OsdDevicesSelectionModalComponent extends BaseModal implements AfterViewInit {
  @ViewChild('inventoryDevices')
  inventoryDevices: InventoryDevicesComponent;

  @Output()
  submitAction = new EventEmitter<CdTableColumnFiltersChange>();

  icons = Icons;

  formGroup: CdFormGroup;
  action: string;

  filteredDevices: InventoryDevice[] = [];
  capacity = 0;
  event: CdTableColumnFiltersChange;
  canSubmit = false;
  requiredFilters: string[] = [];

  constructor(
    private formBuilder: CdFormBuilder,
    private cdRef: ChangeDetectorRef,
    public actionLabels: ActionLabelsI18n,
    public wizardStepService: WizardStepsService,
    
    @Optional() @Inject('hostname') public hostname: string,
    @Optional() @Inject('deviceType') public deviceType: string,
    @Optional() @Inject('diskType') public diskType: string,
    @Optional() @Inject('devices') public devices: string,
    @Optional() @Inject('filterColumns') public filterColumns: (string | number)[]
  ) {
    super();
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
    this.cdRef.detectChanges();
  }

  onSubmit() {
    this.submitAction.emit(this.event);
    this.closeModal();
  }
}
