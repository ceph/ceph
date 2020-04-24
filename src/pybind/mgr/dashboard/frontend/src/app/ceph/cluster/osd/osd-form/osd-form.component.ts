import { Component, OnInit, ViewChild } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';
import { Router } from '@angular/router';

import { I18n } from '@ngx-translate/i18n-polyfill';
import * as _ from 'lodash';
import { BsModalService } from 'ngx-bootstrap/modal';

import { OrchestratorService } from '../../../../shared/api/orchestrator.service';
import { SubmitButtonComponent } from '../../../../shared/components/submit-button/submit-button.component';
import { ActionLabelsI18n } from '../../../../shared/constants/app.constants';
import { Icons } from '../../../../shared/enum/icons.enum';
import { CdForm } from '../../../../shared/forms/cd-form';
import { CdFormGroup } from '../../../../shared/forms/cd-form-group';
import { CdTableColumn } from '../../../../shared/models/cd-table-column';
import { AuthStorageService } from '../../../../shared/services/auth-storage.service';
import { InventoryDevice } from '../../inventory/inventory-devices/inventory-device.model';
import { OsdCreationPreviewModalComponent } from '../osd-creation-preview-modal/osd-creation-preview-modal.component';
import { DevicesSelectionChangeEvent } from '../osd-devices-selection-groups/devices-selection-change-event.interface';
import { DevicesSelectionClearEvent } from '../osd-devices-selection-groups/devices-selection-clear-event.interface';
import { OsdDevicesSelectionGroupsComponent } from '../osd-devices-selection-groups/osd-devices-selection-groups.component';
import { DriveGroup } from './drive-group.model';
import { OsdFeature } from './osd-feature.interface';

@Component({
  selector: 'cd-osd-form',
  templateUrl: './osd-form.component.html',
  styleUrls: ['./osd-form.component.scss']
})
export class OsdFormComponent extends CdForm implements OnInit {
  @ViewChild('dataDeviceSelectionGroups', { static: false })
  dataDeviceSelectionGroups: OsdDevicesSelectionGroupsComponent;

  @ViewChild('walDeviceSelectionGroups', { static: false })
  walDeviceSelectionGroups: OsdDevicesSelectionGroupsComponent;

  @ViewChild('dbDeviceSelectionGroups', { static: false })
  dbDeviceSelectionGroups: OsdDevicesSelectionGroupsComponent;

  @ViewChild('previewButton', { static: false })
  previewButton: SubmitButtonComponent;

  icons = Icons;

  form: CdFormGroup;
  columns: Array<CdTableColumn> = [];

  allDevices: InventoryDevice[] = [];

  availDevices: InventoryDevice[] = [];
  dataDeviceFilters: any[] = [];
  dbDeviceFilters: any[] = [];
  walDeviceFilters: any[] = [];
  hostname = '';
  driveGroup = new DriveGroup();

  action: string;
  resource: string;

  features: { [key: string]: OsdFeature };
  featureList: OsdFeature[] = [];

  hasOrchestrator = true;
  docsUrl: string;

  constructor(
    public actionLabels: ActionLabelsI18n,
    private authStorageService: AuthStorageService,
    private i18n: I18n,
    private orchService: OrchestratorService,
    private router: Router,
    private bsModalService: BsModalService
  ) {
    super();
    this.resource = this.i18n('OSDs');
    this.action = this.actionLabels.CREATE;
    this.features = {
      encrypted: {
        key: 'encrypted',
        desc: this.i18n('Encryption')
      }
    };
    this.featureList = _.map(this.features, (o, key) => Object.assign(o, { key: key }));
    this.createForm();
  }

  ngOnInit() {
    this.orchService.status().subscribe((status) => {
      this.hasOrchestrator = status.available;
      if (status.available) {
        this.getDataDevices();
      } else {
        this.loadingNone();
      }
    });

    this.form.get('walSlots').valueChanges.subscribe((value) => this.setSlots('wal', value));
    this.form.get('dbSlots').valueChanges.subscribe((value) => this.setSlots('db', value));
    _.each(this.features, (feature) => {
      this.form
        .get('features')
        .get(feature.key)
        .valueChanges.subscribe((value) => this.featureFormUpdate(feature.key, value));
    });
  }

  createForm() {
    this.form = new CdFormGroup({
      walSlots: new FormControl(0, {
        updateOn: 'blur',
        validators: [Validators.min(0)]
      }),
      dbSlots: new FormControl(0, {
        updateOn: 'blur',
        validators: [Validators.min(0)]
      }),
      features: new CdFormGroup(
        this.featureList.reduce((acc: object, e) => {
          // disable initially because no data devices are selected
          acc[e.key] = new FormControl({ value: false, disabled: true });
          return acc;
        }, {})
      )
    });
  }

  getDataDevices() {
    this.orchService.inventoryDeviceList().subscribe(
      (devices: InventoryDevice[]) => {
        this.allDevices = _.filter(devices, 'available');
        this.availDevices = [...this.allDevices];
        this.loadingReady();
      },
      () => {
        this.allDevices = [];
        this.availDevices = [];
        this.loadingError();
      }
    );
  }

  setSlots(type: string, slots: number) {
    if (typeof slots !== 'number') {
      return;
    }
    if (slots >= 0) {
      this.driveGroup.setSlots(type, slots);
    }
  }

  featureFormUpdate(key: string, checked: boolean) {
    this.driveGroup.setFeature(key, checked);
  }

  enableFeatures() {
    this.featureList.forEach((feature) => {
      this.form.get(feature.key).enable({ emitEvent: false });
    });
  }

  disableFeatures() {
    this.featureList.forEach((feature) => {
      const control = this.form.get(feature.key);
      control.disable({ emitEvent: false });
      control.setValue(false, { emitEvent: false });
    });
  }

  onDevicesSelected(event: DevicesSelectionChangeEvent) {
    this.availDevices = event.dataOut;

    if (event.type === 'data') {
      // If user selects data devices for a single host, make only remaining devices on
      // that host as available.
      const hostnameFilter = _.find(event.filters, { prop: 'hostname' });
      if (hostnameFilter) {
        this.hostname = hostnameFilter.value.raw;
        this.availDevices = event.dataOut.filter((device: InventoryDevice) => {
          return device.hostname === this.hostname;
        });
        this.driveGroup.setHostPattern(this.hostname);
      } else {
        this.driveGroup.setHostPattern('*');
      }
      this.enableFeatures();
    }
    this.driveGroup.setDeviceSelection(event.type, event.filters);
  }

  onDevicesCleared(event: DevicesSelectionClearEvent) {
    if (event.type === 'data') {
      this.availDevices = [...this.allDevices];
      this.walDeviceSelectionGroups.devices = [];
      this.dbDeviceSelectionGroups.devices = [];
      this.disableFeatures();
      this.driveGroup.reset();
      this.form.get('walSlots').setValue(0, { emitEvent: false });
      this.form.get('dbSlots').setValue(0, { emitEvent: false });
    } else {
      this.availDevices = [...this.availDevices, ...event.clearedDevices];
      this.driveGroup.clearDeviceSelection(event.type);
      const slotControlName = `${event.type}Slots`;
      this.form.get(slotControlName).setValue(0, { emitEvent: false });
    }
  }

  submit() {
    // use user name and timestamp for drive group name
    const user = this.authStorageService.getUsername();
    this.driveGroup.setName(`dashboard-${user}-${_.now()}`);
    const modalRef = this.bsModalService.show(OsdCreationPreviewModalComponent, {
      initialState: { driveGroups: [this.driveGroup.spec] }
    });
    modalRef.content.submitAction.subscribe(() => {
      this.router.navigate(['/osd']);
    });
    this.previewButton.loading = false;
  }
}
