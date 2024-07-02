import {
  Component,
  EventEmitter,
  Input,
  OnDestroy,
  OnInit,
  Output,
  ViewChild
} from '@angular/core';
import { UntypedFormControl } from '@angular/forms';
import { Router } from '@angular/router';

import _ from 'lodash';

import { InventoryDevice } from '~/app/ceph/cluster/inventory/inventory-devices/inventory-device.model';
import { HostService } from '~/app/shared/api/host.service';
import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
import { OsdService } from '~/app/shared/api/osd.service';
import { FormButtonPanelComponent } from '~/app/shared/components/form-button-panel/form-button-panel.component';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { FinishedTask } from '~/app/shared/models/finished-task';
import {
  DeploymentOptions,
  OsdDeploymentOptions
} from '~/app/shared/models/osd-deployment-options';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalService } from '~/app/shared/services/modal.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
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
export class OsdFormComponent extends CdForm implements OnInit, OnDestroy {
  @ViewChild('dataDeviceSelectionGroups')
  dataDeviceSelectionGroups: OsdDevicesSelectionGroupsComponent;

  @ViewChild('walDeviceSelectionGroups')
  walDeviceSelectionGroups: OsdDevicesSelectionGroupsComponent;

  @ViewChild('dbDeviceSelectionGroups')
  dbDeviceSelectionGroups: OsdDevicesSelectionGroupsComponent;

  @ViewChild('previewButtonPanel')
  previewButtonPanel: FormButtonPanelComponent;

  @Input()
  hideTitle = false;

  @Input()
  hideSubmitBtn = false;

  @Output() emitDriveGroup: EventEmitter<DriveGroup> = new EventEmitter();

  @Output() emitDeploymentOption: EventEmitter<object> = new EventEmitter();

  @Output() emitMode: EventEmitter<boolean> = new EventEmitter();

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

  simpleDeployment = true;

  deploymentOptions: DeploymentOptions;
  optionNames = Object.values(OsdDeploymentOptions);

  constructor(
    public actionLabels: ActionLabelsI18n,
    private authStorageService: AuthStorageService,
    private orchService: OrchestratorService,
    private hostService: HostService,
    private router: Router,
    private modalService: ModalService,
    private osdService: OsdService,
    private taskWrapper: TaskWrapperService
  ) {
    super();
    this.resource = $localize`OSDs`;
    this.action = this.actionLabels.CREATE;
    this.features = {
      encrypted: {
        key: 'encrypted',
        desc: $localize`Encryption`
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

    this.osdService.getDeploymentOptions().subscribe((options) => {
      this.deploymentOptions = options;
      if (!this.osdService.selectedFormValues) {
        this.form.get('deploymentOption').setValue(this.deploymentOptions?.recommended_option);
      }

      if (this.deploymentOptions?.recommended_option) {
        this.enableFeatures();
      }
    });

    // restoring form value on back/next
    if (this.osdService.selectedFormValues) {
      this.form = _.cloneDeep(this.osdService.selectedFormValues);
      this.form
        .get('deploymentOption')
        .setValue(this.osdService.selectedFormValues.value?.deploymentOption);
    }
    this.simpleDeployment = this.osdService.isDeployementModeSimple;
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
      walSlots: new UntypedFormControl(0),
      dbSlots: new UntypedFormControl(0),
      features: new CdFormGroup(
        this.featureList.reduce((acc: object, e) => {
          // disable initially because no data devices are selected
          acc[e.key] = new UntypedFormControl({ value: false, disabled: true });
          return acc;
        }, {})
      ),
      deploymentOption: new UntypedFormControl(0)
    });
  }

  getDataDevices() {
    this.hostService.inventoryDeviceList().subscribe(
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

    this.emitDriveGroup.emit(this.driveGroup);
  }

  onDevicesCleared(event: DevicesSelectionClearEvent) {
    if (event.type === 'data') {
      this.hostname = '';
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

  emitDeploymentSelection() {
    const option = this.form.get('deploymentOption').value;
    const encrypted = this.form.get('encrypted').value;
    this.emitDeploymentOption.emit({ option: option, encrypted: encrypted });
  }

  emitDeploymentMode() {
    this.simpleDeployment = !this.simpleDeployment;
    if (!this.simpleDeployment && this.dataDeviceSelectionGroups.devices.length === 0) {
      this.disableFeatures();
    } else {
      this.enableFeatures();
    }
    this.emitMode.emit(this.simpleDeployment);
  }

  submit() {
    if (this.simpleDeployment) {
      const option = this.form.get('deploymentOption').value;
      const encrypted = this.form.get('encrypted').value;
      const deploymentSpec = { option: option, encrypted: encrypted };
      const title = this.deploymentOptions.options[deploymentSpec.option].title;
      const trackingId = `${title} deployment`;
      this.taskWrapper
        .wrapTaskAroundCall({
          task: new FinishedTask('osd/' + URLVerbs.CREATE, {
            tracking_id: trackingId
          }),
          call: this.osdService.create([deploymentSpec], trackingId, 'predefined')
        })
        .subscribe({
          complete: () => {
            this.router.navigate(['/osd']);
          }
        });
    } else {
      // use user name and timestamp for drive group name
      const user = this.authStorageService.getUsername();
      this.driveGroup.setName(`dashboard-${user}-${_.now()}`);
      const modalRef = this.modalService.show(OsdCreationPreviewModalComponent, {
        driveGroups: [this.driveGroup.spec]
      });
      modalRef.componentInstance.submitAction.subscribe(() => {
        this.router.navigate(['/osd']);
      });
      this.previewButtonPanel.submitButton.loading = false;
    }
  }

  ngOnDestroy() {
    this.osdService.selectedFormValues = _.cloneDeep(this.form);
    this.osdService.isDeployementModeSimple = this.dataDeviceSelectionGroups?.devices?.length === 0;
  }
}
