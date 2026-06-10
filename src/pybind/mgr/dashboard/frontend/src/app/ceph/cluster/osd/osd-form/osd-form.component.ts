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
import { FormatterService } from '~/app/shared/services/formatter.service';
import { ModalService } from '~/app/shared/services/modal.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { TearsheetComponent } from '~/app/shared/components/tearsheet/tearsheet.component';
import { OsdCreationPreviewModalComponent } from '../osd-creation-preview-modal/osd-creation-preview-modal.component';
import { DevicesSelectionChangeEvent } from '../osd-devices-selection-groups/devices-selection-change-event.interface';
import { DevicesSelectionClearEvent } from '../osd-devices-selection-groups/devices-selection-clear-event.interface';
import { OsdDevicesSelectionGroupsComponent } from '../osd-devices-selection-groups/osd-devices-selection-groups.component';
import { DriveGroup } from './drive-group.model';
import { OsdFeature } from './osd-feature.interface';
import { Step } from 'carbon-components-angular';

interface ReviewField {
  label: string;
  value: string;
}

interface ReviewDeviceSelection {
  count: number;
  capacity: string;
  filters: ReviewField[];
  slots: number | null;
  hasSelection: boolean;
}

const STEP_LABELS = {
  DEPLOYMENT: $localize`Deployment Options`,
  DATA: $localize`Select data devices`,
  DB_WAL: $localize`Select DB/WAL devices (optional)`,
  FEATURES: $localize`Features`,
  REVIEW: $localize`Review`
} as const;

@Component({
  selector: 'cd-osd-form',
  templateUrl: './osd-form.component.html',
  styleUrls: ['./osd-form.component.scss'],
  standalone: false
})
export class OsdFormComponent extends CdForm implements OnInit, OnDestroy {
  @ViewChild(TearsheetComponent)
  tearsheet!: TearsheetComponent;

  @ViewChild('dataDeviceSelectionGroups')
  dataDeviceSelectionGroups!: OsdDevicesSelectionGroupsComponent;

  @ViewChild('walDeviceSelectionGroups')
  walDeviceSelectionGroups!: OsdDevicesSelectionGroupsComponent;

  @ViewChild('dbDeviceSelectionGroups')
  dbDeviceSelectionGroups!: OsdDevicesSelectionGroupsComponent;

  @ViewChild('previewButtonPanel')
  previewButtonPanel!: FormButtonPanelComponent;

  @Input()
  hideTitle = false;

  @Output() emitDriveGroup: EventEmitter<DriveGroup> = new EventEmitter();

  @Output() emitDeploymentOption: EventEmitter<object> = new EventEmitter();

  @Output() emitMode: EventEmitter<boolean> = new EventEmitter();

  @Output() osdCreated: EventEmitter<void> = new EventEmitter();

  icons = Icons;

  form!: CdFormGroup;
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
  featureList: Array<OsdFeature & { key: string }> = [];

  hasOrchestrator = true;

  simpleDeployment = true;
  createOsdsLabel = $localize`Create OSDs`;
  isSubmitLoading = false;

  deploymentOptions!: DeploymentOptions;
  optionNames = Object.values(OsdDeploymentOptions);

  steps: Array<Step> = this.getStepsForMode('automatic');

  reviewDeploymentModeLabel = $localize`Automatic`;
  reviewDeploymentOptionTitle = '';
  reviewDeploymentOptionDescription = '';
  reviewHostPattern = '';
  reviewEnabledFeatures: string[] = [];
  reviewDataSelection: ReviewDeviceSelection = this.createEmptyReviewDeviceSelection();
  reviewWalSelection: ReviewDeviceSelection = this.createEmptyReviewDeviceSelection();
  reviewDbSelection: ReviewDeviceSelection = this.createEmptyReviewDeviceSelection();

  constructor(
    public actionLabels: ActionLabelsI18n,
    private authStorageService: AuthStorageService,
    private orchService: OrchestratorService,
    private hostService: HostService,
    private router: Router,
    private formatterService: FormatterService,
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
    this.featureList = _.map(this.features, (o, key) => Object.assign({}, o, { key }));
    this.createForm();
  }

  private getStepsForMode(mode: string): Array<Step> {
    return mode !== 'manual'
      ? [
          { label: STEP_LABELS.DEPLOYMENT, invalid: false },
          { label: STEP_LABELS.FEATURES, invalid: false },
          { label: STEP_LABELS.REVIEW, invalid: false }
        ]
      : [
          { label: STEP_LABELS.DEPLOYMENT, invalid: false },
          { label: STEP_LABELS.DATA, invalid: false },
          { label: STEP_LABELS.DB_WAL, invalid: false },
          { label: STEP_LABELS.FEATURES, invalid: false },
          { label: STEP_LABELS.REVIEW, invalid: false }
        ];
  }

  private createEmptyReviewDeviceSelection(): ReviewDeviceSelection {
    return {
      count: 0,
      capacity: '',
      filters: [],
      slots: null,
      hasSelection: false
    };
  }

  private formatHostPattern(pattern?: string): string {
    if (!pattern || pattern === '*') {
      return $localize`All hosts`;
    }

    return pattern;
  }

  private getReviewFilters(selectionGroup?: OsdDevicesSelectionGroupsComponent): ReviewField[] {
    return (selectionGroup?.appliedFilters ?? []).map((filter) => ({
      label: filter.name,
      value: filter.value?.formatted ?? filter.value?.raw ?? '-'
    }));
  }

  private buildReviewDeviceSelection(
    selectionGroup?: OsdDevicesSelectionGroupsComponent,
    slotControlName?: 'walSlots' | 'dbSlots'
  ): ReviewDeviceSelection {
    const devices = selectionGroup?.devices ?? [];
    const totalCapacity = _.sumBy(devices, (device) => device?.sys_api?.size ?? 0);

    return {
      count: devices.length,
      capacity:
        devices.length > 0 ? this.formatterService.formatToBinary(totalCapacity, false) : '',
      filters: this.getReviewFilters(selectionGroup),
      slots:
        slotControlName && devices.length > 0
          ? Number(this.form.get(slotControlName)?.value ?? 0)
          : null,
      hasSelection: devices.length > 0
    };
  }

  private getEnabledFeatures(): string[] {
    return this.featureList
      .filter((feature) => this.form.get('features')?.get(feature.key)?.value)
      .map((feature) => feature.desc);
  }

  private getEncryptedFeatureValue(): boolean {
    return this.form.get('features')?.get('encrypted')?.value ?? false;
  }

  private updateSteps() {
    const mode = this.form?.get('deploymentMode')?.value ?? 'automatic';
    this.steps = this.getStepsForMode(mode);
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
        this.form.get('deploymentMode').setValue('automatic', { emitEvent: false });
        this.form.get('deploymentOption').setValue(this.deploymentOptions?.recommended_option);
      }

      if (this.deploymentOptions?.recommended_option) {
        this.enableFeatures();
      }
    });

    // restoring form value on back/next
    if (this.osdService.selectedFormValues) {
      this.form = _.cloneDeep(this.osdService.selectedFormValues);
      if (!this.form.get('deploymentMode')) {
        this.form.addControl('deploymentMode', new UntypedFormControl('automatic'));
      }
      this.form
        .get('deploymentOption')
        .setValue(this.osdService.selectedFormValues.value?.deploymentOption);
    }
    this.simpleDeployment = this.osdService.isDeployementModeSimple;
    this.form
      .get('deploymentMode')
      .setValue(this.simpleDeployment ? 'automatic' : 'manual', { emitEvent: false });
    this.updateSteps();
    this.form
      .get('deploymentMode')
      .valueChanges.subscribe((mode) => this.onDeploymentModeChanged(mode));
    this.form.get('walSlots').valueChanges.subscribe((value) => this.setSlots('wal', value));
    this.form.get('dbSlots').valueChanges.subscribe((value) => this.setSlots('db', value));
    _.each(this.features, (feature) => {
      const featureControl = this.form.get('features').get(feature.key ?? '');
      if (!featureControl) {
        return;
      }

      featureControl.valueChanges.subscribe((value) =>
        this.featureFormUpdate(feature.key ?? '', value)
      );
    });

    this.populateReviewData();
  }

  createForm() {
    this.form = new CdFormGroup({
      deploymentMode: new UntypedFormControl('automatic'),
      walSlots: new UntypedFormControl(0),
      dbSlots: new UntypedFormControl(0),
      features: new CdFormGroup(
        this.featureList.reduce((acc: object, e) => {
          // disable initially because no data devices are selected
          acc[e.key] = new UntypedFormControl({ value: false, disabled: true });
          return acc;
        }, {})
      ),
      deploymentOption: new UntypedFormControl(null)
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
      this.populateReviewData();
    }
  }

  featureFormUpdate(key: string, checked: boolean) {
    this.driveGroup.setFeature(key, checked);
    this.populateReviewData();
  }

  enableFeatures() {
    this.featureList.forEach((feature) => {
      const control = this.form.get('features').get(feature.key);
      if (!control) {
        return;
      }

      control.enable({ emitEvent: false });
    });
  }

  disableFeatures() {
    this.featureList.forEach((feature) => {
      const control = this.form.get('features').get(feature.key);
      if (!control) {
        return;
      }

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
    this.populateReviewData();

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

    this.populateReviewData();
  }

  emitDeploymentSelection() {
    const option = this.form.get('deploymentOption').value;
    const encrypted = this.getEncryptedFeatureValue();
    this.emitDeploymentOption.emit({ option: option, encrypted: encrypted });
  }

  onDeploymentModeChanged(mode: string) {
    const deploymentMode = mode ?? this.form?.get('deploymentMode')?.value ?? 'automatic';
    this.simpleDeployment = deploymentMode !== 'manual';
    this.updateSteps();
    const hasDataDevices = (this.dataDeviceSelectionGroups?.devices?.length ?? 0) > 0;
    if (!this.simpleDeployment && !hasDataDevices) {
      this.disableFeatures();
    } else {
      this.enableFeatures();
    }
    this.populateReviewData();
    this.emitMode.emit(this.simpleDeployment);
  }

  populateReviewData() {
    this.reviewDeploymentModeLabel = this.simpleDeployment
      ? $localize`Automatic`
      : $localize`Manual selection`;

    const selectedOption = this.form.get('deploymentOption')?.value as OsdDeploymentOptions;
    const deploymentOption = this.deploymentOptions?.options?.[selectedOption];
    this.reviewDeploymentOptionTitle = deploymentOption?.title ?? '';
    this.reviewDeploymentOptionDescription = deploymentOption?.desc ?? '';
    this.reviewEnabledFeatures = this.getEnabledFeatures();

    if (this.simpleDeployment) {
      this.reviewHostPattern = '';
      this.reviewDataSelection = this.createEmptyReviewDeviceSelection();
      this.reviewWalSelection = this.createEmptyReviewDeviceSelection();
      this.reviewDbSelection = this.createEmptyReviewDeviceSelection();
      return;
    }

    this.reviewHostPattern = this.formatHostPattern(
      this.hostname || (this.driveGroup.spec['host_pattern'] as string)
    );
    this.reviewDataSelection = this.buildReviewDeviceSelection(this.dataDeviceSelectionGroups);
    this.reviewWalSelection = this.buildReviewDeviceSelection(
      this.walDeviceSelectionGroups,
      'walSlots'
    );
    this.reviewDbSelection = this.buildReviewDeviceSelection(
      this.dbDeviceSelectionGroups,
      'dbSlots'
    );
  }

  private navigateAfterCreate() {
    const returnUrl = window.history.state?.returnUrl;

    if (this.osdCreated.observers.length > 0) {
      this.osdCreated.emit();
      return;
    }

    if (returnUrl === '/add-storage') {
      this.router.navigate(['/add-storage']);
      return;
    }

    const hasSafeReturnUrl =
      typeof returnUrl === 'string' &&
      returnUrl.startsWith('/') &&
      !returnUrl.startsWith('//') &&
      returnUrl !== '/osd/create';

    if (hasSafeReturnUrl) {
      this.router.navigateByUrl(returnUrl);
      return;
    }

    this.router.navigate(['/osd']);
  }

  submit() {
    if (this.simpleDeployment) {
      this.isSubmitLoading = true;
      const option = this.form.get('deploymentOption').value;
      const encrypted = this.getEncryptedFeatureValue();
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
          error: () => {
            this.isSubmitLoading = false;
          },
          complete: () => {
            this.isSubmitLoading = false;
            this.navigateAfterCreate();
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
        this.navigateAfterCreate();
      });
      this.isSubmitLoading = false;
      this.previewButtonPanel.submitButton.loading = false;
    }
  }

  ngOnDestroy() {
    this.osdService.selectedFormValues = _.cloneDeep(this.form);
    this.osdService.isDeployementModeSimple = this.simpleDeployment;
  }
}
