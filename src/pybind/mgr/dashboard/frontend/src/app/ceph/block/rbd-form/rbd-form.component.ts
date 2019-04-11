import { Component, EventEmitter, OnInit } from '@angular/core';
import { FormControl, ValidatorFn, Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';

import { I18n } from '@ngx-translate/i18n-polyfill';
import * as _ from 'lodash';
import { Observable } from 'rxjs';

import { PoolService } from '../../../shared/api/pool.service';
import { RbdService } from '../../../shared/api/rbd.service';
import { ActionLabelsI18n } from '../../../shared/constants/app.constants';
import { CdFormGroup } from '../../../shared/forms/cd-form-group';
import {
  RbdConfigurationEntry,
  RbdConfigurationSourceField
} from '../../../shared/models/configuration';
import { FinishedTask } from '../../../shared/models/finished-task';
import { Permission } from '../../../shared/models/permissions';
import { DimlessBinaryPipe } from '../../../shared/pipes/dimless-binary.pipe';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { FormatterService } from '../../../shared/services/formatter.service';
import { TaskWrapperService } from '../../../shared/services/task-wrapper.service';
import { RbdFormCloneRequestModel } from './rbd-form-clone-request.model';
import { RbdFormCopyRequestModel } from './rbd-form-copy-request.model';
import { RbdFormCreateRequestModel } from './rbd-form-create-request.model';
import { RbdFormEditRequestModel } from './rbd-form-edit-request.model';
import { RbdFormMode } from './rbd-form-mode.enum';
import { RbdFormResponseModel } from './rbd-form-response.model';

@Component({
  selector: 'cd-rbd-form',
  templateUrl: './rbd-form.component.html',
  styleUrls: ['./rbd-form.component.scss']
})
export class RbdFormComponent implements OnInit {
  poolPermission: Permission;
  rbdForm: CdFormGroup;
  featuresFormGroups: CdFormGroup;
  deepFlattenFormControl: FormControl;
  layeringFormControl: FormControl;
  exclusiveLockFormControl: FormControl;
  objectMapFormControl: FormControl;
  journalingFormControl: FormControl;
  fastDiffFormControl: FormControl;
  getDirtyConfigurationValues: (
    includeLocalField?: boolean,
    localField?: RbdConfigurationSourceField
  ) => RbdConfigurationEntry[];

  pools: Array<string> = null;
  allPools: Array<string> = null;
  dataPools: Array<string> = null;
  allDataPools: Array<string> = null;
  features: any;
  featuresList = [];
  initializeConfigData = new EventEmitter<{
    initialData: RbdConfigurationEntry[];
    sourceType: RbdConfigurationSourceField;
  }>();

  pool: string;

  advancedEnabled = false;

  public rbdFormMode = RbdFormMode;
  mode: RbdFormMode;

  response: RbdFormResponseModel;
  snapName: string;

  defaultObjectSize = '4 MiB';

  objectSizes: Array<string> = [
    '4 KiB',
    '8 KiB',
    '16 KiB',
    '32 KiB',
    '64 KiB',
    '128 KiB',
    '256 KiB',
    '512 KiB',
    '1 MiB',
    '2 MiB',
    '4 MiB',
    '8 MiB',
    '16 MiB',
    '32 MiB'
  ];
  action: string;
  resource: string;

  constructor(
    private authStorageService: AuthStorageService,
    private route: ActivatedRoute,
    private router: Router,
    private poolService: PoolService,
    private rbdService: RbdService,
    private formatter: FormatterService,
    private taskWrapper: TaskWrapperService,
    private dimlessBinaryPipe: DimlessBinaryPipe,
    private i18n: I18n,
    public actionLabels: ActionLabelsI18n
  ) {
    this.poolPermission = this.authStorageService.getPermissions().pool;
    this.resource = this.i18n('RBD');
    this.features = {
      'deep-flatten': {
        desc: this.i18n('Deep flatten'),
        requires: null,
        allowEnable: false,
        allowDisable: true
      },
      layering: {
        desc: this.i18n('Layering'),
        requires: null,
        allowEnable: false,
        allowDisable: false
      },
      'exclusive-lock': {
        desc: this.i18n('Exclusive lock'),
        requires: null,
        allowEnable: true,
        allowDisable: true
      },
      'object-map': {
        desc: this.i18n('Object map (requires exclusive-lock)'),
        requires: 'exclusive-lock',
        allowEnable: true,
        allowDisable: true
      },
      journaling: {
        desc: this.i18n('Journaling (requires exclusive-lock)'),
        requires: 'exclusive-lock',
        allowEnable: true,
        allowDisable: true
      },
      'fast-diff': {
        desc: this.i18n('Fast diff (requires object-map)'),
        requires: 'object-map',
        allowEnable: true,
        allowDisable: true
      }
    };
    this.createForm();
    for (const key of Object.keys(this.features)) {
      const listItem = this.features[key];
      listItem.key = key;
      this.featuresList.push(listItem);
    }
  }

  createForm() {
    this.deepFlattenFormControl = new FormControl(false);
    this.layeringFormControl = new FormControl(false);
    this.exclusiveLockFormControl = new FormControl(false);
    this.objectMapFormControl = new FormControl({ value: false, disabled: true });
    this.journalingFormControl = new FormControl({ value: false, disabled: true });
    this.fastDiffFormControl = new FormControl({ value: false, disabled: true });
    this.featuresFormGroups = new CdFormGroup({
      'deep-flatten': this.deepFlattenFormControl,
      layering: this.layeringFormControl,
      'exclusive-lock': this.exclusiveLockFormControl,
      'object-map': this.objectMapFormControl,
      journaling: this.journalingFormControl,
      'fast-diff': this.fastDiffFormControl
    });
    this.rbdForm = new CdFormGroup(
      {
        parent: new FormControl(''),
        name: new FormControl('', {
          validators: [Validators.required, Validators.pattern(/^[^@/]+?$/)]
        }),
        pool: new FormControl(null, {
          validators: [Validators.required]
        }),
        useDataPool: new FormControl(false),
        dataPool: new FormControl(null),
        size: new FormControl(null, {
          updateOn: 'blur'
        }),
        obj_size: new FormControl(this.defaultObjectSize),
        features: this.featuresFormGroups,
        stripingUnit: new FormControl(null),
        stripingCount: new FormControl(null, {
          updateOn: 'blur'
        })
      },
      this.validateRbdForm(this.formatter)
    );
  }

  disableForEdit() {
    this.rbdForm.get('parent').disable();
    this.rbdForm.get('pool').disable();
    this.rbdForm.get('useDataPool').disable();
    this.rbdForm.get('dataPool').disable();
    this.rbdForm.get('obj_size').disable();
    this.rbdForm.get('stripingUnit').disable();
    this.rbdForm.get('stripingCount').disable();
  }

  disableForClone() {
    this.rbdForm.get('parent').disable();
    this.rbdForm.get('size').disable();
  }

  disableForCopy() {
    this.rbdForm.get('parent').disable();
    this.rbdForm.get('size').disable();
  }

  ngOnInit() {
    if (this.router.url.startsWith('/block/rbd/edit')) {
      this.mode = this.rbdFormMode.editing;
      this.action = this.actionLabels.EDIT;
      this.disableForEdit();
    } else if (this.router.url.startsWith('/block/rbd/clone')) {
      this.mode = this.rbdFormMode.cloning;
      this.disableForClone();
      this.action = this.actionLabels.CLONE;
    } else if (this.router.url.startsWith('/block/rbd/copy')) {
      this.mode = this.rbdFormMode.copying;
      this.action = this.actionLabels.COPY;
      this.disableForCopy();
    } else {
      this.action = this.actionLabels.CREATE;
    }
    if (
      this.mode === this.rbdFormMode.editing ||
      this.mode === this.rbdFormMode.cloning ||
      this.mode === this.rbdFormMode.copying
    ) {
      this.route.params.subscribe((params: { pool: string; name: string; snap: string }) => {
        const poolName = decodeURIComponent(params.pool);
        const rbdName = decodeURIComponent(params.name);
        if (params.snap) {
          this.snapName = decodeURIComponent(params.snap);
        }
        this.rbdService.get(poolName, rbdName).subscribe((resp: RbdFormResponseModel) => {
          this.setResponse(resp, this.snapName);
        });
      });
    } else {
      this.rbdService.defaultFeatures().subscribe((defaultFeatures: Array<string>) => {
        this.setFeatures(defaultFeatures);
      });
    }
    if (this.mode !== this.rbdFormMode.editing && this.poolPermission.read) {
      this.poolService
        .list(['pool_name', 'type', 'flags_names', 'application_metadata'])
        .then((resp) => {
          const pools = [];
          const dataPools = [];
          for (const pool of resp) {
            if (_.indexOf(pool.application_metadata, 'rbd') !== -1) {
              if (!pool.pool_name.includes('/')) {
                if (pool.type === 'replicated') {
                  pools.push(pool);
                  dataPools.push(pool);
                } else if (
                  pool.type === 'erasure' &&
                  pool.flags_names.indexOf('ec_overwrites') !== -1
                ) {
                  dataPools.push(pool);
                }
              }
            }
          }
          this.pools = pools;
          this.allPools = pools;
          this.dataPools = dataPools;
          this.allDataPools = dataPools;
          if (this.pools.length === 1) {
            const poolName = this.pools[0]['pool_name'];
            this.rbdForm.get('pool').setValue(poolName);
            this.onPoolChange(poolName);
          }
        });
    }
    this.deepFlattenFormControl.valueChanges.subscribe((value) => {
      this.watchDataFeatures('deep-flatten', value);
    });
    this.layeringFormControl.valueChanges.subscribe((value) => {
      this.watchDataFeatures('layering', value);
    });
    this.exclusiveLockFormControl.valueChanges.subscribe((value) => {
      this.watchDataFeatures('exclusive-lock', value);
    });
    this.objectMapFormControl.valueChanges.subscribe((value) => {
      this.watchDataFeatures('object-map', value);
    });
    this.journalingFormControl.valueChanges.subscribe((value) => {
      this.watchDataFeatures('journaling', value);
    });
    this.fastDiffFormControl.valueChanges.subscribe((value) => {
      this.watchDataFeatures('fast-diff', value);
    });
  }

  onPoolChange(selectedPoolName) {
    const newDataPools = this.allDataPools.filter((dataPool: any) => {
      return dataPool.pool_name !== selectedPoolName;
    });
    if (this.rbdForm.getValue('dataPool') === selectedPoolName) {
      this.rbdForm.get('dataPool').setValue(null);
    }
    this.dataPools = newDataPools;
  }

  onUseDataPoolChange() {
    if (!this.rbdForm.getValue('useDataPool')) {
      this.rbdForm.get('dataPool').setValue(null);
      this.onDataPoolChange(null);
    }
  }

  onDataPoolChange(selectedDataPoolName) {
    const newPools = this.allPools.filter((pool: any) => {
      return pool.pool_name !== selectedDataPoolName;
    });
    if (this.rbdForm.getValue('pool') === selectedDataPoolName) {
      this.rbdForm.get('pool').setValue(null);
    }
    this.pools = newPools;
  }

  validateRbdForm(formatter: FormatterService): ValidatorFn {
    return (formGroup: CdFormGroup) => {
      // Data Pool
      const useDataPoolControl = formGroup.get('useDataPool');
      const dataPoolControl = formGroup.get('dataPool');
      let dataPoolControlErrors = null;
      if (useDataPoolControl.value && dataPoolControl.value == null) {
        dataPoolControlErrors = { required: true };
      }
      dataPoolControl.setErrors(dataPoolControlErrors);
      // Size
      const sizeControl = formGroup.get('size');
      const objectSizeControl = formGroup.get('obj_size');
      const objectSizeInBytes = formatter.toBytes(
        objectSizeControl.value != null ? objectSizeControl.value : this.defaultObjectSize
      );
      const stripingCountControl = formGroup.get('stripingCount');
      const stripingCount = stripingCountControl.value != null ? stripingCountControl.value : 1;
      let sizeControlErrors = null;
      if (sizeControl.value === null) {
        sizeControlErrors = { required: true };
      } else {
        const sizeInBytes = formatter.toBytes(sizeControl.value);
        if (stripingCount * objectSizeInBytes > sizeInBytes) {
          sizeControlErrors = { invalidSizeObject: true };
        }
      }
      sizeControl.setErrors(sizeControlErrors);
      // Striping Unit
      const stripingUnitControl = formGroup.get('stripingUnit');
      let stripingUnitControlErrors = null;
      if (stripingUnitControl.value === null && stripingCountControl.value !== null) {
        stripingUnitControlErrors = { required: true };
      } else if (stripingUnitControl.value !== null) {
        const stripingUnitInBytes = formatter.toBytes(stripingUnitControl.value);
        if (stripingUnitInBytes > objectSizeInBytes) {
          stripingUnitControlErrors = { invalidStripingUnit: true };
        }
      }
      stripingUnitControl.setErrors(stripingUnitControlErrors);
      // Striping Count
      let stripingCountControlErrors = null;
      if (stripingCountControl.value === null && stripingUnitControl.value !== null) {
        stripingCountControlErrors = { required: true };
      } else if (stripingCount < 1) {
        stripingCountControlErrors = { min: true };
      }
      stripingCountControl.setErrors(stripingCountControlErrors);
      return null;
    };
  }

  deepBoxCheck(key, checked) {
    _.forIn(this.features, (details, feature) => {
      if (details.requires === key) {
        if (checked) {
          this.rbdForm.get(feature).enable();
        } else {
          this.rbdForm.get(feature).disable();
          this.rbdForm.get(feature).setValue(checked);
          this.watchDataFeatures(feature, checked);
          this.deepBoxCheck(feature, checked);
        }
      }
      if (this.mode === this.rbdFormMode.editing && this.rbdForm.get(feature).enabled) {
        if (this.response.features_name.indexOf(feature) !== -1 && !details.allowDisable) {
          this.rbdForm.get(feature).disable();
        } else if (this.response.features_name.indexOf(feature) === -1 && !details.allowEnable) {
          this.rbdForm.get(feature).disable();
        }
      }
    });
  }

  featureFormUpdate(key, checked) {
    if (checked) {
      const required = this.features[key].requires;
      if (required && !this.rbdForm.getValue(required)) {
        this.rbdForm.get(key).setValue(false);
        return;
      }
    }
    this.deepBoxCheck(key, checked);
  }

  watchDataFeatures(key, checked) {
    this.featureFormUpdate(key, checked);
  }

  setFeatures(features: Array<string>) {
    const featuresControl = this.rbdForm.get('features');
    _.forIn(this.features, (feature) => {
      if (features.indexOf(feature.key) !== -1) {
        featuresControl.get(feature.key).setValue(true);
      }
      this.watchDataFeatures(feature.key, featuresControl.get(feature.key).value);
    });
  }

  setResponse(response: RbdFormResponseModel, snapName: string) {
    this.response = response;
    if (this.mode === this.rbdFormMode.cloning) {
      this.rbdForm.get('parent').setValue(`${response.pool_name}/${response.name}@${snapName}`);
    } else if (this.mode === this.rbdFormMode.copying) {
      if (snapName) {
        this.rbdForm.get('parent').setValue(`${response.pool_name}/${response.name}@${snapName}`);
      } else {
        this.rbdForm.get('parent').setValue(`${response.pool_name}/${response.name}`);
      }
    } else if (response.parent) {
      const parent = response.parent;
      this.rbdForm
        .get('parent')
        .setValue(`${parent.pool_name}/${parent.image_name}@${parent.snap_name}`);
    }
    if (this.mode === this.rbdFormMode.editing) {
      this.rbdForm.get('name').setValue(response.name);
    }
    this.rbdForm.get('pool').setValue(response.pool_name);
    if (response.data_pool) {
      this.rbdForm.get('useDataPool').setValue(true);
      this.rbdForm.get('dataPool').setValue(response.data_pool);
    }
    this.rbdForm.get('size').setValue(this.dimlessBinaryPipe.transform(response.size));
    this.rbdForm.get('obj_size').setValue(this.dimlessBinaryPipe.transform(response.obj_size));
    this.setFeatures(response.features_name);
    this.rbdForm
      .get('stripingUnit')
      .setValue(this.dimlessBinaryPipe.transform(response.stripe_unit));
    this.rbdForm.get('stripingCount').setValue(response.stripe_count);

    /* Configuration */
    this.initializeConfigData.emit({
      initialData: this.response.configuration,
      sourceType: RbdConfigurationSourceField.image
    });
  }

  createRequest() {
    const request = new RbdFormCreateRequestModel();
    request.pool_name = this.rbdForm.getValue('pool');
    request.name = this.rbdForm.getValue('name');
    request.size = this.formatter.toBytes(this.rbdForm.getValue('size'));
    request.obj_size = this.formatter.toBytes(this.rbdForm.getValue('obj_size'));
    _.forIn(this.features, (feature) => {
      if (this.rbdForm.getValue(feature.key)) {
        request.features.push(feature.key);
      }
    });

    /* Striping */
    request.stripe_unit = this.formatter.toBytes(this.rbdForm.getValue('stripingUnit'));
    request.stripe_count = this.rbdForm.getValue('stripingCount');
    request.data_pool = this.rbdForm.getValue('dataPool');

    /* Configuration */
    request.configuration = this.getDirtyConfigurationValues();

    return request;
  }

  createAction(): Observable<any> {
    const request = this.createRequest();
    return this.taskWrapper.wrapTaskAroundCall({
      task: new FinishedTask('rbd/create', {
        pool_name: request.pool_name,
        image_name: request.name
      }),
      call: this.rbdService.create(request)
    });
  }

  editRequest() {
    const request = new RbdFormEditRequestModel();
    request.name = this.rbdForm.getValue('name');
    request.size = this.formatter.toBytes(this.rbdForm.getValue('size'));
    _.forIn(this.features, (feature) => {
      if (this.rbdForm.getValue(feature.key)) {
        request.features.push(feature.key);
      }
    });

    request.configuration = this.getDirtyConfigurationValues();

    return request;
  }

  cloneRequest(): RbdFormCloneRequestModel {
    const request = new RbdFormCloneRequestModel();
    request.child_pool_name = this.rbdForm.getValue('pool');
    request.child_image_name = this.rbdForm.getValue('name');
    request.obj_size = this.formatter.toBytes(this.rbdForm.getValue('obj_size'));
    _.forIn(this.features, (feature) => {
      if (this.rbdForm.getValue(feature.key)) {
        request.features.push(feature.key);
      }
    });

    /* Striping */
    request.stripe_unit = this.formatter.toBytes(this.rbdForm.getValue('stripingUnit'));
    request.stripe_count = this.rbdForm.getValue('stripingCount');
    request.data_pool = this.rbdForm.getValue('dataPool');

    /* Configuration */
    request.configuration = this.getDirtyConfigurationValues(
      true,
      RbdConfigurationSourceField.image
    );

    return request;
  }

  editAction(): Observable<any> {
    return this.taskWrapper.wrapTaskAroundCall({
      task: new FinishedTask('rbd/edit', {
        pool_name: this.response.pool_name,
        image_name: this.response.name
      }),
      call: this.rbdService.update(this.response.pool_name, this.response.name, this.editRequest())
    });
  }

  cloneAction(): Observable<any> {
    const request = this.cloneRequest();
    return this.taskWrapper.wrapTaskAroundCall({
      task: new FinishedTask('rbd/clone', {
        parent_pool_name: this.response.pool_name,
        parent_image_name: this.response.name,
        parent_snap_name: this.snapName,
        child_pool_name: request.child_pool_name,
        child_image_name: request.child_image_name
      }),
      call: this.rbdService.cloneSnapshot(
        this.response.pool_name,
        this.response.name,
        this.snapName,
        request
      )
    });
  }

  copyRequest(): RbdFormCopyRequestModel {
    const request = new RbdFormCopyRequestModel();
    if (this.snapName) {
      request.snapshot_name = this.snapName;
    }
    request.dest_pool_name = this.rbdForm.getValue('pool');
    request.dest_image_name = this.rbdForm.getValue('name');
    request.obj_size = this.formatter.toBytes(this.rbdForm.getValue('obj_size'));
    _.forIn(this.features, (feature) => {
      if (this.rbdForm.getValue(feature.key)) {
        request.features.push(feature.key);
      }
    });

    /* Striping */
    request.stripe_unit = this.formatter.toBytes(this.rbdForm.getValue('stripingUnit'));
    request.stripe_count = this.rbdForm.getValue('stripingCount');
    request.data_pool = this.rbdForm.getValue('dataPool');

    /* Configuration */
    request.configuration = this.getDirtyConfigurationValues(
      true,
      RbdConfigurationSourceField.image
    );

    return request;
  }

  copyAction(): Observable<any> {
    const request = this.copyRequest();

    return this.taskWrapper.wrapTaskAroundCall({
      task: new FinishedTask('rbd/copy', {
        src_pool_name: this.response.pool_name,
        src_image_name: this.response.name,
        dest_pool_name: request.dest_pool_name,
        dest_image_name: request.dest_image_name
      }),
      call: this.rbdService.copy(this.response.pool_name, this.response.name, request)
    });
  }

  submit() {
    let action: Observable<any>;

    if (this.mode === this.rbdFormMode.editing) {
      action = this.editAction();
    } else if (this.mode === this.rbdFormMode.cloning) {
      action = this.cloneAction();
    } else if (this.mode === this.rbdFormMode.copying) {
      action = this.copyAction();
    } else {
      action = this.createAction();
    }

    action.subscribe(
      undefined,
      () => this.rbdForm.setErrors({ cdSubmitButton: true }),
      () => this.router.navigate(['/block/rbd'])
    );
  }
}
