import { Component, EventEmitter, OnInit } from '@angular/core';
import { FormControl, ValidatorFn, Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';

import * as _ from 'lodash';
import { forkJoin, Observable, ReplaySubject } from 'rxjs';
import { first, switchMap } from 'rxjs/operators';

import { PoolService } from '../../../shared/api/pool.service';
import { RbdService } from '../../../shared/api/rbd.service';
import { ActionLabelsI18n } from '../../../shared/constants/app.constants';
import { Icons } from '../../../shared/enum/icons.enum';
import { CdForm } from '../../../shared/forms/cd-form';
import { CdFormGroup } from '../../../shared/forms/cd-form-group';
import {
  RbdConfigurationEntry,
  RbdConfigurationSourceField
} from '../../../shared/models/configuration';
import { FinishedTask } from '../../../shared/models/finished-task';
import { ImageSpec } from '../../../shared/models/image-spec';
import { Permission } from '../../../shared/models/permissions';
import { DimlessBinaryPipe } from '../../../shared/pipes/dimless-binary.pipe';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { FormatterService } from '../../../shared/services/formatter.service';
import { TaskWrapperService } from '../../../shared/services/task-wrapper.service';
import { Pool } from '../../pool/pool';
import { RBDImageFormat, RbdModel } from '../rbd-list/rbd-model';
import { RbdImageFeature } from './rbd-feature.interface';
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
export class RbdFormComponent extends CdForm implements OnInit {
  poolPermission: Permission;
  rbdForm: CdFormGroup;
  getDirtyConfigurationValues: (
    includeLocalField?: boolean,
    localField?: RbdConfigurationSourceField
  ) => RbdConfigurationEntry[];

  namespaces: Array<string> = [];
  namespacesByPoolCache = {};
  pools: Array<Pool> = null;
  allPools: Array<Pool> = null;
  dataPools: Array<string> = null;
  allDataPools: Array<string> = null;
  features: { [key: string]: RbdImageFeature };
  featuresList: RbdImageFeature[] = [];
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
  private rbdImage = new ReplaySubject(1);

  icons = Icons;

  constructor(
    private authStorageService: AuthStorageService,
    private route: ActivatedRoute,
    private poolService: PoolService,
    private rbdService: RbdService,
    private formatter: FormatterService,
    private taskWrapper: TaskWrapperService,
    private dimlessBinaryPipe: DimlessBinaryPipe,
    public actionLabels: ActionLabelsI18n,
    public router: Router
  ) {
    super();
    this.poolPermission = this.authStorageService.getPermissions().pool;
    this.resource = $localize`RBD`;
    this.features = {
      'deep-flatten': {
        desc: $localize`Deep flatten`,
        requires: null,
        allowEnable: false,
        allowDisable: true
      },
      layering: {
        desc: $localize`Layering`,
        requires: null,
        allowEnable: false,
        allowDisable: false
      },
      'exclusive-lock': {
        desc: $localize`Exclusive lock`,
        requires: null,
        allowEnable: true,
        allowDisable: true
      },
      'object-map': {
        desc: $localize`Object map (requires exclusive-lock)`,
        requires: 'exclusive-lock',
        allowEnable: true,
        allowDisable: true,
        initDisabled: true
      },
      journaling: {
        desc: $localize`Journaling (requires exclusive-lock)`,
        requires: 'exclusive-lock',
        allowEnable: true,
        allowDisable: true,
        initDisabled: true
      },
      'fast-diff': {
        desc: $localize`Fast diff (interlocked with object-map)`,
        requires: 'object-map',
        allowEnable: true,
        allowDisable: true,
        interlockedWith: 'object-map',
        initDisabled: true
      }
    };
    this.featuresList = this.objToArray(this.features);
    this.createForm();
  }

  objToArray(obj: { [key: string]: any }) {
    return _.map(obj, (o, key) => Object.assign(o, { key: key }));
  }

  createForm() {
    this.rbdForm = new CdFormGroup(
      {
        parent: new FormControl(''),
        name: new FormControl('', {
          validators: [Validators.required, Validators.pattern(/^[^@/]+?$/)]
        }),
        pool: new FormControl(null, {
          validators: [Validators.required]
        }),
        namespace: new FormControl(null),
        useDataPool: new FormControl(false),
        dataPool: new FormControl(null),
        size: new FormControl(null, {
          updateOn: 'blur'
        }),
        obj_size: new FormControl(this.defaultObjectSize),
        features: new CdFormGroup(
          this.featuresList.reduce((acc: object, e) => {
            acc[e.key] = new FormControl({ value: false, disabled: !!e.initDisabled });
            return acc;
          }, {})
        ),
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
    this.rbdForm.get('namespace').disable();
    this.rbdForm.get('useDataPool').disable();
    this.rbdForm.get('dataPool').disable();
    this.rbdForm.get('obj_size').disable();
    this.rbdForm.get('stripingUnit').disable();
    this.rbdForm.get('stripingCount').disable();

    /* RBD Image Format v1 */
    this.rbdImage.subscribe((image: RbdModel) => {
      if (image.image_format === RBDImageFormat.V1) {
        this.rbdForm.get('deep-flatten').disable();
        this.rbdForm.get('layering').disable();
        this.rbdForm.get('exclusive-lock').disable();
      }
    });
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
    enum Promisse {
      RbdServiceGet = 'rbdService.get',
      PoolServiceList = 'poolService.list'
    }
    const promisses = {};
    if (
      this.mode === this.rbdFormMode.editing ||
      this.mode === this.rbdFormMode.cloning ||
      this.mode === this.rbdFormMode.copying
    ) {
      this.route.params.subscribe((params: { image_spec: string; snap: string }) => {
        const imageSpec = ImageSpec.fromString(decodeURIComponent(params.image_spec));
        if (params.snap) {
          this.snapName = decodeURIComponent(params.snap);
        }
        promisses[Promisse.RbdServiceGet] = this.rbdService.get(imageSpec);
      });
    } else {
      // New image
      this.rbdService.defaultFeatures().subscribe((defaultFeatures: Array<string>) => {
        this.setFeatures(defaultFeatures);
      });
    }
    if (this.mode !== this.rbdFormMode.editing && this.poolPermission.read) {
      promisses[Promisse.PoolServiceList] = this.poolService.list([
        'pool_name',
        'type',
        'flags_names',
        'application_metadata'
      ]);
    }

    forkJoin(promisses).subscribe((data: object) => {
      // poolService.list
      if (data[Promisse.PoolServiceList]) {
        const pools: Pool[] = [];
        const dataPools = [];
        for (const pool of data[Promisse.PoolServiceList]) {
          if (this.rbdService.isRBDPool(pool)) {
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
        this.pools = pools;
        this.allPools = pools;
        this.dataPools = dataPools;
        this.allDataPools = dataPools;
        if (this.pools.length === 1) {
          const poolName = this.pools[0].pool_name;
          this.rbdForm.get('pool').setValue(poolName);
          this.onPoolChange(poolName);
        }
      }

      // rbdService.get
      if (data[Promisse.RbdServiceGet]) {
        const resp: RbdFormResponseModel = data[Promisse.RbdServiceGet];
        this.setResponse(resp, this.snapName);
        this.rbdImage.next(resp);
      }

      this.loadingReady();
    });

    _.each(this.features, (feature) => {
      this.rbdForm
        .get('features')
        .get(feature.key)
        .valueChanges.subscribe((value) => this.featureFormUpdate(feature.key, value));
    });
  }

  onPoolChange(selectedPoolName: string) {
    const newDataPools = this.allDataPools
      ? this.allDataPools.filter((dataPool: any) => {
          return dataPool.pool_name !== selectedPoolName;
        })
      : [];
    if (this.rbdForm.getValue('dataPool') === selectedPoolName) {
      this.rbdForm.get('dataPool').setValue(null);
    }
    this.dataPools = newDataPools;
    this.namespaces = null;
    if (selectedPoolName in this.namespacesByPoolCache) {
      this.namespaces = this.namespacesByPoolCache[selectedPoolName];
    } else {
      this.rbdService.listNamespaces(selectedPoolName).subscribe((namespaces: any[]) => {
        namespaces = namespaces.map((namespace) => namespace.namespace);
        this.namespacesByPoolCache[selectedPoolName] = namespaces;
        this.namespaces = namespaces;
      });
    }
    this.rbdForm.get('namespace').setValue(null);
  }

  onUseDataPoolChange() {
    if (!this.rbdForm.getValue('useDataPool')) {
      this.rbdForm.get('dataPool').setValue(null);
      this.onDataPoolChange(null);
    }
  }

  onDataPoolChange(selectedDataPoolName: string) {
    const newPools = this.allPools.filter((pool: Pool) => {
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

  protected getDependendChildFeatures(featureKey: string) {
    return _.filter(this.features, (f) => f.requires === featureKey) || [];
  }

  deepBoxCheck(key: string, checked: boolean) {
    const childFeatures = this.getDependendChildFeatures(key);
    childFeatures.forEach((feature) => {
      const featureControl = this.rbdForm.get(feature.key);
      if (checked) {
        featureControl.enable({ emitEvent: false });
      } else {
        featureControl.disable({ emitEvent: false });
        featureControl.setValue(false, { emitEvent: false });
        this.deepBoxCheck(feature.key, checked);
      }

      const featureFormGroup = this.rbdForm.get('features');
      if (this.mode === this.rbdFormMode.editing && featureFormGroup.get(feature.key).enabled) {
        if (this.response.features_name.indexOf(feature.key) !== -1 && !feature.allowDisable) {
          featureFormGroup.get(feature.key).disable();
        } else if (
          this.response.features_name.indexOf(feature.key) === -1 &&
          !feature.allowEnable
        ) {
          featureFormGroup.get(feature.key).disable();
        }
      }
    });
  }

  interlockCheck(key: string, checked: boolean) {
    // Adds a compatibility layer for Ceph cluster where the feature interlock of features hasn't
    // been implemented yet. It disables the feature interlock for images which only have one of
    // both interlocked features (at the time of this writing: object-map and fast-diff) enabled.
    const feature = this.featuresList.find((f) => f.key === key);
    if (this.response) {
      // Ignore `create` page
      const hasInterlockedFeature = feature.interlockedWith != null;
      const dependentInterlockedFeature = this.featuresList.find(
        (f) => f.interlockedWith === feature.key
      );
      const isOriginFeatureEnabled = !!this.response.features_name.find((e) => e === feature.key); // in this case: fast-diff
      if (hasInterlockedFeature) {
        const isLinkedEnabled = !!this.response.features_name.find(
          (e) => e === feature.interlockedWith
        ); // depends: object-map
        if (isOriginFeatureEnabled !== isLinkedEnabled) {
          return; // Ignore incompatible setting because it's from a previous cluster version
        }
      } else if (dependentInterlockedFeature) {
        const isOtherInterlockedFeatureEnabled = !!this.response.features_name.find(
          (e) => e === dependentInterlockedFeature.key
        );
        if (isOtherInterlockedFeatureEnabled !== isOriginFeatureEnabled) {
          return; // Ignore incompatible setting because it's from a previous cluster version
        }
      }
    }

    if (checked) {
      _.filter(this.features, (f) => f.interlockedWith === key).forEach((f) =>
        this.rbdForm.get(f.key).setValue(true, { emitEvent: false })
      );
    } else {
      if (feature.interlockedWith) {
        // Don't skip emitting the event here, as it prevents `fast-diff` from
        // becoming disabled when manually unchecked.  This is because it
        // triggers an update on `object-map` and if `object-map` doesn't emit,
        // `fast-diff` will not be automatically disabled.
        this.rbdForm.get('features').get(feature.interlockedWith).setValue(false);
      }
    }
  }

  featureFormUpdate(key: string, checked: boolean) {
    if (checked) {
      const required = this.features[key].requires;
      if (required && !this.rbdForm.getValue(required)) {
        this.rbdForm.get(`features.${key}`).setValue(false);
        return;
      }
    }
    this.deepBoxCheck(key, checked);
    this.interlockCheck(key, checked);
  }

  setFeatures(features: Array<string>) {
    const featuresControl = this.rbdForm.get('features');
    _.forIn(this.features, (feature) => {
      if (features.indexOf(feature.key) !== -1) {
        featuresControl.get(feature.key).setValue(true);
      }
      this.featureFormUpdate(feature.key, featuresControl.get(feature.key).value);
    });
  }

  setResponse(response: RbdFormResponseModel, snapName: string) {
    this.response = response;
    const imageSpec = new ImageSpec(
      response.pool_name,
      response.namespace,
      response.name
    ).toString();
    if (this.mode === this.rbdFormMode.cloning) {
      this.rbdForm.get('parent').setValue(`${imageSpec}@${snapName}`);
    } else if (this.mode === this.rbdFormMode.copying) {
      if (snapName) {
        this.rbdForm.get('parent').setValue(`${imageSpec}@${snapName}`);
      } else {
        this.rbdForm.get('parent').setValue(`${imageSpec}`);
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
    this.onPoolChange(response.pool_name);
    this.rbdForm.get('namespace').setValue(response.namespace);
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
    request.namespace = this.rbdForm.getValue('namespace');
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
        namespace: request.namespace,
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
    request.child_namespace = this.rbdForm.getValue('namespace');
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
    const imageSpec = new ImageSpec(
      this.response.pool_name,
      this.response.namespace,
      this.response.name
    );
    return this.taskWrapper.wrapTaskAroundCall({
      task: new FinishedTask('rbd/edit', {
        image_spec: imageSpec.toString()
      }),
      call: this.rbdService.update(imageSpec, this.editRequest())
    });
  }

  cloneAction(): Observable<any> {
    const request = this.cloneRequest();
    const imageSpec = new ImageSpec(
      this.response.pool_name,
      this.response.namespace,
      this.response.name
    );
    return this.taskWrapper.wrapTaskAroundCall({
      task: new FinishedTask('rbd/clone', {
        parent_image_spec: imageSpec.toString(),
        parent_snap_name: this.snapName,
        child_pool_name: request.child_pool_name,
        child_namespace: request.child_namespace,
        child_image_name: request.child_image_name
      }),
      call: this.rbdService.cloneSnapshot(imageSpec, this.snapName, request)
    });
  }

  copyRequest(): RbdFormCopyRequestModel {
    const request = new RbdFormCopyRequestModel();
    if (this.snapName) {
      request.snapshot_name = this.snapName;
    }
    request.dest_pool_name = this.rbdForm.getValue('pool');
    request.dest_namespace = this.rbdForm.getValue('namespace');
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
    const imageSpec = new ImageSpec(
      this.response.pool_name,
      this.response.namespace,
      this.response.name
    );
    return this.taskWrapper.wrapTaskAroundCall({
      task: new FinishedTask('rbd/copy', {
        src_image_spec: imageSpec.toString(),
        dest_pool_name: request.dest_pool_name,
        dest_namespace: request.dest_namespace,
        dest_image_name: request.dest_image_name
      }),
      call: this.rbdService.copy(imageSpec, request)
    });
  }

  submit() {
    if (!this.mode) {
      this.rbdImage.next('create');
    }
    this.rbdImage
      .pipe(
        first(),
        switchMap(() => {
          if (this.mode === this.rbdFormMode.editing) {
            return this.editAction();
          } else if (this.mode === this.rbdFormMode.cloning) {
            return this.cloneAction();
          } else if (this.mode === this.rbdFormMode.copying) {
            return this.copyAction();
          } else {
            return this.createAction();
          }
        })
      )
      .subscribe(
        () => undefined,
        () => this.rbdForm.setErrors({ cdSubmitButton: true }),
        () => this.router.navigate(['/block/rbd'])
      );
  }
}
