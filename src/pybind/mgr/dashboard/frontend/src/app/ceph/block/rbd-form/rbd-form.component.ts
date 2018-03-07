import { Component, ElementRef, OnInit } from '@angular/core';
import { FormControl, FormGroup, Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';

import * as _ from 'lodash';
import { ToastsManager } from 'ng2-toastr';

import { UnixErrno } from '../../../shared/enum/unix_errno.enum';
import { FormService } from '../../../shared/services/form.service';
import { FormatterService } from '../../../shared/services/formatter.service';
import { PoolService } from '../../../shared/services/pool.service';
import { RbdService } from '../../../shared/services/rbd.service';
import { RbdFormRequestModel } from './rbd-form-request.model';
import { RbdFormModel } from './rbd-form.model';

@Component({
  selector: 'cd-rbd-form',
  templateUrl: './rbd-form.component.html',
  styleUrls: ['./rbd-form.component.scss']
})
export class RbdFormComponent implements OnInit {

  rbdForm: FormGroup;
  featuresFormGroups: FormGroup;
  defaultFeaturesFormControl: FormControl;
  deepFlattenFormControl: FormControl;
  layeringFormControl: FormControl;
  stripingFormControl: FormControl;
  exclusiveLockFormControl: FormControl;
  objectMapFormControl: FormControl;
  journalingFormControl: FormControl;
  fastDiffFormControl: FormControl;

  model = new RbdFormModel();
  pools: Array<string> = null;
  dataPools: Array<string> = null;
  features: any;
  featuresList = [];

  routeParamsSubscribe: any;
  pool: string;

  constructor(private route: ActivatedRoute,
              private router: Router,
              private poolService: PoolService,
              private rbdService: RbdService,
              private formatter: FormatterService,
              private toastr: ToastsManager,
              private el: ElementRef,
              private formService: FormService) {
    this.createForm();
    this.features = {
      'deep-flatten': {
        desc: 'Deep flatten',
        requires: null,
        excludes: null,
      },
      'layering': {
        desc: 'Layering',
        requires: null,
        excludes: null,
      },
      'striping': {
        desc: 'Striping',
        helperHtml: `<div>
  <p>
  RBD images are striped over many objects, which are then stored by the
  Ceph distributed object store (RADOS).
  </p>
  <p>Striping features:</p>
  <ul>
    <li>Read and write requests distributed across many nodes</li>
    <li>Prevents single node bottleneck for large or busy RBDs</li>
  </ul>
</div>`,
        requires: null,
        excludes: null,
      },
      'exclusive-lock': {
        desc: 'Exclusive lock',
        requires: null,
        excludes: null,
      },
      'object-map': {
        desc: 'Object map (requires exclusive-lock)',
        requires: 'exclusive-lock',
        excludes: null,
      },
      'journaling': {
        desc: 'Journaling (requires exclusive-lock)',
        requires: 'exclusive-lock',
        excludes: null,
      },
      'fast-diff': {
        desc: 'Fast diff (requires object-map)',
        requires: 'object-map',
        excludes: null,
      }
    };
    for (const key of Object.keys(this.features)) {
      const listItem = this.features[key];
      listItem.key = key;
      this.featuresList.push(listItem);
    }
  }

  createForm() {
    this.defaultFeaturesFormControl = new FormControl(true);
    this.deepFlattenFormControl = new FormControl(false);
    this.layeringFormControl = new FormControl(false);
    this.stripingFormControl = new FormControl(false);
    this.exclusiveLockFormControl = new FormControl(false);
    this.objectMapFormControl = new FormControl({value: false, disabled: true});
    this.journalingFormControl = new FormControl({value: false, disabled: true});
    this.fastDiffFormControl = new FormControl({value: false, disabled: true});
    this.featuresFormGroups = new FormGroup({
      defaultFeatures: this.defaultFeaturesFormControl,
      'deep-flatten': this.deepFlattenFormControl,
      'layering': this.layeringFormControl,
      'striping': this.stripingFormControl,
      'exclusive-lock': this.exclusiveLockFormControl,
      'object-map': this.objectMapFormControl,
      'journaling': this.journalingFormControl,
      'fast-diff': this.fastDiffFormControl,
    }, this.validateFeatures);
    this.rbdForm = new FormGroup({
      name: new FormControl('', {
        validators: [
          Validators.required
        ]
      }),
      useDataPool: new FormControl(false),
      pool: new FormControl(null, {
        validators: [
          Validators.required
        ]
      }),
      dataPool: new FormControl(null),
      size: new FormControl(null, {
        updateOn: 'blur'
      }),
      obj_size: new FormControl(null, {
        validators: [
          Validators.required
        ],
        updateOn: 'blur'
      }),
      features: this.featuresFormGroups,
      stripingUnit: new FormControl(null, {
        updateOn: 'blur'
      }),
      stripingCount: new FormControl(null, {
        validators: [
          Validators.required,
          Validators.min(2)
        ],
        updateOn: 'blur'
      })
    }, this.validateRbdForm(this.formatter));
  }

  ngOnInit() {
    this.routeParamsSubscribe = this.route.params.subscribe((params: { pool: string }) => {
      this.pool = params.pool;
      this.rbdForm.get('pool').setValue(this.pool);
    });
    this.poolService.list(['pool_name', 'type', 'flags_names']).then(resp => {
      const pools = [];
      const dataPools = [];
      for (const pool of resp) {
        if (pool.type === 'replicated') {
          pools.push(pool);
          dataPools.push(pool);
        } else if (pool.type === 'erasure' && pool.flags_names.indexOf('ec_overwrites') !== -1) {
          dataPools.push(pool);
        }
      }
      this.pools = pools;
      this.dataPools = dataPools;
    });
    this.defaultFeaturesFormControl.valueChanges.subscribe((value) => {
      this.watchDataFeatures(null, value);
    });
    this.deepFlattenFormControl.valueChanges.subscribe((value) => {
      this.watchDataFeatures('deep-flatten', value);
    });
    this.layeringFormControl.valueChanges.subscribe((value) => {
      this.watchDataFeatures('layering', value);
    });
    this.stripingFormControl.valueChanges.subscribe((value) => {
      this.watchDataFeatures('striping', value);
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

  validateFeatures(formGroup: FormGroup) {
    if (!formGroup.value.defaultFeatures) {
      const noneSelected = Object.keys(formGroup.value).every((feature) => {
        if (feature !== 'defaultFeatures') {
          return !formGroup.value[feature];
        } else {
          return true;
        }
      });
      if (noneSelected) {
        return {'noFeatureSelected': true};
      }
    }
    return null;
  }

  validateRbdForm(formatter) {
    return (formGroup: FormGroup) => {
      // Data Pool
      const useDataPoolControl = formGroup.get('useDataPool');
      const dataPoolControl = formGroup.get('dataPool');
      let dataPoolControlErrors = null;
      if (useDataPoolControl.value && dataPoolControl.value == null) {
        dataPoolControlErrors = {'required': true};
      }
      dataPoolControl.setErrors(dataPoolControlErrors);
      // Size
      const sizeControl = formGroup.get('size');
      const objectSizeControl = formGroup.get('obj_size');
      let sizeControlErrors = null;
      if (sizeControl.value === null) {
        sizeControlErrors = {'required': true};
      } else if (objectSizeControl.valid) {
        const sizeInBytes = formatter.parseFloat(sizeControl.value, 'b');
        const objectSizeInBytes = formatter.parseFloat(objectSizeControl.value, 'b');
        if (this.stripingFormControl.value) {
          const stripingCountFormControl = formGroup.get('stripingCount');
          if (stripingCountFormControl.valid) {
            if (stripingCountFormControl.value * objectSizeInBytes > sizeInBytes) {
              sizeControlErrors = {'invalidSizeObjectStriping': true};
            }
          }
        } else {
          if (objectSizeInBytes > sizeInBytes) {
            sizeControlErrors = {'invalidSizeObject': true};
          }
        }
      }
      sizeControl.setErrors(sizeControlErrors);
      // Striping Unit
      const stripingUnitControl = formGroup.get('stripingUnit');
      let stripingUnitControlErrors = null;
      if (this.stripingFormControl.value) {
        if (stripingUnitControl.value == null) {
          stripingUnitControlErrors = {'required': true};
        } else if (objectSizeControl.valid) {
          const objectSizeInBytes = formatter.parseFloat(objectSizeControl.value, 'b');
          const stripingUnitInBytes = formatter.parseFloat(stripingUnitControl.value, 'b');
          if (objectSizeInBytes % stripingUnitInBytes !== 0 ||
              stripingUnitInBytes > objectSizeInBytes) {
            stripingUnitControlErrors = {'invalidStripingUnit': true};
          }
        }
      }
      stripingUnitControl.setErrors(stripingUnitControlErrors);
      // Striping Count
      const stripingCountControl = formGroup.get('stripingCount');
      let stripingCountControlErrors = null;
      if (this.stripingFormControl.value) {
        if (stripingCountControl.value == null) {
          stripingCountControlErrors = {'required': true};
        } else if (stripingCountControl.value < 2) {
          stripingCountControlErrors = {'min': true};
        }
      }
      stripingCountControl.setErrors(stripingCountControlErrors);
    };
  }

  deepBoxCheck(key, checked) {
    _.forIn(this.features, (details, feature) => {
      if (details.requires === key) {
        if (checked) {
          this.featuresFormGroups.get(feature).enable();
        } else {
          this.featuresFormGroups.get(feature).disable();
          this.featuresFormGroups.get(feature).setValue(checked);
          this.watchDataFeatures(feature, checked);
          this.deepBoxCheck(feature, checked);
        }
      }
      if (details.excludes === key) {
        if (checked) {
          this.featuresFormGroups.get(feature).disable();
        } else {
          this.featuresFormGroups.get(feature).enable();
        }
      }
    });
  }

  featureFormUpdate(key, checked) {
    if (checked) {
      const required = this.features[key].requires;
      const excluded = this.features[key].excludes;
      if (excluded && this.featuresFormGroups.get(excluded).value ||
        required && !this.featuresFormGroups.get(required).value) {
        this.featuresFormGroups.get(key).setValue(false);
        return;
      }
    }
    this.deepBoxCheck(key, checked);
  }

  watchDataFeatures(key, checked) {
    if (!this.defaultFeaturesFormControl.value && key) {
      this.featureFormUpdate(key, checked);
    }
  }

  createRequest() {
    const request = new RbdFormRequestModel();
    request.pool_name = this.rbdForm.get('pool').value;
    request.name = this.rbdForm.get('name').value;
    request.size = this.formatter.parseFloat(this.rbdForm.get('size').value, 'b');
    request.obj_size = this.formatter.parseFloat(this.rbdForm.get('obj_size').value, 'b');
    if (!this.defaultFeaturesFormControl.value) {
      _.forIn(this.features, (feature) => {
        if (this.featuresFormGroups.get(feature.key).value) {
          request.features.push(feature.key);
        }
      });
    }
    request.stripe_unit = this.formatter.parseFloat(this.rbdForm.get('stripingUnit').value, 'b');
    request.stripe_count = this.rbdForm.get('stripingCount').value;
    request.data_pool = this.rbdForm.get('dataPool').value;
    return request;
  }

  submit() {
    if (this.rbdForm.invalid) {
      this.formService.focusInvalid(this.el);
      return;
    }
    const request = this.createRequest();
    this.rbdService.create(request).then((resp) => {
      this.toastr.success(`RBD <strong>${request.name}</strong> have been created successfully`);
      this.router.navigate([`/block/pool/${this.pool}`]);
    }).catch((resp) => {
      this.toastr.error(this.getErrorMessage(resp.error, request), 'Failted to create RBD image');
    });
  }

  getErrorMessage(error, request) {
    switch (error.errno) {
      case UnixErrno.EEXIST:
        return `RBD name <strong>${request.name}</strong> is already in use.`;
      default:
        return error.detail || '';
    }
  }
}
