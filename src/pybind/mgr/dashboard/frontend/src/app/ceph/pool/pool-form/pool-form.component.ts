import { Component, OnInit } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';

import * as _ from 'lodash';
import { forkJoin } from 'rxjs';

import { ErasureCodeProfileService } from '../../../shared/api/erasure-code-profile.service';
import { PoolService } from '../../../shared/api/pool.service';
import { CdFormGroup } from '../../../shared/forms/cd-form-group';
import { CdValidators } from '../../../shared/forms/cd-validators';
import { CrushRule } from '../../../shared/models/crush-rule';
import { CrushStep } from '../../../shared/models/crush-step';
import { ErasureCodeProfile } from '../../../shared/models/erasure-code-profile';
import { FinishedTask } from '../../../shared/models/finished-task';
import { Permission } from '../../../shared/models/permissions';
import { DimlessBinaryPipe } from '../../../shared/pipes/dimless-binary.pipe';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { FormatterService } from '../../../shared/services/formatter.service';
import { TaskWrapperService } from '../../../shared/services/task-wrapper.service';
import { Pool } from '../pool';
import { PoolFormData } from './pool-form-data';
import { PoolFormInfo } from './pool-form-info';

@Component({
  selector: 'cd-pool-form',
  templateUrl: './pool-form.component.html',
  styleUrls: ['./pool-form.component.scss']
})
export class PoolFormComponent implements OnInit {
  permission: Permission;
  form: CdFormGroup;
  compressionForm: CdFormGroup;
  ecProfiles: ErasureCodeProfile[];
  info: PoolFormInfo;
  routeParamsSubscribe: any;
  editing = false;
  data = new PoolFormData();
  externalPgChange = false;
  current = {
    rules: []
  };

  constructor(
    private dimlessBinaryPipe: DimlessBinaryPipe,
    private route: ActivatedRoute,
    private router: Router,
    private poolService: PoolService,
    private authStorageService: AuthStorageService,
    private formatter: FormatterService,
    private taskWrapper: TaskWrapperService,
    private ecpService: ErasureCodeProfileService
  ) {
    this.editing = this.router.url.startsWith('/pool/edit');
    this.authenticate();
    this.createForms();
  }

  authenticate() {
    this.permission = this.authStorageService.getPermissions().pool;
    if (
      !this.permission.read ||
      ((!this.permission.update && this.editing) || (!this.permission.create && !this.editing))
    ) {
      this.router.navigate(['/404']);
    }
  }

  private createForms() {
    this.compressionForm = new CdFormGroup({
      mode: new FormControl('none'),
      algorithm: new FormControl(''),
      minBlobSize: new FormControl('', {
        updateOn: 'blur'
      }),
      maxBlobSize: new FormControl('', {
        updateOn: 'blur'
      }),
      ratio: new FormControl('', {
        updateOn: 'blur'
      })
    });
    this.form = new CdFormGroup(
      {
        name: new FormControl('', {
          validators: [
            Validators.pattern('[A-Za-z0-9_-]+'),
            Validators.required,
            CdValidators.custom(
              'uniqueName',
              (value) => this.info && this.info.pool_names.indexOf(value) !== -1
            )
          ]
        }),
        poolType: new FormControl('', {
          validators: [Validators.required]
        }),
        crushRule: new FormControl(null, {
          validators: [
            CdValidators.custom(
              'tooFewOsds',
              (rule) => this.info && rule && this.info.osd_count < rule.min_size
            )
          ]
        }),
        size: new FormControl('', {
          updateOn: 'blur'
        }),
        erasureProfile: new FormControl(null),
        pgNum: new FormControl('', {
          validators: [Validators.required, Validators.min(1)]
        }),
        ecOverwrites: new FormControl(false),
        compression: this.compressionForm
      },
      CdValidators.custom('form', () => null)
    );
  }

  ngOnInit() {
    forkJoin(this.poolService.getInfo(), this.ecpService.list()).subscribe(
      (data: [PoolFormInfo, ErasureCodeProfile[]]) => {
        this.initInfo(data[0]);
        this.initEcp(data[1]);
        if (this.editing) {
          this.initEditMode();
        }
        this.listenToChanges();
        this.setComplexValidators();
      }
    );
  }

  private initInfo(info: PoolFormInfo) {
    info.compression_algorithms = info.compression_algorithms.filter((m) => m.length > 0);
    this.info = info;
  }

  private initEcp(ecProfiles: ErasureCodeProfile[]) {
    if (ecProfiles.length === 1) {
      const control = this.form.get('erasureProfile');
      control.setValue(ecProfiles[0]);
      control.disable();
    }
    this.ecProfiles = ecProfiles;
  }

  private initEditMode() {
    this.disableForEdit();
    this.routeParamsSubscribe = this.route.params.subscribe((param: { name: string }) =>
      this.poolService.get(param.name).subscribe((pool: Pool) => {
        this.data.pool = pool;
        this.initEditFormData(pool);
      })
    );
  }

  private disableForEdit() {
    ['name', 'poolType', 'crushRule', 'size', 'erasureProfile', 'ecOverwrites'].forEach(
      (controlName) => this.form.get(controlName).disable()
    );
  }

  private initEditFormData(pool: Pool) {
    const transform = {
      name: 'pool_name',
      poolType: 'type',
      crushRule: (p) =>
        this.info['crush_rules_' + p.type].find(
          (rule: CrushRule) => rule.rule_name === p.crush_rule
        ),
      size: 'size',
      erasureProfile: (p) => this.ecProfiles.find((ecp) => ecp.name === p.erasure_code_profile),
      pgNum: 'pg_num',
      ecOverwrites: (p) => p.flags_names.includes('ec_overwrites'),
      mode: 'options.compression_mode',
      algorithm: 'options.compression_algorithm',
      minBlobSize: (p) => this.dimlessBinaryPipe.transform(p.options.compression_min_blob_size),
      maxBlobSize: (p) => this.dimlessBinaryPipe.transform(p.options.compression_max_blob_size),
      ratio: 'options.compression_required_ratio'
    };
    Object.keys(transform).forEach((key) => {
      const attrib = transform[key];
      const value = _.isFunction(attrib) ? attrib(pool) : _.get(pool, attrib);
      if (!_.isUndefined(value) && value !== '') {
        this.form.silentSet(key, value);
      }
    });
    this.data.applications.selected = pool.application_metadata;
  }

  private listenToChanges() {
    this.listenToChangesDuringAddEdit();
    if (!this.editing) {
      this.listenToChangesDuringAdd();
    }
  }

  private listenToChangesDuringAddEdit() {
    this.form.get('pgNum').valueChanges.subscribe((pgs) => {
      const change = pgs - this.data.pgs;
      if (Math.abs(change) === 1) {
        this.pgUpdate(undefined, change);
      }
    });
  }

  private listenToChangesDuringAdd() {
    this.form.get('poolType').valueChanges.subscribe((poolType) => {
      this.form.get('size').updateValueAndValidity();
      this.rulesChange();
      if (poolType === 'replicated') {
        this.replicatedRuleChange();
      }
      this.pgCalc();
    });
    this.form.get('crushRule').valueChanges.subscribe(() => {
      if (this.form.getValue('poolType') === 'replicated') {
        this.replicatedRuleChange();
      }
      this.pgCalc();
    });
    this.form.get('size').valueChanges.subscribe(() => {
      this.pgCalc();
    });
    this.form.get('erasureProfile').valueChanges.subscribe(() => {
      this.pgCalc();
    });
    this.form.get('mode').valueChanges.subscribe(() => {
      ['minBlobSize', 'maxBlobSize', 'ratio'].forEach((name) =>
        this.form.get(name).updateValueAndValidity()
      );
    });
    this.form.get('minBlobSize').valueChanges.subscribe(() => {
      this.form.get('maxBlobSize').updateValueAndValidity({ emitEvent: false });
    });
    this.form.get('maxBlobSize').valueChanges.subscribe(() => {
      this.form.get('minBlobSize').updateValueAndValidity({ emitEvent: false });
    });
  }

  private rulesChange() {
    const poolType = this.form.getValue('poolType');
    if (!poolType || !this.info) {
      this.current.rules = [];
      return;
    }
    const rules = this.info['crush_rules_' + poolType] || [];
    const control = this.form.get('crushRule');
    if (rules.length === 1) {
      control.setValue(rules[0]);
      control.disable();
    } else {
      control.setValue(null);
      control.enable();
    }
    this.current.rules = rules;
  }

  private replicatedRuleChange() {
    if (this.form.getValue('poolType') !== 'replicated') {
      return;
    }
    const control = this.form.get('size');
    let size = this.form.getValue('size') || 3;
    const min = this.getMinSize();
    const max = this.getMaxSize();
    if (size < min) {
      size = min;
    } else if (size > max) {
      size = max;
    }
    if (size !== control.value) {
      this.form.silentSet('size', size);
    }
  }

  getMinSize(): number {
    if (!this.info || this.info.osd_count < 1) {
      return;
    }
    const rule = this.form.getValue('crushRule');
    if (rule) {
      return rule.min_size;
    }
    return 1;
  }

  getMaxSize(): number {
    if (!this.info || this.info.osd_count < 1) {
      return;
    }
    const osds: number = this.info.osd_count;
    if (this.form.getValue('crushRule')) {
      const max: number = this.form.get('crushRule').value.max_size;
      if (max < osds) {
        return max;
      }
    }
    return osds;
  }

  private pgCalc() {
    const poolType = this.form.getValue('poolType');
    if (!this.info || this.form.get('pgNum').dirty || !poolType) {
      return;
    }
    const pgMax = this.info.osd_count * 100;
    const pgs =
      poolType === 'replicated' ? this.replicatedPgCalc(pgMax) : this.erasurePgCalc(pgMax);
    if (!pgs) {
      return;
    }
    const oldValue = this.data.pgs;
    this.pgUpdate(pgs);
    const newValue = this.data.pgs;
    if (!this.externalPgChange) {
      this.externalPgChange = oldValue !== newValue;
    }
  }

  private replicatedPgCalc(pgs): number {
    const sizeControl = this.form.get('size');
    const size = sizeControl.value;
    if (sizeControl.valid && size > 0) {
      return pgs / size;
    }
  }

  private erasurePgCalc(pgs): number {
    const ecpControl = this.form.get('erasureProfile');
    const ecp = ecpControl.value;
    if ((ecpControl.valid || ecpControl.disabled) && ecp) {
      return pgs / (ecp.k + ecp.m);
    }
  }

  private pgUpdate(pgs?, jump?) {
    pgs = _.isNumber(pgs) ? pgs : this.form.getValue('pgNum');
    if (pgs < 1) {
      pgs = 1;
    }
    let power = Math.round(Math.log(pgs) / Math.log(2));
    if (_.isNumber(jump)) {
      power += jump;
    }
    if (power < 0) {
      power = 0;
    }
    pgs = Math.pow(2, power); // Set size the nearest accurate size.
    this.data.pgs = pgs;
    this.form.silentSet('pgNum', pgs);
  }

  private setComplexValidators() {
    if (this.editing) {
      this.form
        .get('pgNum')
        .setValidators(
          CdValidators.custom('noDecrease', (pgs) => this.data.pool && pgs < this.data.pool.pg_num)
        );
    } else {
      CdValidators.validateIf(
        this.form.get('size'),
        () => this.form.get('poolType').value === 'replicated',
        [
          CdValidators.custom(
            'min',
            (value) => this.form.getValue('size') && value < this.getMinSize()
          ),
          CdValidators.custom(
            'max',
            (value) => this.form.getValue('size') && this.getMaxSize() < value
          )
        ]
      );
    }
    this.setCompressionValidators();
  }

  private setCompressionValidators() {
    CdValidators.validateIf(this.form.get('minBlobSize'), () => this.activatedCompression(), [
      Validators.min(0),
      CdValidators.custom('maximum', (size) =>
        this.compareBlobSize(size, this.form.getValue('maxBlobSize'))
      )
    ]);
    CdValidators.validateIf(this.form.get('maxBlobSize'), () => this.activatedCompression(), [
      Validators.min(0),
      CdValidators.custom('minimum', (size) =>
        this.compareBlobSize(this.form.getValue('minBlobSize'), size)
      )
    ]);
    CdValidators.validateIf(this.form.get('ratio'), () => this.activatedCompression(), [
      Validators.min(0),
      Validators.max(1)
    ]);
  }

  private compareBlobSize(minimum, maximum) {
    return Boolean(
      minimum && maximum && this.formatter.toBytes(minimum) >= this.formatter.toBytes(maximum)
    );
  }

  activatedCompression() {
    return this.form.getValue('mode') && this.form.get('mode').value.toLowerCase() !== 'none';
  }

  pgKeyUp($e) {
    const key = $e.key;
    const included = (arr: string[]): number => (arr.indexOf(key) !== -1 ? 1 : 0);
    const jump = included(['ArrowUp', '+']) - included(['ArrowDown', '-']);
    if (jump) {
      this.pgUpdate(undefined, jump);
    }
  }

  describeCrushStep(step: CrushStep) {
    return [
      step.op.replace('_', ' '),
      step.item_name || '',
      step.type ? step.num + ' type ' + step.type : ''
    ].join(' ');
  }

  submit() {
    if (this.form.invalid) {
      this.form.setErrors({ cdSubmitButton: true });
      return;
    }
    const pool = {};
    this.extendByItemsForSubmit(pool, [
      { api: 'pool', name: 'name', edit: true },
      { api: 'pool_type', name: 'poolType' },
      { api: 'pg_num', name: 'pgNum', edit: true },
      this.form.getValue('poolType') === 'replicated'
        ? { api: 'size', name: 'size' }
        : { api: 'erasure_code_profile', name: 'erasureProfile', attr: 'name' },
      { api: 'rule_name', name: 'crushRule', attr: 'rule_name' }
    ]);
    if (this.info.is_all_bluestore) {
      this.extendByItemForSubmit(pool, {
        api: 'flags',
        name: 'ecOverwrites',
        fn: () => ['ec_overwrites']
      });
      if (this.form.getValue('mode')) {
        this.extendByItemsForSubmit(pool, [
          {
            api: 'compression_mode',
            name: 'mode',
            edit: true,
            fn: (value) => this.activatedCompression() && value
          },
          { api: 'compression_algorithm', name: 'algorithm', edit: true },
          {
            api: 'compression_min_blob_size',
            name: 'minBlobSize',
            fn: this.formatter.toBytes,
            edit: true
          },
          {
            api: 'compression_max_blob_size',
            name: 'maxBlobSize',
            fn: this.formatter.toBytes,
            edit: true
          },
          { api: 'compression_required_ratio', name: 'ratio', edit: true }
        ]);
      }
    }
    const apps = this.data.applications.selected;
    if (apps.length > 0 || this.editing) {
      pool['application_metadata'] = apps;
    }
    this.triggerApiTask(pool);
  }

  private extendByItemsForSubmit(pool, items: any[]) {
    items.forEach((item) => this.extendByItemForSubmit(pool, item));
  }

  private extendByItemForSubmit(
    pool,
    {
      api,
      name,
      attr,
      fn,
      edit
    }: {
      api: string;
      name: string;
      attr?: string;
      fn?: Function;
      edit?: boolean;
    }
  ) {
    if (this.editing && !edit) {
      return;
    }
    const value = this.form.getValue(name);
    const apiValue = fn ? fn(value) : attr ? _.get(value, attr) : value;
    if (!value || !apiValue) {
      return;
    }
    pool[api] = apiValue;
  }

  private triggerApiTask(pool) {
    this.taskWrapper
      .wrapTaskAroundCall({
        task: new FinishedTask('pool/' + (this.editing ? 'edit' : 'create'), {
          pool_name: pool.pool
        }),
        call: this.poolService[this.editing ? 'update' : 'create'](pool)
      })
      .subscribe(
        undefined,
        (resp) => {
          if (_.isObject(resp.error) && resp.error.code === '34') {
            this.form.get('pgNum').setErrors({ '34': true });
          }
          this.form.setErrors({ cdSubmitButton: true });
        },
        () => this.router.navigate(['/pool'])
      );
  }
}
