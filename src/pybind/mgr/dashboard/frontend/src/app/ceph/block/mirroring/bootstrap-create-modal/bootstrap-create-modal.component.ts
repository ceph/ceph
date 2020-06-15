import { Component, OnDestroy, OnInit } from '@angular/core';
import { FormControl, FormGroup, ValidatorFn, Validators } from '@angular/forms';

import * as _ from 'lodash';
import { BsModalRef } from 'ngx-bootstrap/modal';
import { concat, forkJoin, Subscription } from 'rxjs';
import { last, tap } from 'rxjs/operators';

import { RbdMirroringService } from '../../../../shared/api/rbd-mirroring.service';
import { CdFormGroup } from '../../../../shared/forms/cd-form-group';
import { FinishedTask } from '../../../../shared/models/finished-task';
import { TaskWrapperService } from '../../../../shared/services/task-wrapper.service';
import { Pool } from '../../../pool/pool';

@Component({
  selector: 'cd-bootstrap-create-modal',
  templateUrl: './bootstrap-create-modal.component.html',
  styleUrls: ['./bootstrap-create-modal.component.scss']
})
export class BootstrapCreateModalComponent implements OnDestroy, OnInit {
  siteName: string;
  pools: any[] = [];
  token: string;

  subs: Subscription;

  createBootstrapForm: CdFormGroup;

  constructor(
    public modalRef: BsModalRef,
    private rbdMirroringService: RbdMirroringService,
    private taskWrapper: TaskWrapperService
  ) {
    this.createForm();
  }

  createForm() {
    this.createBootstrapForm = new CdFormGroup({
      siteName: new FormControl('', {
        validators: [Validators.required]
      }),
      pools: new FormGroup(
        {},
        {
          validators: [this.validatePools()]
        }
      ),
      token: new FormControl('', {})
    });
  }

  ngOnInit() {
    this.createBootstrapForm.get('siteName').setValue(this.siteName);
    this.rbdMirroringService.getSiteName().subscribe((response: any) => {
      this.createBootstrapForm.get('siteName').setValue(response.site_name);
    });

    this.subs = this.rbdMirroringService.subscribeSummary((data) => {
      const pools = data.content_data.pools;
      this.pools = pools.reduce((acc: any[], pool: Pool) => {
        acc.push({
          name: pool['name'],
          mirror_mode: pool['mirror_mode']
        });
        return acc;
      }, []);

      const poolsControl = this.createBootstrapForm.get('pools') as FormGroup;
      _.each(this.pools, (pool) => {
        const poolName = pool['name'];
        const mirroring_disabled = pool['mirror_mode'] === 'disabled';
        const control = poolsControl.controls[poolName];
        if (control) {
          if (mirroring_disabled && control.disabled) {
            control.enable();
          } else if (!mirroring_disabled && control.enabled) {
            control.disable();
            control.setValue(true);
          }
        } else {
          poolsControl.addControl(
            poolName,
            new FormControl({ value: !mirroring_disabled, disabled: !mirroring_disabled })
          );
        }
      });
    });
  }

  ngOnDestroy() {
    if (this.subs) {
      this.subs.unsubscribe();
    }
  }

  validatePools(): ValidatorFn {
    return (poolsControl: FormGroup): { [key: string]: any } => {
      let checkedCount = 0;
      _.each(poolsControl.controls, (control) => {
        if (control.value === true) {
          ++checkedCount;
        }
      });

      if (checkedCount > 0) {
        return null;
      }

      return { requirePool: true };
    };
  }

  generate() {
    this.createBootstrapForm.get('token').setValue('');

    let bootstrapPoolName = '';
    const poolNames: string[] = [];
    const poolsControl = this.createBootstrapForm.get('pools') as FormGroup;
    _.each(poolsControl.controls, (control, poolName) => {
      if (control.value === true) {
        bootstrapPoolName = poolName;
        if (!control.disabled) {
          poolNames.push(poolName);
        }
      }
    });

    const poolModeRequest = {
      mirror_mode: 'image'
    };

    const apiActionsObs = concat(
      this.rbdMirroringService.setSiteName(this.createBootstrapForm.getValue('siteName')),
      forkJoin(
        poolNames.map((poolName) => this.rbdMirroringService.updatePool(poolName, poolModeRequest))
      ),
      this.rbdMirroringService
        .createBootstrapToken(bootstrapPoolName)
        .pipe(tap((data: any) => this.createBootstrapForm.get('token').setValue(data['token'])))
    ).pipe(last());

    const finishHandler = () => {
      this.rbdMirroringService.refresh();
      this.createBootstrapForm.setErrors({ cdSubmitButton: true });
    };

    const taskObs = this.taskWrapper.wrapTaskAroundCall({
      task: new FinishedTask('rbd/mirroring/bootstrap/create', {}),
      call: apiActionsObs
    });
    taskObs.subscribe({ error: finishHandler, complete: finishHandler });
  }
}
