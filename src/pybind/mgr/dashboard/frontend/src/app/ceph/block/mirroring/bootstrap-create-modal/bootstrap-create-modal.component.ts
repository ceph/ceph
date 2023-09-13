import { Component, OnDestroy, OnInit } from '@angular/core';
import { UntypedFormControl, UntypedFormGroup, ValidatorFn, Validators } from '@angular/forms';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import _ from 'lodash';
import { concat, forkJoin, Subscription } from 'rxjs';
import { last, tap } from 'rxjs/operators';

import { Pool } from '~/app/ceph/pool/pool';
import { RbdMirroringService } from '~/app/shared/api/rbd-mirroring.service';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';

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
    public activeModal: NgbActiveModal,
    private rbdMirroringService: RbdMirroringService,
    private taskWrapper: TaskWrapperService
  ) {
    this.createForm();
  }

  createForm() {
    this.createBootstrapForm = new CdFormGroup({
      siteName: new UntypedFormControl('', {
        validators: [Validators.required]
      }),
      pools: new UntypedFormGroup(
        {},
        {
          validators: [this.validatePools()]
        }
      ),
      token: new UntypedFormControl('', {})
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

      const poolsControl = this.createBootstrapForm.get('pools') as UntypedFormGroup;
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
            new UntypedFormControl({ value: !mirroring_disabled, disabled: !mirroring_disabled })
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
    return (poolsControl: UntypedFormGroup): { [key: string]: any } => {
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
    const poolsControl = this.createBootstrapForm.get('pools') as UntypedFormGroup;
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
