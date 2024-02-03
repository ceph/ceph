import { Component, OnDestroy, OnInit } from '@angular/core';
import { UntypedFormControl, UntypedFormGroup, ValidatorFn, Validators } from '@angular/forms';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import _ from 'lodash';
import { concat, forkJoin, Observable, Subscription } from 'rxjs';
import { last } from 'rxjs/operators';

import { Pool } from '~/app/ceph/pool/pool';
import { RbdMirroringService } from '~/app/shared/api/rbd-mirroring.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';

@Component({
  selector: 'cd-bootstrap-import-modal',
  templateUrl: './bootstrap-import-modal.component.html',
  styleUrls: ['./bootstrap-import-modal.component.scss']
})
export class BootstrapImportModalComponent implements OnInit, OnDestroy {
  siteName: string;
  pools: any[] = [];
  token: string;

  subs: Subscription;

  importBootstrapForm: CdFormGroup;

  directions: Array<any> = [
    { key: 'rx-tx', desc: 'Bidirectional' },
    { key: 'rx', desc: 'Unidirectional (receive-only)' }
  ];

  constructor(
    public activeModal: NgbActiveModal,
    public actionLabels: ActionLabelsI18n,
    private rbdMirroringService: RbdMirroringService,
    private taskWrapper: TaskWrapperService
  ) {
    this.createForm();
  }

  createForm() {
    this.importBootstrapForm = new CdFormGroup({
      siteName: new UntypedFormControl('', {
        validators: [Validators.required]
      }),
      direction: new UntypedFormControl('rx-tx', {}),
      pools: new UntypedFormGroup(
        {},
        {
          validators: [this.validatePools()]
        }
      ),
      token: new UntypedFormControl('', {
        validators: [Validators.required, this.validateToken()]
      })
    });
  }

  ngOnInit() {
    this.rbdMirroringService.getSiteName().subscribe((response: any) => {
      this.importBootstrapForm.get('siteName').setValue(response.site_name);
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

      const poolsControl = this.importBootstrapForm.get('pools') as UntypedFormGroup;
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

  validateToken(): ValidatorFn {
    return (token: UntypedFormControl): { [key: string]: any } => {
      try {
        if (JSON.parse(atob(token.value))) {
          return null;
        }
      } catch (error) {}
      return { invalidToken: true };
    };
  }

  import() {
    const bootstrapPoolNames: string[] = [];
    const poolNames: string[] = [];
    const poolsControl = this.importBootstrapForm.get('pools') as UntypedFormGroup;
    _.each(poolsControl.controls, (control, poolName) => {
      if (control.value === true) {
        bootstrapPoolNames.push(poolName);
        if (!control.disabled) {
          poolNames.push(poolName);
        }
      }
    });

    const poolModeRequest = {
      mirror_mode: 'image'
    };

    let apiActionsObs: Observable<any> = concat(
      this.rbdMirroringService.setSiteName(this.importBootstrapForm.getValue('siteName')),
      forkJoin(
        poolNames.map((poolName) => this.rbdMirroringService.updatePool(poolName, poolModeRequest))
      )
    );

    apiActionsObs = bootstrapPoolNames
      .reduce((obs, poolName) => {
        return concat(
          obs,
          this.rbdMirroringService.importBootstrapToken(
            poolName,
            this.importBootstrapForm.getValue('direction'),
            this.importBootstrapForm.getValue('token')
          )
        );
      }, apiActionsObs)
      .pipe(last());

    const finishHandler = () => {
      this.rbdMirroringService.refresh();
      this.importBootstrapForm.setErrors({ cdSubmitButton: true });
    };

    const taskObs = this.taskWrapper.wrapTaskAroundCall({
      task: new FinishedTask('rbd/mirroring/bootstrap/import', {}),
      call: apiActionsObs
    });
    taskObs.subscribe({
      error: finishHandler,
      complete: () => {
        finishHandler();
        this.activeModal.close();
      }
    });
  }
}
