import { Component, OnInit } from '@angular/core';
import {
  AbstractControl,
  AsyncValidatorFn,
  UntypedFormControl,
  ValidationErrors,
  ValidatorFn
} from '@angular/forms';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { Subject } from 'rxjs';

import { Pool } from '~/app/ceph/pool/pool';
import { PoolService } from '~/app/shared/api/pool.service';
import { RbdService } from '~/app/shared/api/rbd.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { NotificationService } from '~/app/shared/services/notification.service';

@Component({
  selector: 'cd-rbd-namespace-form-modal',
  templateUrl: './rbd-namespace-form-modal.component.html',
  styleUrls: ['./rbd-namespace-form-modal.component.scss']
})
export class RbdNamespaceFormModalComponent implements OnInit {
  poolPermission: Permission;
  pools: Array<Pool> = null;
  pool: string;
  namespace: string;

  namespaceForm: CdFormGroup;

  editing = false;

  public onSubmit: Subject<void>;

  constructor(
    public activeModal: NgbActiveModal,
    public actionLabels: ActionLabelsI18n,
    private authStorageService: AuthStorageService,
    private notificationService: NotificationService,
    private poolService: PoolService,
    private rbdService: RbdService
  ) {
    this.poolPermission = this.authStorageService.getPermissions().pool;
    this.createForm();
  }

  createForm() {
    this.namespaceForm = new CdFormGroup(
      {
        pool: new UntypedFormControl(''),
        namespace: new UntypedFormControl('')
      },
      this.validator(),
      this.asyncValidator()
    );
  }

  validator(): ValidatorFn {
    return (control: AbstractControl) => {
      const poolCtrl = control.get('pool');
      const namespaceCtrl = control.get('namespace');
      let poolErrors = null;
      if (!poolCtrl.value) {
        poolErrors = { required: true };
      }
      poolCtrl.setErrors(poolErrors);
      let namespaceErrors = null;
      if (!namespaceCtrl.value) {
        namespaceErrors = { required: true };
      }
      namespaceCtrl.setErrors(namespaceErrors);
      return null;
    };
  }

  asyncValidator(): AsyncValidatorFn {
    return (control: AbstractControl): Promise<ValidationErrors | null> => {
      return new Promise((resolve) => {
        const poolCtrl = control.get('pool');
        const namespaceCtrl = control.get('namespace');
        this.rbdService.listNamespaces(poolCtrl.value).subscribe((namespaces: any[]) => {
          if (namespaces.some((ns) => ns.namespace === namespaceCtrl.value)) {
            const error = { namespaceExists: true };
            namespaceCtrl.setErrors(error);
            resolve(error);
          } else {
            resolve(null);
          }
        });
      });
    };
  }

  ngOnInit() {
    this.onSubmit = new Subject();

    if (this.poolPermission.read) {
      this.poolService.list(['pool_name', 'type', 'application_metadata']).then((resp) => {
        const pools: Pool[] = [];
        for (const pool of resp) {
          if (this.rbdService.isRBDPool(pool) && pool.type === 'replicated') {
            pools.push(pool);
          }
        }
        this.pools = pools;
        if (this.pools.length === 1) {
          const poolName = this.pools[0]['pool_name'];
          this.namespaceForm.get('pool').setValue(poolName);
        }
      });
    }
  }

  submit() {
    const pool = this.namespaceForm.getValue('pool');
    const namespace = this.namespaceForm.getValue('namespace');
    const finishedTask = new FinishedTask();
    finishedTask.name = 'rbd/namespace/create';
    finishedTask.metadata = {
      pool: pool,
      namespace: namespace
    };
    this.rbdService
      .createNamespace(pool, namespace)
      .toPromise()
      .then(() => {
        this.notificationService.show(
          NotificationType.success,
          $localize`Created namespace '${pool}/${namespace}'`
        );
        this.activeModal.close();
        this.onSubmit.next();
      })
      .catch(() => {
        this.namespaceForm.setErrors({ cdSubmitButton: true });
      });
  }
}
