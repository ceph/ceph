import { Component, OnInit } from '@angular/core';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

import { PoolService } from '../../../shared/api/pool.service';
import { RbdService } from '../../../shared/api/rbd.service';
import { CdFormBuilder } from '../../../shared/forms/cd-form-builder';
import { CdFormGroup } from '../../../shared/forms/cd-form-group';
import { FinishedTask } from '../../../shared/models/finished-task';
import { Permission } from '../../../shared/models/permissions';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { TaskWrapperService } from '../../../shared/services/task-wrapper.service';
import { Pool } from '../../pool/pool';

@Component({
  selector: 'cd-rbd-trash-purge-modal',
  templateUrl: './rbd-trash-purge-modal.component.html',
  styleUrls: ['./rbd-trash-purge-modal.component.scss']
})
export class RbdTrashPurgeModalComponent implements OnInit {
  poolPermission: Permission;
  purgeForm: CdFormGroup;
  pools: any[];

  constructor(
    private authStorageService: AuthStorageService,
    private rbdService: RbdService,
    public activeModal: NgbActiveModal,
    private fb: CdFormBuilder,
    private poolService: PoolService,
    private taskWrapper: TaskWrapperService
  ) {
    this.poolPermission = this.authStorageService.getPermissions().pool;
  }

  createForm() {
    this.purgeForm = this.fb.group({
      poolName: ''
    });
  }

  ngOnInit() {
    if (this.poolPermission.read) {
      this.poolService.list(['pool_name', 'application_metadata']).then((resp) => {
        this.pools = resp
          .filter((pool: Pool) => pool.application_metadata.includes('rbd'))
          .map((pool: Pool) => pool.pool_name);
      });
    }

    this.createForm();
  }

  purge() {
    const poolName = this.purgeForm.getValue('poolName') || '';
    this.taskWrapper
      .wrapTaskAroundCall({
        task: new FinishedTask('rbd/trash/purge', {
          pool_name: poolName
        }),
        call: this.rbdService.purgeTrash(poolName)
      })
      .subscribe({
        error: () => {
          this.purgeForm.setErrors({ cdSubmitButton: true });
        },
        complete: () => {
          this.activeModal.close();
        }
      });
  }
}
