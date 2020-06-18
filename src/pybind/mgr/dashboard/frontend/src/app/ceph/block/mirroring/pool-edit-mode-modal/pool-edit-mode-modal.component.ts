import { Component, OnDestroy, OnInit } from '@angular/core';
import { AbstractControl, FormControl, Validators } from '@angular/forms';

import { I18n } from '@ngx-translate/i18n-polyfill';
import { BsModalRef } from 'ngx-bootstrap/modal';
import { Subscription } from 'rxjs';

import { RbdMirroringService } from '../../../../shared/api/rbd-mirroring.service';
import { CdFormGroup } from '../../../../shared/forms/cd-form-group';
import { FinishedTask } from '../../../../shared/models/finished-task';
import { TaskWrapperService } from '../../../../shared/services/task-wrapper.service';
import { PoolEditModeResponseModel } from './pool-edit-mode-response.model';

@Component({
  selector: 'cd-pool-edit-mode-modal',
  templateUrl: './pool-edit-mode-modal.component.html',
  styleUrls: ['./pool-edit-mode-modal.component.scss']
})
export class PoolEditModeModalComponent implements OnInit, OnDestroy {
  poolName: string;

  subs: Subscription;

  editModeForm: CdFormGroup;
  bsConfig = {
    containerClass: 'theme-default'
  };
  pattern: string;

  response: PoolEditModeResponseModel;
  peerExists = false;

  mirrorModes: Array<{ id: string; name: string }> = [
    { id: 'disabled', name: this.i18n('Disabled') },
    { id: 'pool', name: this.i18n('Pool') },
    { id: 'image', name: this.i18n('Image') }
  ];

  constructor(
    public modalRef: BsModalRef,
    private i18n: I18n,
    private rbdMirroringService: RbdMirroringService,
    private taskWrapper: TaskWrapperService
  ) {
    this.createForm();
  }

  createForm() {
    this.editModeForm = new CdFormGroup({
      mirrorMode: new FormControl('', {
        validators: [Validators.required, this.validateMode.bind(this)]
      })
    });
  }

  ngOnInit() {
    this.pattern = `${this.poolName}`;
    this.rbdMirroringService.getPool(this.poolName).subscribe((resp: PoolEditModeResponseModel) => {
      this.setResponse(resp);
    });

    this.subs = this.rbdMirroringService.subscribeSummary((data) => {
      this.peerExists = false;
      const poolData = data.content_data.pools;
      const pool = poolData.find((o: any) => this.poolName === o['name']);
      this.peerExists = pool && pool['peer_uuids'].length;
    });
  }

  ngOnDestroy(): void {
    this.subs.unsubscribe();
  }

  validateMode(control: AbstractControl) {
    if (control.value === 'disabled' && this.peerExists) {
      return { cannotDisable: { value: control.value } };
    }
    return null;
  }

  setResponse(response: PoolEditModeResponseModel) {
    this.editModeForm.get('mirrorMode').setValue(response.mirror_mode);
  }

  update() {
    const request = new PoolEditModeResponseModel();
    request.mirror_mode = this.editModeForm.getValue('mirrorMode');

    const action = this.taskWrapper.wrapTaskAroundCall({
      task: new FinishedTask('rbd/mirroring/pool/edit', {
        pool_name: this.poolName
      }),
      call: this.rbdMirroringService.updatePool(this.poolName, request)
    });

    action.subscribe({
      error: () => this.editModeForm.setErrors({ cdSubmitButton: true }),
      complete: () => {
        this.rbdMirroringService.refresh();
        this.modalRef.hide();
      }
    });
  }
}
