import { Component, Inject, OnInit, Optional } from '@angular/core';
import { AbstractControl, UntypedFormControl, Validators } from '@angular/forms';

import { RbdMirroringService } from '~/app/shared/api/rbd-mirroring.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { PoolEditPeerResponseModel } from './pool-edit-peer-response.model';
import { BaseModal } from 'carbon-components-angular';

@Component({
  selector: 'cd-pool-edit-peer-modal',
  templateUrl: './pool-edit-peer-modal.component.html',
  styleUrls: ['./pool-edit-peer-modal.component.scss']
})
export class PoolEditPeerModalComponent extends BaseModal implements OnInit {
  editPeerForm: CdFormGroup;
  bsConfig = {
    containerClass: 'theme-default'
  };
  pattern: string;

  response: PoolEditPeerResponseModel;

  constructor(
    public actionLabels: ActionLabelsI18n,
    private rbdMirroringService: RbdMirroringService,
    private taskWrapper: TaskWrapperService,

    @Inject('poolName') public poolName: string,
    @Optional() @Inject('peerUUID') public peerUUID = '',
    @Optional() @Inject('mode') public mode = ''
  ) {
    super();
    this.createForm();
  }

  createForm() {
    this.editPeerForm = new CdFormGroup({
      clusterName: new UntypedFormControl('', {
        validators: [Validators.required, this.validateClusterName]
      }),
      clientID: new UntypedFormControl('', {
        validators: [Validators.required, this.validateClientID]
      }),
      monAddr: new UntypedFormControl('', {
        validators: [this.validateMonAddr]
      }),
      key: new UntypedFormControl('', {
        validators: [this.validateKey]
      })
    });
  }

  ngOnInit() {
    this.pattern = `${this.poolName}/${this.peerUUID}`;
    if (this.mode === 'edit') {
      this.rbdMirroringService
        .getPeer(this.poolName, this.peerUUID)
        .subscribe((resp: PoolEditPeerResponseModel) => {
          this.setResponse(resp);
        });
    }
  }

  validateClusterName(control: AbstractControl) {
    if (!control.value.match(/^[\w\-_]*$/)) {
      return { invalidClusterName: { value: control.value } };
    }

    return undefined;
  }

  validateClientID(control: AbstractControl) {
    if (!control.value.match(/^(?!client\.)[\w\-_.]*$/)) {
      return { invalidClientID: { value: control.value } };
    }

    return undefined;
  }

  validateMonAddr(control: AbstractControl) {
    if (!control.value.match(/^[,; ]*([\w.\-_\[\]]+(:[\d]+)?[,; ]*)*$/)) {
      return { invalidMonAddr: { value: control.value } };
    }

    return undefined;
  }

  validateKey(control: AbstractControl) {
    try {
      if (control.value === '' || !!atob(control.value)) {
        return null;
      }
    } catch (error) {}
    return { invalidKey: { value: control.value } };
  }

  setResponse(response: PoolEditPeerResponseModel) {
    this.response = response;
    this.editPeerForm.get('clusterName').setValue(response.cluster_name);
    this.editPeerForm.get('clientID').setValue(response.client_id);
    this.editPeerForm.get('monAddr').setValue(response.mon_host);
    this.editPeerForm.get('key').setValue(response.key);
  }

  update() {
    const request = new PoolEditPeerResponseModel();
    request.cluster_name = this.editPeerForm.getValue('clusterName');
    request.client_id = this.editPeerForm.getValue('clientID');
    request.mon_host = this.editPeerForm.getValue('monAddr');
    request.key = this.editPeerForm.getValue('key');

    let action;
    if (this.mode === 'edit') {
      action = this.taskWrapper.wrapTaskAroundCall({
        task: new FinishedTask('rbd/mirroring/peer/edit', {
          pool_name: this.poolName
        }),
        call: this.rbdMirroringService.updatePeer(this.poolName, this.peerUUID, request)
      });
    } else {
      action = this.taskWrapper.wrapTaskAroundCall({
        task: new FinishedTask('rbd/mirroring/peer/add', {
          pool_name: this.poolName
        }),
        call: this.rbdMirroringService.addPeer(this.poolName, request)
      });
    }

    action.subscribe({
      error: () => this.editPeerForm.setErrors({ cdSubmitButton: true }),
      complete: () => {
        this.rbdMirroringService.refresh();
        this.closeModal();
      }
    });
  }
}
