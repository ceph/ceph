import { Component, Inject, OnInit } from '@angular/core';
import { UntypedFormControl, Validators } from '@angular/forms';
import { BaseModal } from 'carbon-components-angular';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';

@Component({
  selector: 'cd-nvmeof-edit-host-key-modal',
  templateUrl: './nvmeof-edit-host-key-modal.component.html',
  styleUrls: ['./nvmeof-edit-host-key-modal.component.scss'],
  standalone: false
})
export class NvmeofEditHostKeyModalComponent extends BaseModal implements OnInit {
  editForm: CdFormGroup;
  showConfirmation = false;

  constructor(
    @Inject('subsystemNQN') public subsystemNQN: string,
    @Inject('hostNQN') public hostNQN: string,
    @Inject('group') public group: string,
    @Inject('dhchapKey') public existingDhchapKey: string,
    private nvmeofService: NvmeofService,
    private taskWrapper: TaskWrapperService
  ) {
    super();
  }

  ngOnInit() {
    this.editForm = new CdFormGroup({
      dhchapKey: new UntypedFormControl(this.existingDhchapKey || '', [
        Validators.required,
        CdValidators.base64()
      ])
    });
  }

  onSave() {
    if (this.editForm.invalid) {
      return;
    }
    this.showConfirmation = true;
  }

  goBack() {
    this.showConfirmation = false;
  }

  onSubmit() {
    if (this.editForm.invalid) {
      return;
    }
    const dhchapKey = this.editForm.getValue('dhchapKey');
    this.taskWrapper
      .wrapTaskAroundCall({
        task: new FinishedTask('nvmeof/initiator/edit', {
          nqn: this.subsystemNQN
        }),
        call: this.nvmeofService.updateHostKey(this.subsystemNQN, {
          host_nqn: this.hostNQN,
          dhchap_key: dhchapKey,
          gw_group: this.group
        })
      })
      .subscribe({
        error: () => {
          this.editForm.setErrors({ cdSubmitButton: true });
        },
        complete: () => {
          this.closeModal();
        }
      });
  }
}
