import { Component, EventEmitter, Inject, OnInit } from '@angular/core';
import { UntypedFormControl, Validators } from '@angular/forms';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CephServiceService } from '~/app/shared/api/ceph-service.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { TaskMessageService } from '~/app/shared/services/task-message.service';
import { CephServiceSpec, CephServiceSpecUpdate } from '~/app/shared/models/service.interface';

@Component({
  selector: 'cd-nvmeof-gateway-group-edit-modal',
  templateUrl: './nvmeof-gateway-group-edit-modal.component.html',
  styleUrls: ['./nvmeof-gateway-group-edit-modal.component.scss'],
  standalone: false
})
export class NvmeofGatewayGroupEditModalComponent extends CdForm implements OnInit {
  editForm!: CdFormGroup;
  public groupUpdated = new EventEmitter<void>();
  private readonly EDIT_GATEWAY_GROUP_TASK = 'nvmeof/gateway/group/edit';

  constructor(
    private cephServiceService: CephServiceService,
    private notificationService: NotificationService,
    private taskMessageService: TaskMessageService,
    @Inject('gatewayGroup') public gatewayGroup: CephServiceSpec
  ) {
    super();
  }

  ngOnInit(): void {
    this.createForm();
  }

  createForm(): void {
    const enableAuth = this.gatewayGroup.spec?.enable_auth ?? false;
    const enableMtls = this.gatewayGroup.spec?.enable_mtls ?? false;

    this.editForm = new CdFormGroup({
      groupName: new UntypedFormControl(
        { value: this.gatewayGroup.spec?.group || '', disabled: true },
        [Validators.required]
      ),
      enableEncryption: new UntypedFormControl(enableAuth),
      enableMtls: new UntypedFormControl(enableMtls)
    });
  }

  onSubmit(): void {
    if (this.editForm.invalid) {
      return;
    }

    this.loadingStart();

    const formValues = this.editForm.getRawValue();
    const modifiedSpec = this.createServiceSpecPayload(formValues);

    this.cephServiceService.update(modifiedSpec).subscribe({
      next: () => {
        this.notificationService.show(
          NotificationType.success,
          this.taskMessageService.messages[this.EDIT_GATEWAY_GROUP_TASK]?.success({
            group_name: this.gatewayGroup.spec?.group
          }) || $localize`Gateway group updated successfully.`
        );
        this.groupUpdated.emit();
        this.loadingReady();
        this.closeModal();
      },
      error: (e) => {
        this.loadingReady();
        const errorDetail = e?.error?.detail || e?.message || '';
        this.notificationService.show(
          NotificationType.error,
          this.taskMessageService.messages[this.EDIT_GATEWAY_GROUP_TASK]?.failure({
            group_name: this.gatewayGroup.spec?.group
          }) || $localize`Failed to update gateway group.`,
          errorDetail
        );
      }
    });
  }

  private createServiceSpecPayload(formValues: any): CephServiceSpecUpdate {
    const { status, ...modifiedSpec } = this.gatewayGroup;

    if (modifiedSpec.events) {
      delete modifiedSpec.events;
    }

    if (modifiedSpec.spec) {
      modifiedSpec.spec = {
        ...modifiedSpec.spec,
        enable_auth: formValues.enableEncryption,
        enable_mtls: formValues.enableMtls
      };
    }

    return modifiedSpec;
  }
}

// Made with Bob
