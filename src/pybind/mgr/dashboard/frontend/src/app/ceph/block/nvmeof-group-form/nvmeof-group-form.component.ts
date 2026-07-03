import { Component, OnInit, ViewChild } from '@angular/core';
import { UntypedFormControl, Validators } from '@angular/forms';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';

import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { NvmeofGatewayNodeComponent } from '../nvmeof-gateway-node/nvmeof-gateway-node.component';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { CephServiceService } from '~/app/shared/api/ceph-service.service';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { Router } from '@angular/router';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { Pool } from '../../pool/pool';
import { CertificateType } from '~/app/shared/models/service.interface';

@Component({
  selector: 'cd-nvmeof-group-form',
  templateUrl: './nvmeof-group-form.component.html',
  styleUrls: ['./nvmeof-group-form.component.scss'],
  standalone: false
})
export class NvmeofGroupFormComponent extends CdForm implements OnInit {
  @ViewChild(NvmeofGatewayNodeComponent) gatewayNodeComponent!: NvmeofGatewayNodeComponent;

  readonly CertificateType = CertificateType;
  permission: Permission;
  groupForm!: CdFormGroup;
  action!: string;
  resource: string;
  group = '';
  pageURL = '';
  pools: Pool[] = [];
  poolsLoading = false;
  hasAvailableNodes = true;

  constructor(
    private authStorageService: AuthStorageService,
    public actionLabels: ActionLabelsI18n,
    private taskWrapperService: TaskWrapperService,
    private cephServiceService: CephServiceService,
    private nvmeofService: NvmeofService,
    private router: Router
  ) {
    super();
    this.permission = this.authStorageService.getPermissions().nvmeof;
    this.resource = $localize`gateway group`;
  }

  ngOnInit() {
    this.action = this.actionLabels.CREATE;
    this.createForm();
  }

  createForm() {
    this.groupForm = new CdFormGroup({
      groupName: new UntypedFormControl(
        null,
        [
          Validators.required,
          (control) => {
            const value = control.value;
            return value && /[^a-zA-Z0-9_-]/.test(value) ? { invalidChars: true } : null;
          }
        ],
        [CdValidators.unique(this.nvmeofService.exists, this.nvmeofService)]
      ),
      unmanaged: new UntypedFormControl(false),
      enableEncryption: new UntypedFormControl(false),
      encryptionConfig: new UntypedFormControl(null),
      encryptionKey: new UntypedFormControl(null),
      enableMtls: new UntypedFormControl(false),
      certificateType: new UntypedFormControl(CertificateType.internal),
      pool: new UntypedFormControl('rbd'),
      custom_sans: new UntypedFormControl([]),
      rootCACert: new UntypedFormControl(null),
      clientCert: new UntypedFormControl(null),
      clientKey: new UntypedFormControl(null),
      serverCert: new UntypedFormControl(null),
      serverKey: new UntypedFormControl(null)
    });

    this.groupForm.get('enableEncryption')?.valueChanges.subscribe((enabled) => {
      // Keep both legacy and new encryption fields in sync.
      if (!enabled) {
        return;
      }
      const encryptionConfigControl = this.groupForm.get('encryptionConfig');
      const encryptionKeyControl = this.groupForm.get('encryptionKey');
      if (!encryptionKeyControl?.value && encryptionConfigControl?.value) {
        encryptionKeyControl.setValue(encryptionConfigControl.value, { emitEvent: false });
      }
      if (!encryptionConfigControl?.value && encryptionKeyControl?.value) {
        encryptionConfigControl.setValue(encryptionKeyControl.value, { emitEvent: false });
      }
    });
  }

  onHostsLoaded(count: number): void {
    this.hasAvailableNodes = count > 0;
  }

  get isCreateDisabled(): boolean {
    if (!this.hasAvailableNodes) {
      return true;
    }
    if (!this.groupForm) {
      return true;
    }
    if (this.groupForm.pending) {
      return true;
    }
    if (this.groupForm.invalid) {
      return true;
    }
    const errors = this.groupForm.errors as { [key: string]: any } | null;
    if (errors && errors.cdSubmitButton) {
      return true;
    }
    if (this.gatewayNodeComponent) {
      const selected = this.gatewayNodeComponent.getSelectedHostnames?.() || [];
      if (selected.length === 0) {
        return true;
      }
    }

    return false;
  }

  onSubmit() {
    if (this.groupForm.invalid) {
      return;
    }

    if (this.groupForm.pending) {
      this.groupForm.setErrors({ cdSubmitButton: true });
      return;
    }

    const formValues = this.groupForm.value;
    const selectedHostnames = this.gatewayNodeComponent?.getSelectedHostnames() || [];
    if (selectedHostnames.length === 0) {
      this.groupForm.setErrors({ cdSubmitButton: true });
      return;
    }
    const taskUrl = `service/${URLVerbs.CREATE}`;
    const serviceId = `${formValues.groupName}`;

    const serviceSpec: Record<string, any> = {
      service_type: 'nvmeof',
      service_id: serviceId,
      group: formValues.groupName,
      placement: {
        hosts: selectedHostnames
      },
      unmanaged: formValues.unmanaged
    };

    if (formValues.enableEncryption) {
      const encryptionKey = formValues.encryptionKey || formValues.encryptionConfig;
      if (encryptionKey) {
        serviceSpec['encryption_key'] = encryptionKey;
      }
    }

    if (formValues.enableMtls) {
      serviceSpec['ssl'] = true;
      serviceSpec['enable_auth'] = true;
      serviceSpec['certificate_source'] =
        formValues.certificateType === CertificateType.internal ? 'cephadm-signed' : 'inline';

      if (formValues.pool) {
        serviceSpec['pool'] = formValues.pool;
        serviceSpec['service_id'] = `${formValues.pool}.${formValues.groupName}`;
      }

      if (
        formValues.certificateType === CertificateType.internal &&
        formValues.custom_sans?.length > 0
      ) {
        serviceSpec['custom_sans'] = formValues.custom_sans;
      }

      if (formValues.certificateType === CertificateType.external) {
        serviceSpec['root_ca_cert'] = formValues.rootCACert;
        serviceSpec['client_cert'] = formValues.clientCert;
        serviceSpec['client_key'] = formValues.clientKey;
        serviceSpec['server_cert'] = formValues.serverCert;
        serviceSpec['server_key'] = formValues.serverKey;
      }
    }

    this.taskWrapperService
      .wrapTaskAroundCall({
        task: new FinishedTask(taskUrl, {
          service_name: `nvmeof.${serviceId}`
        }),
        call: this.cephServiceService.create(serviceSpec)
      })
      .subscribe({
        complete: () => {
          this.goToListView();
        },
        error: () => {
          this.groupForm.setErrors({ cdSubmitButton: true });
        }
      });
  }

  private goToListView() {
    this.router.navigateByUrl('/block/nvmeof/gateways');
  }
  onCertificateTypeChange(type: CertificateType): void {
    this.groupForm.get('certificateType')?.setValue(type);
  }

  onFileUpload(event: Event, controlName: string): void {
    const target = event.target as HTMLInputElement;
    const file = target?.files?.[0];
    const control = this.groupForm.get(controlName);
    if (!file || !control) {
      return;
    }

    const reader = new FileReader();
    reader.onload = () => control.setValue(reader.result);
    reader.readAsText(file, 'utf8');
  }
}
