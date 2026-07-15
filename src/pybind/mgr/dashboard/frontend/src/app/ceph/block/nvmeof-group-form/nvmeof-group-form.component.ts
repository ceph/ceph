import { Component, OnInit, ViewChild } from '@angular/core';
import { UntypedFormControl, Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';

import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { NvmeofGatewayNodeComponent } from '../nvmeof-gateway-node/nvmeof-gateway-node.component';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { CephServiceService } from '~/app/shared/api/ceph-service.service';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
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
  hasAvailableNodes = true;
  editing = false;
  gatewayGroupName = '';
  existingServiceData: any = null;

  constructor(
    private authStorageService: AuthStorageService,
    public actionLabels: ActionLabelsI18n,
    private taskWrapperService: TaskWrapperService,
    private cephServiceService: CephServiceService,
    private nvmeofService: NvmeofService,
    private router: Router,
    private route: ActivatedRoute
  ) {
    super();
    this.permission = this.authStorageService.getPermissions().nvmeof;
    this.resource = $localize`gateway group`;
  }

  ngOnInit() {
    this.createForm();

    this.route.params.subscribe((params) => {
      if (params['name']) {
        this.editing = true;
        this.gatewayGroupName = params['name'];
        this.action = this.actionLabels.EDIT;
        this.loadGatewayGroupData(params['name']);
      } else {
        this.editing = false;
        this.action = this.actionLabels.CREATE;
      }
    });
  }

  createForm() {
    const groupNameValidators = [
      Validators.required,
      (control: any) => {
        const value = control.value;
        return value && /[^a-zA-Z0-9_-]/.test(value) ? { invalidChars: true } : null;
      }
    ];

    const groupNameAsyncValidators = this.editing
      ? []
      : [CdValidators.unique(this.nvmeofService.exists, this.nvmeofService)];

    this.groupForm = new CdFormGroup({
      groupName: new UntypedFormControl(null, groupNameValidators, groupNameAsyncValidators),
      unmanaged: new UntypedFormControl(false),
      enable_auth: new UntypedFormControl(false),
      enableEncryption: new UntypedFormControl(false),
      encryptionConfig: new UntypedFormControl(null),
      encryptionKey: new UntypedFormControl(null),
      enableMtls: new UntypedFormControl(false),
      certificateType: new UntypedFormControl(CertificateType.internal),
      custom_sans: new UntypedFormControl([]),
      rootCACert: new UntypedFormControl(null),
      clientCert: new UntypedFormControl(null),
      clientKey: new UntypedFormControl(null),
      serverCert: new UntypedFormControl(null),
      serverKey: new UntypedFormControl(null)
    });

    this.groupForm.get('enableEncryption')?.valueChanges.subscribe((enabled) => {
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

    if (this.editing) {
      this.groupForm.get('groupName')?.disable();
    }
  }

  loadGatewayGroupData(groupName: string) {
    this.nvmeofService.listGatewayGroups().subscribe(
      (gatewayGroups: any) => {
        const groups = gatewayGroups?.[0] ?? [];
        const group = groups.find((g: any) => g.spec?.group === groupName);

        if (group) {
          this.existingServiceData = group;
          const spec = group.spec || {};

          this.groupForm.patchValue({
            groupName: groupName,
            unmanaged: spec.unmanaged || false,
            enableEncryption: spec.ssl || false,
            enableMtls: spec.ssl || false,
            certificateType:
              spec.certificate_source === 'inline'
                ? CertificateType.external
                : CertificateType.internal,
            encryptionKey: spec.encryption_key || '',
            custom_sans: spec.custom_sans || []
          });

          if (spec.certificate_source === 'inline') {
            this.groupForm.patchValue({
              rootCACert: spec.root_ca_cert || null,
              clientCert: spec.client_cert || null,
              clientKey: spec.client_key || null,
              serverCert: spec.server_cert || null,
              serverKey: spec.server_key || null
            });
          }
        }
      },
      (_error) => {
        // Error loading gateway group data
      }
    );
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

    const formValues = this.groupForm.getRawValue();
    const selectedHostnames = this.gatewayNodeComponent?.getSelectedHostnames() || [];
    if (selectedHostnames.length === 0) {
      this.groupForm.setErrors({ cdSubmitButton: true });
      return;
    }

    const groupName = this.editing ? this.gatewayGroupName : formValues.groupName;
    const serviceName = `nvmeof.${groupName}`;
    const taskUrl = this.editing ? `service/${URLVerbs.EDIT}` : `service/${URLVerbs.CREATE}`;

    const serviceSpec: Record<string, any> = {
      service_type: 'nvmeof',
      service_id: serviceName,
      group: groupName,
      placement: {
        hosts: selectedHostnames
      },
      unmanaged: formValues.unmanaged
    };

    if (formValues.enableEncryption || formValues.enable_auth) {
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

    const apiCall = this.editing
      ? this.cephServiceService.update(serviceSpec)
      : this.cephServiceService.create(serviceSpec);

    this.taskWrapperService
      .wrapTaskAroundCall({
        task: new FinishedTask(taskUrl, {
          service_name: serviceName
        }),
        call: apiCall
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

  onCertificateTypeChange(type: CertificateType): void {
    this.groupForm.get('certificateType')?.setValue(type);
    if (type === CertificateType.internal) {
      this.groupForm.patchValue({
        rootCACert: null,
        clientCert: null,
        clientKey: null,
        serverCert: null,
        serverKey: null
      });
    } else {
      this.groupForm.patchValue({
        custom_sans: []
      });
    }
  }
}
