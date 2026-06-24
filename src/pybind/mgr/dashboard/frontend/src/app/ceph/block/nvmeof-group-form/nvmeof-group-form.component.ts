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

enum CertificateType {
  internal = 'internal',
  external = 'external'
}

@Component({
  selector: 'cd-nvmeof-group-form',
  templateUrl: './nvmeof-group-form.component.html',
  styleUrls: ['./nvmeof-group-form.component.scss'],
  standalone: false
})
export class NvmeofGroupFormComponent extends CdForm implements OnInit {
  @ViewChild(NvmeofGatewayNodeComponent) gatewayNodeComponent!: NvmeofGatewayNodeComponent;

  permission: Permission;
  groupForm!: CdFormGroup;
  action!: string;
  resource: string;
  group: string;
  pageURL: string;
  hasAvailableNodes = true;
  CertificateType = CertificateType;
  editing = false;
  gatewayGroupName = '';
  existingServiceData: any = null;
  pools: Array<{ pool_name: string }> = [{ pool_name: 'rbd' }];
  poolsLoading = false;

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
    this.route.params.subscribe((params) => {
      if (params['name']) {
        this.editing = true;
        this.gatewayGroupName = params['name'];
        this.action = this.actionLabels.EDIT;
        this.createForm();
        this.loadGatewayGroupData(params['name']);
      } else {
        this.editing = false;
        this.action = this.actionLabels.CREATE;
        this.gatewayGroupName = '';
        this.createForm();
      }
    });
  }

  createForm() {
    const groupNameValidators = this.editing
      ? [
          Validators.required,
          (control) => {
            const value = control.value;
            return value && /[^a-zA-Z0-9_-]/.test(value) ? { invalidChars: true } : null;
          }
        ]
      : [
          Validators.required,
          (control) => {
            const value = control.value;
            return value && /[^a-zA-Z0-9_-]/.test(value) ? { invalidChars: true } : null;
          }
        ];

    const groupNameAsyncValidators = this.editing
      ? []
      : [CdValidators.unique(this.nvmeofService.exists, this.nvmeofService)];

    this.groupForm = new CdFormGroup({
      groupName: new UntypedFormControl(null, groupNameValidators, groupNameAsyncValidators),
      pool: new UntypedFormControl('rbd', {
        validators: [Validators.required]
      }),
      unmanaged: new UntypedFormControl(false),
      enableEncryption: new UntypedFormControl(false),
      enableMtls: new UntypedFormControl(false),
      encryptionKey: new UntypedFormControl('', [
        CdValidators.composeIf(
          {
            enableEncryption: true
          },
          [Validators.required]
        )
      ]),
      certificateType: new UntypedFormControl(CertificateType.internal),
      custom_sans: new UntypedFormControl([]),
      rootCACert: new UntypedFormControl(null, [
        CdValidators.composeIf(
          {
            enableEncryption: true,
            certificateType: 'external'
          },
          [Validators.required]
        )
      ]),
      clientCert: new UntypedFormControl(null, [
        CdValidators.composeIf(
          {
            enableEncryption: true,
            certificateType: 'external'
          },
          [Validators.required]
        )
      ]),
      clientKey: new UntypedFormControl(null, [
        CdValidators.composeIf(
          {
            enableEncryption: true,
            certificateType: 'external'
          },
          [Validators.required]
        )
      ]),
      serverCert: new UntypedFormControl(null, [
        CdValidators.composeIf(
          {
            enableEncryption: true,
            certificateType: 'external'
          },
          [Validators.required]
        )
      ]),
      serverKey: new UntypedFormControl(null, [
        CdValidators.composeIf(
          {
            enableEncryption: true,
            certificateType: CertificateType.external
          },
          [Validators.required]
        )
      ])
    });

    // Disable group name field in edit mode
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

          // Prefill form with existing data
          this.groupForm.patchValue({
            groupName: groupName,
            pool: spec.pool || 'rbd',
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

          // If external certificates, load them
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

    const formValues = this.groupForm.getRawValue(); // Use getRawValue to get disabled fields too
    const selectedHostnames = this.gatewayNodeComponent?.getSelectedHostnames() || [];
    if (selectedHostnames.length === 0) {
      this.groupForm.setErrors({ cdSubmitButton: true });
      return;
    }

    const groupName = this.editing ? this.gatewayGroupName : formValues.groupName;
    const pool = formValues.pool;
    const serviceName = `${pool}.${groupName}`;

    let taskUrl = this.editing ? `service/${URLVerbs.EDIT}` : `service/${URLVerbs.CREATE}`;

    const serviceSpec: Record<string, any> = {
      service_type: 'nvmeof',
      service_id: serviceName,
      pool: pool,
      group: groupName,
      placement: {
        hosts: selectedHostnames
      },
      unmanaged: formValues.unmanaged
    };

    if (formValues.enableMtls) {
      serviceSpec['ssl'] = true;
      serviceSpec['enable_auth'] = true;
      serviceSpec['certificate_source'] =
        formValues.certificateType === CertificateType.internal ? 'cephadm-signed' : 'inline';

      if (formValues.certificateType === CertificateType.internal) {
        if (formValues.custom_sans && formValues.custom_sans.length > 0) {
          serviceSpec['custom_sans'] = formValues.custom_sans;
        }
      } else {
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
          service_name: `nvmeof.${serviceName}`
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

  onFileUpload(event: any, controlName: string) {
    const file = event.target.files[0];
    if (file) {
      const reader = new FileReader();
      reader.onload = (e: any) => {
        this.groupForm.get(controlName)?.setValue(e.target.result);
      };
      reader.readAsText(file);
    }
  }

  onCertificateTypeChange(type: CertificateType) {
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
