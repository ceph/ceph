import { HttpParams } from '@angular/common/http';
import { Component, OnInit, ViewChild } from '@angular/core';
import { UntypedFormControl, Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { of } from 'rxjs';
import { catchError, switchMap, tap } from 'rxjs/operators';

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
import { NotificationService } from '~/app/shared/services/notification.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import {
  CephServiceAdditionalSpec,
  CephServiceCertificate,
  CephServiceSpec,
  CertificateType
} from '~/app/shared/models/service.interface';

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
  existingServiceData: CephServiceSpec | null = null;
  preSelectedHostnames: string[] = [];
  currentCertificate: CephServiceCertificate = null;
  currentSpecCertificateSource = '';
  showCertSourceChangeWarning = false;

  constructor(
    private authStorageService: AuthStorageService,
    public actionLabels: ActionLabelsI18n,
    private taskWrapperService: TaskWrapperService,
    private cephServiceService: CephServiceService,
    private nvmeofService: NvmeofService,
    private notificationService: NotificationService,
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
        this.createForm();
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
      const encryptionConfigControl = this.groupForm.get('encryptionConfig');
      const encryptionKeyControl = this.groupForm.get('encryptionKey');

      if (!enabled) {
        // Encryption disabled — clear values and validators so the form stays valid
        encryptionKeyControl?.setValue(null, { emitEvent: false });
        encryptionConfigControl?.setValue(null, { emitEvent: false });
        encryptionKeyControl?.setValidators(null);
        encryptionConfigControl?.setValidators(null);
        encryptionKeyControl?.updateValueAndValidity({ emitEvent: false });
        encryptionConfigControl?.updateValueAndValidity({ emitEvent: false });
        return;
      }

      // Encryption enabled — the key is required (backend rejects empty encryption_key).
      // Both fields mirror each other; only encryptionKey is surfaced in the template,
      // so only that control needs the required validator on the user-visible form path.
      encryptionKeyControl?.setValidators([Validators.required]);
      encryptionKeyControl?.updateValueAndValidity({ emitEvent: false });
      // Keep encryptionConfig in sync but do NOT add required to it;
      // it is an internal mirror and never shown to the user.
      if (!encryptionKeyControl?.value && encryptionConfigControl?.value) {
        encryptionKeyControl.setValue(encryptionConfigControl.value, { emitEvent: false });
      }
      if (!encryptionConfigControl?.value && encryptionKeyControl?.value) {
        encryptionConfigControl.setValue(encryptionKeyControl.value, { emitEvent: false });
      }
    });
  }

  loadGatewayGroupData(groupName: string) {
    // Resolve by spec.group — service_name may be nvmeof.<group> or nvmeof.<pool>.<group>
    this.nvmeofService
      .listGatewayGroups()
      .pipe(
        switchMap((gatewayGroups: CephServiceSpec[][]) => {
          const groups = gatewayGroups?.[0] ?? [];
          const group = groups.find((g: CephServiceSpec) => g.spec?.group === groupName);
          if (!group) {
            return of(null);
          }

          const serviceName = group.service_name;
          if (!serviceName) {
            return of(group);
          }

          return this.cephServiceService
            .list(new HttpParams({ fromObject: { limit: -1, offset: 0 } }), serviceName)
            .observable.pipe(
              catchError(() => of(group)),
              switchMap((response: CephServiceSpec[] | CephServiceSpec) => {
                if (Array.isArray(response) && response[0]) {
                  const svcSpec: Partial<CephServiceAdditionalSpec> = response[0].spec || {};
                  return of({
                    ...group,
                    ...response[0],

                    placement: group.placement || response[0].placement,
                    spec: {
                      ...svcSpec,

                      ssl: group.spec?.ssl,
                      enable_auth: group.spec?.enable_auth,
                      encryption_key: group.spec?.encryption_key,
                      certificate_source:
                        group.spec?.certificate_source ?? svcSpec.certificate_source,
                      custom_sans: group.spec?.custom_sans ?? svcSpec.custom_sans
                    }
                  } as CephServiceSpec);
                }
                return of(group);
              })
            );
        })
      )
      .subscribe((group: CephServiceSpec | null) => {
        if (!group) {
          return;
        }
        this.populateFormFromService(group, groupName);
      });
  }

  private populateFormFromService(group: CephServiceSpec, groupName: string) {
    this.existingServiceData = group;
    const spec: Partial<CephServiceAdditionalSpec> = group.spec || {};
    const encryptionKey = spec.encryption_key || '';
    const enableMtls = spec.enable_auth === true;

    this.preSelectedHostnames = group.placement?.hosts || [];

    if (group.certificate) {
      this.currentCertificate = group.certificate;
    }
    if (spec.certificate_source) {
      this.currentSpecCertificateSource = spec.certificate_source;
    }

    this.groupForm.patchValue(
      {
        groupName: groupName,
        unmanaged: group.unmanaged || false,
        encryptionKey: encryptionKey,
        encryptionConfig: encryptionKey,
        enableMtls: enableMtls,
        certificateType:
          enableMtls && spec.certificate_source !== 'cephadm-signed'
            ? CertificateType.external
            : CertificateType.internal,
        custom_sans: spec.custom_sans || []
      },
      { emitEvent: false }
    );

    // Cert PEM fields are optional on edit; prepopulate when present.
    if (enableMtls) {
      this.groupForm.patchValue(
        {
          rootCACert: spec.root_ca_cert || null,
          clientCert: spec.client_cert || null,
          clientKey: spec.client_key || null,
          serverCert: spec.server_cert || null,
          serverKey: spec.server_key || null
        },
        { emitEvent: false }
      );
    }

    this.groupForm.get('enableEncryption')?.setValue(!!encryptionKey, { emitEvent: true });
  }

  onHostsLoaded(count: number): void {
    this.hasAvailableNodes = count > 0;
  }

  get isCreateDisabled(): boolean {
    if (!this.hasAvailableNodes && !(this.editing && this.preSelectedHostnames.length > 0)) {
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
    if (this.getSelectedOrPreselectedHosts().length === 0) {
      return true;
    }

    return false;
  }

  private getSelectedOrPreselectedHosts(): string[] {
    const selected = this.gatewayNodeComponent?.getSelectedHostnames?.() || [];
    if (selected.length > 0) {
      return selected;
    }
    // Edit: fall back to loaded placement while table selection syncs
    return this.editing ? this.preSelectedHostnames : [];
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
    const selectedHostnames = this.getSelectedOrPreselectedHosts();
    if (selectedHostnames.length === 0) {
      this.groupForm.setErrors({ cdSubmitButton: true });
      return;
    }

    const groupName = this.editing ? this.gatewayGroupName : formValues.groupName;
    // Match service-form: service_id is the group name (not nvmeof.<group>)
    const serviceId = this.editing ? this.existingServiceData?.service_id || groupName : groupName;
    const serviceName =
      this.editing && this.existingServiceData?.service_name
        ? this.existingServiceData.service_name
        : `nvmeof.${serviceId}`;
    const taskUrl = this.editing ? `service/${URLVerbs.EDIT}` : `service/${URLVerbs.CREATE}`;

    const serviceSpec: Record<string, any> = {
      service_type: 'nvmeof',
      service_id: serviceId,
      group: groupName,
      placement: {
        hosts: selectedHostnames
      },
      unmanaged: formValues.unmanaged
    };

    // Preserve the existing pool on edit. Omitting it lets the orchestrator
    // default to .nvmeof and silently move a non-default pool (e.g. rbd).
    if (this.editing) {
      const pool = this.existingServiceData?.spec?.pool;
      if (pool) {
        serviceSpec['pool'] = pool;
      }
    }

    if (formValues.enableEncryption || formValues.enable_auth) {
      const encryptionKey = formValues.encryptionKey || formValues.encryptionConfig;
      if (encryptionKey) {
        serviceSpec['encryption_key'] = encryptionKey;
      }
    } else if (this.editing) {
      // Explicitly clear the encryption key when the user disables encryption on an edit.
      // Omitting the field from the update payload leaves the existing key in place
      // because the orchestrator performs a merge, not a replace.
      serviceSpec['encryption_key'] = null;
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
        if (formValues.rootCACert) {
          serviceSpec['root_ca_cert'] = formValues.rootCACert;
        }
        if (formValues.clientCert) {
          serviceSpec['client_cert'] = formValues.clientCert;
        }
        if (formValues.clientKey) {
          serviceSpec['client_key'] = formValues.clientKey;
        }
        if (formValues.serverCert) {
          serviceSpec['server_cert'] = formValues.serverCert;
        }
        if (formValues.serverKey) {
          serviceSpec['server_key'] = formValues.serverKey;
        }
      }
    }

    if (this.editing) {
      this.cephServiceService
        .update(serviceSpec)
        .pipe(
          tap(() => {
            this.notificationService.show(
              NotificationType.success,
              $localize`Gateway group '${serviceName}' updated successfully.`
            );
          })
        )
        .subscribe({
          next: () => {
            this.goToListView();
          },
          error: () => {
            this.groupForm.setErrors({ cdSubmitButton: true });
          }
        });
    } else {
      this.taskWrapperService
        .wrapTaskAroundCall({
          task: new FinishedTask(taskUrl, {
            service_name: serviceName
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

    if (this.editing && this.currentCertificate?.has_certificate) {
      const originalSource =
        this.currentSpecCertificateSource === 'cephadm-signed'
          ? CertificateType.internal
          : CertificateType.external;
      this.showCertSourceChangeWarning = type !== originalSource;
    }

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
