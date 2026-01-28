import { Component, OnInit, ViewChild } from '@angular/core';
import { UntypedFormControl, Validators } from '@angular/forms';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';

import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { PoolService } from '~/app/shared/api/pool.service';
import { Pool } from '../../pool/pool';
import { NvmeofGatewayNodeComponent } from '../nvmeof-gateway-node/nvmeof-gateway-node.component';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { CephServiceService } from '~/app/shared/api/ceph-service.service';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { Router } from '@angular/router';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';

@Component({
  selector: 'cd-nvmeof-group-form',
  templateUrl: './nvmeof-group-form.component.html',
  styleUrls: ['./nvmeof-group-form.component.scss'],
  standalone: false
})
export class NvmeofGroupFormComponent extends CdForm implements OnInit {
  @ViewChild(NvmeofGatewayNodeComponent) gatewayNodeComponent: NvmeofGatewayNodeComponent;

  permission: Permission;
  groupForm: CdFormGroup;
  action: string;
  resource: string;
  group: string;
  pools: Pool[] = [];
  poolsLoading = false;
  pageURL: string;
  hasAvailableNodes = true;

  constructor(
    private authStorageService: AuthStorageService,
    public actionLabels: ActionLabelsI18n,
    private poolService: PoolService,
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
    this.loadPools();
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
      pool: new UntypedFormControl('rbd', {
        validators: [Validators.required]
      }),
      unmanaged: new UntypedFormControl(false)
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

  loadPools() {
    this.poolsLoading = true;
    this.poolService.list().then(
      (pools: Pool[]) => {
        this.pools = (pools || []).filter(
          (pool: Pool) => pool.application_metadata && pool.application_metadata.includes('rbd')
        );
        this.poolsLoading = false;
        if (this.pools.length >= 1) {
          const allPoolNames = this.pools.map((pool) => pool.pool_name);
          const poolName = allPoolNames.includes('rbd') ? 'rbd' : this.pools[0].pool_name;
          this.groupForm.patchValue({ pool: poolName });
        }
      },
      () => {
        this.pools = [];
        this.poolsLoading = false;
      }
    );
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
    let taskUrl = `service/${URLVerbs.CREATE}`;
    const serviceName = `${formValues.pool}.${formValues.groupName}`;

    const serviceSpec = {
      service_type: 'nvmeof',
      service_id: serviceName,
      pool: formValues.pool,
      group: formValues.groupName,
      placement: {
        hosts: selectedHostnames
      },
      unmanaged: formValues.unmanaged
    };

    this.taskWrapperService
      .wrapTaskAroundCall({
        task: new FinishedTask(taskUrl, {
          service_name: `nvmeof.${serviceName}`
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
}
