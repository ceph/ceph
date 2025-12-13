import { Component, OnInit, ViewChild } from '@angular/core';
import { UntypedFormControl, Validators, NgForm } from '@angular/forms';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';

import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { PoolService } from '~/app/shared/api/pool.service';
import { Pool } from '../../pool/pool';
import { NvmeofGatewayNodeComponent } from '../nvmeof-gateway-node/nvmeof-gateway-node.component';

@Component({
  selector: 'cd-nvmeof-group-form',
  templateUrl: './nvmeof-group-form.component.html',
  styleUrls: ['./nvmeof-group-form.component.scss']
})
export class NvmeofGroupFormComponent extends CdForm implements OnInit {
  @ViewChild('formDir') formDir: NgForm;
  @ViewChild(NvmeofGatewayNodeComponent) gatewayNodeComponent: NvmeofGatewayNodeComponent;

  permission: Permission;
  groupForm: CdFormGroup;
  action: string;
  resource: string;
  group: string;
  pools: Pool[] = [];
  poolsLoading = false;

  constructor(
    private authStorageService: AuthStorageService,
    public actionLabels: ActionLabelsI18n,
    public activeModal: NgbActiveModal,
    private poolService: PoolService
  ) {
    super();
    this.permission = this.authStorageService.getPermissions().nvmeof;
    this.resource = $localize`gateways group`;
  }

  ngOnInit() {
    this.action = this.actionLabels.CREATE;
    this.createForm();
    this.loadPools();
  }

  createForm() {
    this.groupForm = new CdFormGroup({
      groupName: new UntypedFormControl(null, {
        validators: [Validators.required]
      }),
      pool: new UntypedFormControl(null, {
        validators: [Validators.required]
      })
    });
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
          this.groupForm.get('pool').setValue(poolName);
        }
      },
      () => {
        this.pools = [];
        this.poolsLoading = false;
      }
    );
  }

  onSubmit() {
    const formValues = this.groupForm.value;
    const selectedHosts = this.gatewayNodeComponent?.getSelectedHosts() || [];
    const selectedHostnames = this.gatewayNodeComponent?.getSelectedHostnames() || [];
    const submitData = {
      groupName: formValues.groupName,
      pool: formValues.pool,
      targetNodes: selectedHosts,
      targetNodeNames: selectedHostnames,
      nodeCount: selectedHosts.length
    };
    if (selectedHosts.length === 0) {
      return;
    }
    console.log('Submitting NVMe-oF Gateway Group:', submitData);
  }
}
