import { Component, OnInit } from '@angular/core';
import { UntypedFormControl, Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import {
  NamespaceCreateRequest,
  NamespaceEditRequest,
  NvmeofService
} from '~/app/shared/api/nvmeof.service';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { NvmeofSubsystemNamespace } from '~/app/shared/models/nvmeof';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { Pool } from '../../pool/pool';
import { PoolService } from '~/app/shared/api/pool.service';
import { RbdService } from '~/app/shared/api/rbd.service';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { Observable } from 'rxjs';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';

@Component({
  selector: 'cd-nvmeof-namespaces-form',
  templateUrl: './nvmeof-namespaces-form.component.html',
  styleUrls: ['./nvmeof-namespaces-form.component.scss']
})
export class NvmeofNamespacesFormComponent implements OnInit {
  action: string;
  permission: Permission;
  poolPermission: Permission;
  resource: string;
  pageURL: string;
  edit: boolean = false;
  nsForm: CdFormGroup;
  subsystemNQN: string;
  rbdPools: Array<Pool> = null;
  units: Array<string> = ['KiB', 'MiB', 'GiB', 'TiB'];
  nsid: string;
  currentBytes: number;
  invalidSizeError: boolean;
  group: string;

  constructor(
    public actionLabels: ActionLabelsI18n,
    private authStorageService: AuthStorageService,
    private taskWrapperService: TaskWrapperService,
    private nvmeofService: NvmeofService,
    private poolService: PoolService,
    private rbdService: RbdService,
    private router: Router,
    private route: ActivatedRoute,
    public activeModal: NgbActiveModal,
    public formatterService: FormatterService,
    public dimlessBinaryPipe: DimlessBinaryPipe
  ) {
    this.permission = this.authStorageService.getPermissions().nvmeof;
    this.poolPermission = this.authStorageService.getPermissions().pool;
    this.resource = $localize`Namespace`;
    this.pageURL = 'block/nvmeof/subsystems';
  }

  init() {
    this.route.queryParams.subscribe((params) => {
      this.group = params?.['group'];
    });
    this.createForm();
    this.action = this.actionLabels.CREATE;
    this.route.params.subscribe((params: { subsystem_nqn: string; nsid: string }) => {
      this.subsystemNQN = params.subsystem_nqn;
      this.nsid = params?.nsid;
    });
  }

  initForEdit() {
    this.edit = true;
    this.action = this.actionLabels.EDIT;
    this.nvmeofService
      .getNamespace(this.subsystemNQN, this.nsid, this.group)
      .subscribe((res: NvmeofSubsystemNamespace) => {
        const convertedSize = this.dimlessBinaryPipe.transform(res.rbd_image_size).split(' ');
        this.currentBytes = res.rbd_image_size;
        this.nsForm.get('image').setValue(res.rbd_image_name);
        this.nsForm.get('pool').setValue(res.rbd_pool_name);
        this.nsForm.get('unit').setValue(convertedSize[1]);
        this.nsForm.get('image_size').setValue(convertedSize[0]);
        this.nsForm.get('image_size').addValidators(Validators.required);
        this.nsForm.get('image').disable();
        this.nsForm.get('pool').disable();
      });
  }

  initForCreate() {
    this.poolService.getList().subscribe((resp: Pool[]) => {
      this.rbdPools = resp.filter(this.rbdService.isRBDPool);
    });
  }

  ngOnInit() {
    this.init();
    if (this.router.url.includes('subsystems/(modal:edit')) {
      this.initForEdit();
    } else {
      this.initForCreate();
    }
  }

  createForm() {
    this.nsForm = new CdFormGroup({
      image: new UntypedFormControl(`nvme_ns_image:${Date.now()}`, {
        validators: [Validators.required, Validators.pattern(/^[^@/]+?$/)]
      }),
      pool: new UntypedFormControl(null, {
        validators: [Validators.required]
      }),
      image_size: new UntypedFormControl(1, [CdValidators.number(false), Validators.min(1)]),
      unit: new UntypedFormControl(this.units[2])
    });
  }

  buildRequest(): NamespaceCreateRequest | NamespaceEditRequest {
    const image_size = this.nsForm.getValue('image_size');
    const image_size_unit = this.nsForm.getValue('unit');
    const request = {} as NamespaceCreateRequest | NamespaceEditRequest;
    request['gw_group'] = this.group;
    if (image_size) {
      const key: string = this.edit ? 'rbd_image_size' : 'size';
      const value: number = this.formatterService.toBytes(image_size + image_size_unit);
      request[key] = value;
    }
    if (!this.edit) {
      const image = this.nsForm.getValue('image');
      const pool = this.nsForm.getValue('pool');
      request['rbd_image_name'] = image;
      request['rbd_pool'] = pool;
    }
    return request;
  }

  validateSize() {
    const unit = this.nsForm.getValue('unit');
    const image_size = this.nsForm.getValue('image_size');
    if (image_size && unit) {
      const bytes = this.formatterService.toBytes(image_size + unit);
      return bytes <= this.currentBytes;
    }
    return null;
  }

  onSubmit() {
    if (this.validateSize()) {
      this.invalidSizeError = true;
      this.nsForm.setErrors({ cdSubmitButton: true });
    } else {
      this.invalidSizeError = false;
      const component = this;
      const taskUrl: string = `nvmeof/namespace/${this.edit ? URLVerbs.EDIT : URLVerbs.CREATE}`;
      const request = this.buildRequest();
      let action: Observable<any>;

      if (this.edit) {
        action = this.taskWrapperService.wrapTaskAroundCall({
          task: new FinishedTask(taskUrl, {
            nqn: this.subsystemNQN,
            nsid: this.nsid
          }),
          call: this.nvmeofService.updateNamespace(
            this.subsystemNQN,
            this.nsid,
            request as NamespaceEditRequest
          )
        });
      } else {
        action = this.taskWrapperService.wrapTaskAroundCall({
          task: new FinishedTask(taskUrl, {
            nqn: this.subsystemNQN
          }),
          call: this.nvmeofService.createNamespace(
            this.subsystemNQN,
            request as NamespaceCreateRequest
          )
        });
      }

      action.subscribe({
        error() {
          component.nsForm.setErrors({ cdSubmitButton: true });
        },
        complete: () => {
          this.router.navigate([this.pageURL, { outlets: { modal: null } }]);
        }
      });
    }
  }
}
