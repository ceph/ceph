import { Component, OnInit } from '@angular/core';
import { UntypedFormControl, Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import {
  NamespaceCreateRequest,
  NamespaceUpdateRequest,
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
import { forkJoin, Observable } from 'rxjs';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';
import { HttpResponse } from '@angular/common/http';

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
  units: Array<string> = ['MiB', 'GiB', 'TiB'];
  nsid: string;
  currentBytes: number;
  invalidSizeError: boolean;
  group: string;
  MAX_NAMESPACE_CREATE: number = 5;
  MIN_NAMESPACE_CREATE: number = 1;
  requiredInvalidText: string = $localize`This field is required`;
  nsCountInvalidText: string = $localize`The namespace count should be between 1 and 5`;

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
        this.nsForm.get('pool').setValue(res.rbd_pool_name);
        this.nsForm.get('unit').setValue(convertedSize[1]);
        this.nsForm.get('image_size').setValue(convertedSize[0]);
        this.nsForm.get('image_size').addValidators(Validators.required);
        this.nsForm.get('pool').disable();
      });
  }

  initForCreate() {
    this.poolService.getList().subscribe((resp: Pool[]) => {
      this.rbdPools = resp.filter(this.rbdService.isRBDPool);
      if (this.rbdPools?.length) {
        this.nsForm.get('pool').setValue(this.rbdPools[0].pool_name);
      }
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
      pool: new UntypedFormControl(null, {
        validators: [Validators.required]
      }),
      image_size: new UntypedFormControl(1, [CdValidators.number(false), Validators.min(1)]),
      unit: new UntypedFormControl(this.units[1]),
      nsCount: new UntypedFormControl(this.MAX_NAMESPACE_CREATE, [
        Validators.required,
        Validators.max(this.MAX_NAMESPACE_CREATE),
        Validators.min(this.MIN_NAMESPACE_CREATE)
      ])
    });
  }

  buildUpdateRequest(rbdImageSize: number): Observable<HttpResponse<Object>> {
    const request: NamespaceUpdateRequest = {
      gw_group: this.group,
      rbd_image_size: rbdImageSize
    };
    return this.nvmeofService.updateNamespace(
      this.subsystemNQN,
      this.nsid,
      request as NamespaceUpdateRequest
    );
  }

  randomString() {
    return Math.random().toString(36).substring(2);
  }

  buildCreateRequest(rbdImageSize: number, nsCount: number): Observable<HttpResponse<Object>>[] {
    const pool = this.nsForm.getValue('pool');
    const requests: Observable<HttpResponse<Object>>[] = [];

    for (let i = 1; i <= nsCount; i++) {
      const request: NamespaceCreateRequest = {
        gw_group: this.group,
        rbd_image_name: `nvme_${pool}_${this.group}_${this.randomString()}`,
        rbd_pool: pool,
        create_image: true
      };
      if (rbdImageSize) {
        request['rbd_image_size'] = rbdImageSize;
      }
      requests.push(this.nvmeofService.createNamespace(this.subsystemNQN, request));
    }

    return requests;
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
      const image_size = this.nsForm.getValue('image_size');
      const nsCount = this.nsForm.getValue('nsCount');
      let action: Observable<HttpResponse<Object>>;
      let rbdImageSize: number = null;

      if (image_size) {
        const image_size_unit = this.nsForm.getValue('unit');
        const value: number = this.formatterService.toBytes(image_size + image_size_unit);
        rbdImageSize = value;
      }
      if (this.edit) {
        action = this.taskWrapperService.wrapTaskAroundCall({
          task: new FinishedTask(taskUrl, {
            nqn: this.subsystemNQN,
            nsid: this.nsid
          }),
          call: this.buildUpdateRequest(rbdImageSize)
        });
      } else {
        action = this.taskWrapperService.wrapTaskAroundCall({
          task: new FinishedTask(taskUrl, {
            nqn: this.subsystemNQN,
            nsCount
          }),
          call: forkJoin(this.buildCreateRequest(rbdImageSize, nsCount))
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
