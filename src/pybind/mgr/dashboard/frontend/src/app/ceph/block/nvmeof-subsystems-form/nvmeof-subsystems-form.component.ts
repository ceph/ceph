import { Component, OnInit } from '@angular/core';
import { UntypedFormControl, Validators } from '@angular/forms';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { ActivatedRoute, Router } from '@angular/router';
import { MAX_NAMESPACE, NvmeofService } from '~/app/shared/api/nvmeof.service';

@Component({
  selector: 'cd-nvmeof-subsystems-form',
  templateUrl: './nvmeof-subsystems-form.component.html',
  styleUrls: ['./nvmeof-subsystems-form.component.scss']
})
export class NvmeofSubsystemsFormComponent implements OnInit {
  permission: Permission;
  subsystemForm: CdFormGroup;
  action: string;
  resource: string;
  pageURL: string;
  defaultMaxNamespace: number = MAX_NAMESPACE;
  group: string;

  constructor(
    private authStorageService: AuthStorageService,
    public actionLabels: ActionLabelsI18n,
    public activeModal: NgbActiveModal,
    private nvmeofService: NvmeofService,
    private taskWrapperService: TaskWrapperService,
    private router: Router,
    private route: ActivatedRoute
  ) {
    this.permission = this.authStorageService.getPermissions().nvmeof;
    this.resource = $localize`Subsystem`;
    this.pageURL = 'block/nvmeof/subsystems';
  }

  DEFAULT_NQN = 'nqn.2001-07.com.ceph:' + Date.now();
  NQN_REGEX = /^nqn\.(19|20)\d\d-(0[1-9]|1[0-2])\.\D{2,3}(\.[A-Za-z0-9-]+)+(:[A-Za-z0-9-\.]+(:[A-Za-z0-9-\.]+)*)$/;
  NQN_REGEX_UUID = /^nqn\.2014-08\.org\.nvmexpress:uuid:[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/;

  customNQNValidator = CdValidators.custom(
    'pattern',
    (nqnInput: string) =>
      !!nqnInput && !(this.NQN_REGEX.test(nqnInput) || this.NQN_REGEX_UUID.test(nqnInput))
  );

  ngOnInit() {
    this.route.queryParams.subscribe((params) => {
      this.group = params?.['group'];
    });
    this.createForm();
    this.action = this.actionLabels.CREATE;
  }

  createForm() {
    this.subsystemForm = new CdFormGroup({
      nqn: new UntypedFormControl(this.DEFAULT_NQN, {
        validators: [
          this.customNQNValidator,
          Validators.required,
          this.customNQNValidator,
          CdValidators.custom(
            'maxLength',
            (nqnInput: string) => new TextEncoder().encode(nqnInput).length > 223
          )
        ],
        asyncValidators: [
          CdValidators.unique(
            this.nvmeofService.isSubsystemPresent,
            this.nvmeofService,
            null,
            null,
            this.group
          )
        ]
      }),
      max_namespaces: new UntypedFormControl(this.defaultMaxNamespace, {
        validators: [
          CdValidators.number(false),
          Validators.max(this.defaultMaxNamespace),
          Validators.min(1)
        ]
      })
    });
  }

  onSubmit() {
    const component = this;
    const nqn: string = this.subsystemForm.getValue('nqn');
    const max_namespaces: number = Number(this.subsystemForm.getValue('max_namespaces'));
    let taskUrl = `nvmeof/subsystem/${URLVerbs.CREATE}`;

    const request = {
      nqn,
      enable_ha: true,
      gw_group: this.group,
      max_namespaces
    };

    if (!max_namespaces) {
      delete request.max_namespaces;
    }
    this.taskWrapperService
      .wrapTaskAroundCall({
        task: new FinishedTask(taskUrl, {
          nqn: nqn
        }),
        call: this.nvmeofService.createSubsystem(request)
      })
      .subscribe({
        error() {
          component.subsystemForm.setErrors({ cdSubmitButton: true });
        },
        complete: () => {
          this.router.navigate([this.pageURL, { outlets: { modal: null } }]);
        }
      });
  }
}
