import { Component, OnInit } from '@angular/core';
import { UntypedFormArray, UntypedFormControl, Validators } from '@angular/forms';

import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { ActivatedRoute, Router } from '@angular/router';
import { InitiatorRequest, NvmeofService } from '~/app/shared/api/nvmeof.service';

@Component({
  selector: 'cd-nvmeof-initiators-form',
  templateUrl: './nvmeof-initiators-form.component.html',
  styleUrls: ['./nvmeof-initiators-form.component.scss']
})
export class NvmeofInitiatorsFormComponent implements OnInit {
  permission: Permission;
  initiatorForm: CdFormGroup;
  action: string;
  resource: string;
  pageURL: string;
  remove: boolean = false;
  subsystemNQN: string;
  removeHosts: { name: string; value: boolean; id: number }[] = [];
  group: string;

  constructor(
    private authStorageService: AuthStorageService,
    public actionLabels: ActionLabelsI18n,
    private nvmeofService: NvmeofService,
    private taskWrapperService: TaskWrapperService,
    private router: Router,
    private route: ActivatedRoute,
    private formBuilder: CdFormBuilder
  ) {
    this.permission = this.authStorageService.getPermissions().nvmeof;
    this.resource = $localize`Initiator`;
    this.pageURL = 'block/nvmeof/subsystems';
  }

  NQN_REGEX = /^nqn\.(19|20)\d\d-(0[1-9]|1[0-2])\.\D{2,3}(\.[A-Za-z0-9-]+)+(:[A-Za-z0-9-\.]+(:[A-Za-z0-9-\.]+)*)$/;
  NQN_REGEX_UUID = /^nqn\.2014-08\.org\.nvmexpress:uuid:[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/;
  ALLOW_ALL_HOST = '*';

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
    this.action = this.actionLabels.ADD;
    this.route.params.subscribe((params: { subsystem_nqn: string }) => {
      this.subsystemNQN = params.subsystem_nqn;
    });
  }

  createForm() {
    this.initiatorForm = new CdFormGroup({
      allowAnyHost: new UntypedFormControl(false),
      addHost: new CdFormGroup({
        addHostCheck: new UntypedFormControl(false),
        addedHosts: this.formBuilder.array(
          [],
          [
            CdValidators.custom(
              'duplicate',
              (hosts: string[]) => !!hosts.length && new Set(hosts)?.size !== hosts.length
            )
          ]
        )
      })
    });
  }

  get addedHosts(): UntypedFormArray {
    return this.initiatorForm.get('addHost.addedHosts') as UntypedFormArray;
  }

  addHost() {
    let newHostFormGroup;
    newHostFormGroup = this.formBuilder.control('', [this.customNQNValidator, Validators.required]);
    this.addedHosts.push(newHostFormGroup);
  }

  removeHost(index: number) {
    this.addedHosts.removeAt(index);
  }

  setAddHostCheck() {
    const addHostCheck = this.initiatorForm.get('addHost.addHostCheck').value;
    if (!addHostCheck) {
      while (this.addedHosts.length !== 0) {
        this.addedHosts.removeAt(0);
      }
    } else {
      this.addHost();
    }
  }

  onSubmit() {
    const component = this;
    const allowAnyHost: boolean = this.initiatorForm.getValue('allowAnyHost');
    const hosts: string[] = this.addedHosts.value;
    let taskUrl = `nvmeof/initiator/${URLVerbs.ADD}`;

    const request: InitiatorRequest = {
      host_nqn: hosts.join(','),
      gw_group: this.group
    };

    if (allowAnyHost) {
      hosts.push('*');
      request['host_nqn'] = hosts.join(',');
    }
    this.taskWrapperService
      .wrapTaskAroundCall({
        task: new FinishedTask(taskUrl, {
          nqn: this.subsystemNQN
        }),
        call: this.nvmeofService.addInitiators(this.subsystemNQN, request)
      })
      .subscribe({
        error() {
          component.initiatorForm.setErrors({ cdSubmitButton: true });
        },
        complete: () => {
          this.router.navigate([this.pageURL, { outlets: { modal: null } }]);
        }
      });
  }
}
