import { Component, OnInit } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';
import { Router } from '@angular/router';

import { HostService } from '~/app/shared/api/host.service';
import { SelectMessages } from '~/app/shared/components/select/select-messages.model';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';

@Component({
  selector: 'cd-host-form',
  templateUrl: './host-form.component.html',
  styleUrls: ['./host-form.component.scss']
})
export class HostFormComponent extends CdForm implements OnInit {
  hostForm: CdFormGroup;
  action: string;
  resource: string;
  hostnames: string[];
  addr: string;
  status: string;
  allLabels: any;

  messages = new SelectMessages({
    empty: $localize`There are no labels.`,
    filter: $localize`Filter or add labels`,
    add: $localize`Add label`
  });

  constructor(
    private router: Router,
    private actionLabels: ActionLabelsI18n,
    private hostService: HostService,
    private taskWrapper: TaskWrapperService
  ) {
    super();
    this.resource = $localize`host`;
    this.action = this.actionLabels.ADD;
    this.createForm();
  }

  ngOnInit() {
    this.hostService.list().subscribe((resp: any[]) => {
      this.hostnames = resp.map((host) => {
        return host['hostname'];
      });
      this.loadingReady();
    });
  }

  private createForm() {
    this.hostForm = new CdFormGroup({
      hostname: new FormControl('', {
        validators: [
          Validators.required,
          CdValidators.custom('uniqueName', (hostname: string) => {
            return this.hostnames && this.hostnames.indexOf(hostname) !== -1;
          })
        ]
      }),
      addr: new FormControl('', {
        validators: [CdValidators.ip()]
      }),
      labels: new FormControl([]),
      maintenance: new FormControl(false)
    });
  }

  submit() {
    const hostname = this.hostForm.get('hostname').value;
    this.addr = this.hostForm.get('addr').value;
    this.status = this.hostForm.get('maintenance').value ? 'maintenance' : '';
    this.allLabels = this.hostForm.get('labels').value;
    this.taskWrapper
      .wrapTaskAroundCall({
        task: new FinishedTask('host/' + URLVerbs.CREATE, {
          hostname: hostname
        }),
        call: this.hostService.create(hostname, this.addr, this.allLabels, this.status)
      })
      .subscribe({
        error: () => {
          this.hostForm.setErrors({ cdSubmitButton: true });
        },
        complete: () => {
          this.router.navigate(['/hosts']);
        }
      });
  }
}
