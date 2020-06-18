import { Component, OnInit } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';
import { Router } from '@angular/router';

import { I18n } from '@ngx-translate/i18n-polyfill';

import { HostService } from '../../../../shared/api/host.service';
import { ActionLabelsI18n, URLVerbs } from '../../../../shared/constants/app.constants';
import { CdForm } from '../../../../shared/forms/cd-form';
import { CdFormGroup } from '../../../../shared/forms/cd-form-group';
import { CdValidators } from '../../../../shared/forms/cd-validators';
import { FinishedTask } from '../../../../shared/models/finished-task';
import { TaskWrapperService } from '../../../../shared/services/task-wrapper.service';

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

  constructor(
    private router: Router,
    private i18n: I18n,
    private actionLabels: ActionLabelsI18n,
    private hostService: HostService,
    private taskWrapper: TaskWrapperService
  ) {
    super();
    this.resource = this.i18n('host');
    this.action = this.actionLabels.CREATE;
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
      })
    });
  }

  submit() {
    const hostname = this.hostForm.get('hostname').value;
    this.taskWrapper
      .wrapTaskAroundCall({
        task: new FinishedTask('host/' + URLVerbs.CREATE, {
          hostname: hostname
        }),
        call: this.hostService.create(hostname)
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
