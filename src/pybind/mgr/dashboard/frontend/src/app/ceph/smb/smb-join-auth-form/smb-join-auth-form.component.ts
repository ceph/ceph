import { Component, OnInit } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';
import { Router } from '@angular/router';
import { SmbService } from '~/app/shared/api/smb.service';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';

@Component({
  selector: 'cd-smb-join-auth-form',
  templateUrl: './smb-join-auth-form.component.html',
  styleUrls: ['./smb-join-auth-form.component.scss']
})
export class SmbJoinAuthFormComponent extends CdForm implements OnInit {
    form: CdFormGroup;
    action: string;
    resource: string;
    icons = Icons;

    constructor(
      private actionLabels: ActionLabelsI18n,
      private taskWrapperService: TaskWrapperService,
      private formBuilder: CdFormBuilder,
      private smbService: SmbService,
      private router: Router
    ) {
      super();
    }

    ngOnInit() {
      this.action = this.actionLabels.CREATE;
      this.createForm();
    }

    createForm() {
      this.form = this.formBuilder.group({
        auth_id: new FormControl('', {
          validators: [Validators.required]
        }),
        username: new FormControl('', {
          validators: [Validators.required]
        }),
        password: new FormControl('', {
          validators: [Validators.required]
        })
      });
    }

    submit() {
      const authId = this.form.getValue('auth_id');
      const username = this.form.getValue('username');
      const password = this.form.getValue('password');
      const BASE_URL = 'cephfs/smb/joinauth'

      const self = this;
      let taskUrl = `${BASE_URL}/${URLVerbs.CREATE}`;
      this.taskWrapperService
        .wrapTaskAroundCall({
          task: new FinishedTask(taskUrl, {
            authId: authId
          }),
          call: this.smbService.createJoinAuth(authId, username, password)
        })
        .subscribe({
          error() {
            self.form.setErrors({ cdSubmitButton: true });
          },
          complete: () => {
            this.router.navigate([`${BASE_URL}`]);
          }
        });
    }
}
