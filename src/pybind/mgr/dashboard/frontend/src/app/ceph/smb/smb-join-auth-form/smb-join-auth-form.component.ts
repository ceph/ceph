import { Component, OnInit } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { SmbService } from '~/app/shared/api/smb.service';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { JOIN_AUTH_RESOURCE, SMBCluster, SMBJoinAuth } from '../smb.model';
import { Observable } from 'rxjs';
import { JOINAUTH_URL } from '../smb-join-auth-list/smb-join-auth-list.component';
import { Location } from '@angular/common';

@Component({
  selector: 'cd-smb-join-auth-form',
  templateUrl: './smb-join-auth-form.component.html',
  styleUrls: ['./smb-join-auth-form.component.scss']
})
export class SmbJoinAuthFormComponent extends CdForm implements OnInit {
  form: CdFormGroup;
  action: string;
  resource: string;
  editing: boolean;
  icons = Icons;

  smbClusters$: Observable<SMBCluster[]>;

  constructor(
    private actionLabels: ActionLabelsI18n,
    private taskWrapperService: TaskWrapperService,
    private formBuilder: CdFormBuilder,
    private smbService: SmbService,
    private router: Router,
    private route: ActivatedRoute,
    private location: Location
  ) {
    super();
    this.editing = this.router.url.startsWith(`${JOINAUTH_URL}/${URLVerbs.EDIT}`);
    this.resource = $localize`Active directory (AD) access resource`;
  }

  ngOnInit() {
    this.action = this.actionLabels.CREATE;
    this.smbClusters$ = this.smbService.listClusters();
    this.createForm();

    if (this.editing) {
      this.action = this.actionLabels.UPDATE;
      let editingAuthId: string;
      this.route.params.subscribe((params: { authId: string }) => {
        editingAuthId = params.authId;
      });

      this.smbService.getJoinAuth(editingAuthId).subscribe((joinAuth: SMBJoinAuth) => {
        this.form.get('authId').setValue(joinAuth.auth_id);
        this.form.get('username').setValue(joinAuth.auth.username);
        this.form.get('password').setValue(joinAuth.auth.password);
        this.form.get('linkedToCluster').setValue(joinAuth.linked_to_cluster);
      });
    }
  }

  createForm() {
    this.form = this.formBuilder.group({
      authId: new FormControl('', {
        validators: [Validators.required]
      }),
      username: new FormControl('', {
        validators: [Validators.required]
      }),
      password: new FormControl('', {
        validators: [Validators.required]
      }),
      linkedToCluster: new FormControl(null)
    });
  }

  submit() {
    const authId = this.form.getValue('authId');
    const username = this.form.getValue('username');
    const password = this.form.getValue('password');
    const linkedToCluster = this.form.getValue('linkedToCluster');
    const BASE_URL = 'smb/ad/';

    const joinAuth: SMBJoinAuth = {
      resource_type: JOIN_AUTH_RESOURCE,
      auth_id: authId,
      auth: { username: username, password: password },
      linked_to_cluster: linkedToCluster
    };

    const self = this;
    let taskUrl = `${BASE_URL}${this.editing ? URLVerbs.EDIT : URLVerbs.CREATE}`;
    this.taskWrapperService
      .wrapTaskAroundCall({
        task: new FinishedTask(taskUrl, {
          authId: authId
        }),
        call: this.smbService.createJoinAuth(joinAuth)
      })
      .subscribe({
        error() {
          self.form.setErrors({ cdSubmitButton: true });
        },
        complete: () => {
          this.location.back();
        }
      });
  }
}
