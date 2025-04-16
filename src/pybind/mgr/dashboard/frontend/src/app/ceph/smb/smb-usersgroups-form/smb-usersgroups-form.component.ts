import {
  ChangeDetectorRef,
  Component,
  computed,
  effect,
  OnInit,
  Signal,
  OnDestroy
} from '@angular/core';
import { FormArray, FormControl, Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { Observable } from 'rxjs';
import { SmbService } from '~/app/shared/api/smb.service';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { Group, SMBCluster, SMBUsersGroups, User, USERSGROUPS_RESOURCE } from '../smb.model';
import { Location } from '@angular/common';
import { USERSGROUPS_PATH } from '../smb-usersgroups-list/smb-usersgroups-list.component';

@Component({
  selector: 'cd-smb-usersgroups-form',
  templateUrl: './smb-usersgroups-form.component.html',
  styleUrls: ['./smb-usersgroups-form.component.scss']
})
export class SmbUsersgroupsFormComponent extends CdForm implements OnInit, OnDestroy {
  form: CdFormGroup;
  action: string;
  resource: string;
  editing: boolean;
  icons = Icons;
  hideUploader: boolean = false;

  smbClusters$: Observable<SMBCluster[]>;
  uploadedData: Signal<SMBUsersGroups> = computed(() => {
    return this.smbService.dataUploader() as SMBUsersGroups;
  });

  constructor(
    private actionLabels: ActionLabelsI18n,
    private taskWrapperService: TaskWrapperService,
    private formBuilder: CdFormBuilder,
    public smbService: SmbService,
    private router: Router,
    private cd: ChangeDetectorRef,
    private route: ActivatedRoute,
    private location: Location
  ) {
    super();
    this.editing = this.router.url.startsWith(`/${USERSGROUPS_PATH}/${URLVerbs.EDIT}`);
    this.resource = $localize`users and groups access resource`;
    effect(() => {
      const formData = this.uploadedData();
      if (formData) {
        this.fillForm(formData);
        this.hideUploader = true;
      }
    });
  }

  ngOnInit() {
    this.action = this.actionLabels.CREATE;
    this.smbClusters$ = this.smbService.listClusters();
    this.createForm();
    if (this.editing) {
      this.action = this.actionLabels.UPDATE;
      this.form.get('usersGroupsId').disable();
      let editingUsersGroupId: string;
      this.route.params.subscribe((params: { usersGroupsId: string }) => {
        editingUsersGroupId = params.usersGroupsId;
      });
      this.smbService
        .getUsersGroups(editingUsersGroupId)
        .subscribe((usersGroups: SMBUsersGroups) => {
          this.fillForm(usersGroups);
        });
    } else {
      this.addUser();
    }
  }

  ngOnDestroy() {
    this.smbService.setDataUploaded(null);
  }

  fillForm(usersGroups: SMBUsersGroups) {
    this.form.get('usersGroupsId').setValue(usersGroups.users_groups_id);
    this.form.get('linkedToCluster').setValue(usersGroups.linked_to_cluster);
    this.users.controls.forEach((userField: CdFormGroup) => {
      if (!userField.controls.name.value && userField.controls.password) {
        userField.patchValue({
          name: usersGroups.values.users[0].name,
          password: usersGroups.values.users[0].password
        });
        usersGroups.values.users.shift();
      }
    });
    this.groups.controls.forEach((groupField: CdFormGroup) => {
      if (!groupField.controls.name.value) {
        groupField.patchValue({
          name: usersGroups.values.users[0].name
        });
        usersGroups.values.groups.shift();
      }
    });
    usersGroups.values.users.forEach((user: User) => {
      this.addUser(user);
    });
    usersGroups.values.groups.forEach((group: Group) => {
      this.addGroup(group);
    });
  }

  createForm() {
    this.form = this.formBuilder.group({
      usersGroupsId: new FormControl('', {
        validators: [Validators.required]
      }),
      linkedToCluster: new FormControl(null),
      users: new FormArray([]),
      groups: new FormArray([])
    });
  }

  submit() {
    const usersGroupsId = this.form.getValue('usersGroupsId');
    const linkedToCluster = this.form.getValue('linkedToCluster');
    const users = this.form.getValue('users');
    const groups = this.form.getValue('groups');
    const usersgroups: SMBUsersGroups = {
      resource_type: USERSGROUPS_RESOURCE,
      users_groups_id: usersGroupsId,
      values: { users: users, groups: groups },
      linked_to_cluster: linkedToCluster
    };

    const self = this;

    let taskUrl = `${USERSGROUPS_PATH}/${this.editing ? URLVerbs.EDIT : URLVerbs.CREATE}`;
    this.taskWrapperService
      .wrapTaskAroundCall({
        task: new FinishedTask(taskUrl, {
          usersGroupsId: usersGroupsId
        }),
        call: this.smbService.createUsersGroups(usersgroups)
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

  get users(): FormArray {
    return this.form.get('users') as FormArray;
  }

  get groups(): FormArray {
    return this.form.get('groups') as FormArray;
  }

  newUser(user?: User): CdFormGroup {
    return this.formBuilder.group({
      name: [user ? user.name : '', Validators.required],
      password: [user ? user.password : '', [Validators.required]]
    });
  }

  newGroup(group?: Group): CdFormGroup {
    return this.formBuilder.group({
      name: [group ? group.name : '']
    });
  }

  addUser(user?: User): void {
    this.users.push(this.newUser(user));
  }

  addGroup(group?: Group): void {
    this.groups.push(this.newGroup(group));
  }

  removeUser(index: number): void {
    this.users.removeAt(index);
    this.cd.detectChanges();
  }

  removeGroup(index: number): void {
    this.groups.removeAt(index);
    this.cd.detectChanges();
  }
}
