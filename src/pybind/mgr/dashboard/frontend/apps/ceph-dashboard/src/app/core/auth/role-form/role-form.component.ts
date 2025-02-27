import { Component, OnInit } from '@angular/core';
import { UntypedFormControl, Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';

import _ from 'lodash';
import { forkJoin as observableForkJoin } from 'rxjs';

import { RoleService } from '~/app/shared/api/role.service';
import { ScopeService } from '~/app/shared/api/scope.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { NotificationService } from '~/app/shared/services/notification.service';
import { RoleFormMode } from './role-form-mode.enum';
import { RoleFormModel } from './role-form.model';

@Component({
  selector: 'cd-role-form',
  templateUrl: './role-form.component.html',
  styleUrls: ['./role-form.component.scss']
})
export class RoleFormComponent extends CdForm implements OnInit {
  roleForm: CdFormGroup;
  response: RoleFormModel;

  columns: CdTableColumn[];
  scopes: Array<string> = [];
  scopes_permissions: Array<any> = [];
  initialValue = {};

  roleFormMode = RoleFormMode;
  mode: RoleFormMode;

  action: string;
  resource: string;

  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private roleService: RoleService,
    private scopeService: ScopeService,
    private notificationService: NotificationService,
    public actionLabels: ActionLabelsI18n
  ) {
    super();
    this.resource = $localize`role`;
    this.createForm();
    // this.listenToChanges();
  }

  createForm() {
    this.roleForm = new CdFormGroup({
      name: new UntypedFormControl('', {
        validators: [Validators.required],
        asyncValidators: [CdValidators.unique(this.roleService.exists, this.roleService)]
      }),
      description: new UntypedFormControl(''),
      scopes_permissions: new UntypedFormControl({})
    });
  }

  ngOnInit() {
    this.columns = [
      {
        prop: 'scope',
        name: $localize`All`,
        flexGrow: 2
      },
      {
        prop: 'read',
        name: $localize`Read`,
        flexGrow: 1,
        cellClass: 'text-center'
      },
      {
        prop: 'create',
        name: $localize`Create`,
        flexGrow: 1,
        cellClass: 'text-center'
      },
      {
        prop: 'update',
        name: $localize`Update`,
        flexGrow: 1,
        cellClass: 'text-center'
      },
      {
        prop: 'delete',
        name: $localize`Delete`,
        flexGrow: 1,
        cellClass: 'text-center'
      }
    ];
    if (this.router.url.startsWith('/user-management/roles/edit')) {
      this.mode = this.roleFormMode.editing;
      this.action = this.actionLabels.EDIT;
    } else {
      this.action = this.actionLabels.CREATE;
    }
    if (this.mode === this.roleFormMode.editing) {
      this.initEdit();
    } else {
      this.initCreate();
    }
  }

  initCreate() {
    // Load the scopes and initialize the default scopes/permissions data.
    this.scopeService.list().subscribe((scopes: Array<string>) => {
      this.scopes = scopes;

      this.loadingReady();
    });
  }

  initEdit() {
    // Disable the 'Name' input field.
    this.roleForm.get('name').disable();
    // Load the scopes and the role data.
    this.route.params.subscribe((params: { name: string }) => {
      const observables = [];
      observables.push(this.scopeService.list());
      observables.push(this.roleService.get(params.name));
      observableForkJoin(observables).subscribe((resp: any[]) => {
        this.scopes = resp[0];
        ['name', 'description', 'scopes_permissions'].forEach((key) =>
          this.roleForm.get(key).setValue(resp[1][key])
        );
        this.initialValue = resp[1]['scopes_permissions'];

        this.loadingReady();
      });
    });
  }

  getRequest(): RoleFormModel {
    const roleFormModel = new RoleFormModel();
    ['name', 'description', 'scopes_permissions'].forEach(
      (key) => (roleFormModel[key] = this.roleForm.get(key).value)
    );
    return roleFormModel;
  }

  createAction() {
    const roleFormModel = this.getRequest();
    this.roleService.create(roleFormModel).subscribe(
      () => {
        this.notificationService.show(
          NotificationType.success,
          $localize`Created role '${roleFormModel.name}'`
        );
        this.router.navigate(['/user-management/roles']);
      },
      () => {
        this.roleForm.setErrors({ cdSubmitButton: true });
      }
    );
  }

  editAction() {
    const roleFormModel = this.getRequest();
    this.roleService.update(roleFormModel).subscribe(
      () => {
        this.notificationService.show(
          NotificationType.success,
          $localize`Updated role '${roleFormModel.name}'`
        );
        this.router.navigate(['/user-management/roles']);
      },
      () => {
        this.roleForm.setErrors({ cdSubmitButton: true });
      }
    );
  }

  submit() {
    if (this.mode === this.roleFormMode.editing) {
      this.editAction();
    } else {
      this.createAction();
    }
  }
}
