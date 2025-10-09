import { Component, OnInit } from '@angular/core';
import { ValidatorFn, Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';

import _ from 'lodash';
import { forkJoin as observableForkJoin } from 'rxjs';

import { MgrModuleService } from '~/app/shared/api/mgr-module.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { NotificationService } from '~/app/shared/services/notification.service';

@Component({
  selector: 'cd-mgr-module-form',
  templateUrl: './mgr-module-form.component.html',
  styleUrls: ['./mgr-module-form.component.scss']
})
export class MgrModuleFormComponent extends CdForm implements OnInit {
  mgrModuleForm: CdFormGroup;
  moduleName = '';
  moduleOptions: any[] = [];

  constructor(
    public actionLabels: ActionLabelsI18n,
    private route: ActivatedRoute,
    private router: Router,
    private formBuilder: CdFormBuilder,
    private mgrModuleService: MgrModuleService,
    private notificationService: NotificationService
  ) {
    super();
  }

  ngOnInit() {
    this.route.params.subscribe((params: { name: string }) => {
      this.moduleName = decodeURIComponent(params.name);
      const observables = [
        this.mgrModuleService.getOptions(this.moduleName),
        this.mgrModuleService.getConfig(this.moduleName)
      ];
      observableForkJoin(observables).subscribe(
        (resp: object) => {
          this.moduleOptions = resp[0];
          // Create the form dynamically.
          this.createForm();
          // Set the form field values.
          this.mgrModuleForm.setValue(resp[1]);
          this.loadingReady();
        },
        (_error) => {
          this.loadingError();
        }
      );
    });
  }

  getValidators(moduleOption: any): ValidatorFn[] {
    const result = [];
    switch (moduleOption.type) {
      case 'addr':
        result.push(CdValidators.ip());
        break;
      case 'uint':
      case 'int':
      case 'size':
      case 'secs':
        result.push(Validators.required);
        break;
      case 'str':
        if (_.isNumber(moduleOption.min)) {
          result.push(Validators.minLength(moduleOption.min));
        }
        if (_.isNumber(moduleOption.max)) {
          result.push(Validators.maxLength(moduleOption.max));
        }
        break;
      case 'float':
        result.push(Validators.required);
        result.push(CdValidators.decimalNumber());
        break;
      case 'uuid':
        result.push(CdValidators.uuid());
        break;
    }
    return result;
  }

  createForm() {
    const controlsConfig = {};
    _.forEach(this.moduleOptions, (moduleOption) => {
      controlsConfig[moduleOption.name] = [
        moduleOption.default_value,
        this.getValidators(moduleOption)
      ];
    });
    this.mgrModuleForm = this.formBuilder.group(controlsConfig);
  }

  goToListView() {
    this.router.navigate(['/mgr-modules']);
  }

  onSubmit() {
    // Exit immediately if the form isn't dirty.
    if (this.mgrModuleForm.pristine) {
      this.goToListView();
      return;
    }
    const config = {};
    _.forEach(this.moduleOptions, (moduleOption) => {
      const control = this.mgrModuleForm.get(moduleOption.name);
      // Append the option only if the value has been modified.
      if (control.dirty && control.valid) {
        config[moduleOption.name] = control.value;
      }
    });
    this.mgrModuleService.updateConfig(this.moduleName, config).subscribe(
      () => {
        this.notificationService.show(
          NotificationType.success,
          $localize`Updated options for module '${this.moduleName}'.`
        );
        this.goToListView();
      },
      () => {
        // Reset the 'Submit' button.
        this.mgrModuleForm.setErrors({ cdSubmitButton: true });
      }
    );
  }
}
