import { Component, OnInit } from '@angular/core';
import { UntypedFormControl, UntypedFormGroup, ValidatorFn } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';

import _ from 'lodash';

import { ConfigurationService } from '~/app/shared/api/configuration.service';
import { ConfigFormModel } from '~/app/shared/components/config-option/config-option.model';
import { ConfigOptionTypes } from '~/app/shared/components/config-option/config-option.types';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { NotificationService } from '~/app/shared/services/notification.service';
import { ConfigFormCreateRequestModel } from './configuration-form-create-request.model';

@Component({
  selector: 'cd-configuration-form',
  templateUrl: './configuration-form.component.html',
  styleUrls: ['./configuration-form.component.scss']
})
export class ConfigurationFormComponent extends CdForm implements OnInit {
  configForm: CdFormGroup;
  response: ConfigFormModel;
  type: string;
  inputType: string;
  humanReadableType: string;
  minValue: number;
  maxValue: number;
  patternHelpText: string;
  availSections = ['global', 'mon', 'mgr', 'osd', 'mds', 'client'];

  constructor(
    public actionLabels: ActionLabelsI18n,
    private route: ActivatedRoute,
    private router: Router,
    private configService: ConfigurationService,
    private notificationService: NotificationService
  ) {
    super();
    this.createForm();
  }

  createForm() {
    const formControls = {
      name: new UntypedFormControl({ value: null }),
      desc: new UntypedFormControl({ value: null }),
      long_desc: new UntypedFormControl({ value: null }),
      values: new UntypedFormGroup({}),
      default: new UntypedFormControl({ value: null }),
      daemon_default: new UntypedFormControl({ value: null }),
      services: new UntypedFormControl([])
    };

    this.availSections.forEach((section) => {
      formControls.values.addControl(section, new UntypedFormControl(null));
    });

    this.configForm = new CdFormGroup(formControls);
  }

  ngOnInit() {
    this.route.params.subscribe((params: { name: string }) => {
      const configName = params.name;
      this.configService.get(configName).subscribe((resp: ConfigFormModel) => {
        this.setResponse(resp);
        this.loadingReady();
      });
    });
  }

  getValidators(configOption: any): ValidatorFn[] {
    const typeValidators = ConfigOptionTypes.getTypeValidators(configOption);
    if (typeValidators) {
      this.patternHelpText = typeValidators.patternHelpText;

      if ('max' in typeValidators && typeValidators.max !== '') {
        this.maxValue = typeValidators.max;
      }

      if ('min' in typeValidators && typeValidators.min !== '') {
        this.minValue = typeValidators.min;
      }

      return typeValidators.validators;
    }

    return undefined;
  }

  getStep(type: string, value: number): number | undefined {
    return ConfigOptionTypes.getTypeStep(type, value);
  }

  setResponse(response: ConfigFormModel) {
    this.response = response;
    const validators = this.getValidators(response);

    this.configForm.get('name').setValue(response.name);
    this.configForm.get('desc').setValue(response.desc);
    this.configForm.get('long_desc').setValue(response.long_desc);
    this.configForm.get('default').setValue(response.default);
    this.configForm.get('daemon_default').setValue(response.daemon_default);
    this.configForm.get('services').setValue(response.services);

    if (this.response.value) {
      this.response.value.forEach((value) => {
        // Check value type. If it's a boolean value we need to convert it because otherwise we
        // would use the string representation. That would cause issues for e.g. checkboxes.
        let sectionValue = null;
        if (value.value === 'true') {
          sectionValue = true;
        } else if (value.value === 'false') {
          sectionValue = false;
        } else {
          sectionValue = value.value;
        }
        this.configForm.get('values').get(value.section).setValue(sectionValue);
      });
    }

    this.availSections.forEach((section) => {
      this.configForm.get('values').get(section).setValidators(validators);
    });

    const currentType = ConfigOptionTypes.getType(response.type);
    this.type = currentType.name;
    this.inputType = currentType.inputType;
    this.humanReadableType = currentType.humanReadable;
  }

  createRequest(): ConfigFormCreateRequestModel | null {
    const values: any[] = [];

    this.availSections.forEach((section) => {
      const sectionValue = this.configForm.getValue(section);
      if (sectionValue !== null && sectionValue !== '') {
        values.push({ section: section, value: sectionValue });
      }
    });

    if (!_.isEqual(this.response.value, values)) {
      const request = new ConfigFormCreateRequestModel();
      request.name = this.configForm.getValue('name');
      request.value = values;
      return request;
    }

    return null;
  }

  submit() {
    const request = this.createRequest();

    if (request) {
      this.configService.create(request).subscribe(
        () => {
          this.notificationService.show(
            NotificationType.success,
            $localize`Updated config option ${request.name}`
          );
          this.router.navigate(['/configuration']);
        },
        () => {
          this.configForm.setErrors({ cdSubmitButton: true });
        }
      );
    }

    this.router.navigate(['/configuration']);
  }
}
