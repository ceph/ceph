import { Component, OnInit } from '@angular/core';
import { FormControl, FormGroup, ValidatorFn } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';

import { I18n } from '@ngx-translate/i18n-polyfill';
import * as _ from 'lodash';

import { ConfigurationService } from '../../../../shared/api/configuration.service';
import { NotificationType } from '../../../../shared/enum/notification-type.enum';
import { CdFormGroup } from '../../../../shared/forms/cd-form-group';
import { NotificationService } from '../../../../shared/services/notification.service';
import { ConfigFormCreateRequestModel } from './configuration-form-create-request.model';
import { ConfigFormModel } from './configuration-form.model';
import { ConfigOptionTypes } from './configuration-form.types';

@Component({
  selector: 'cd-configuration-form',
  templateUrl: './configuration-form.component.html',
  styleUrls: ['./configuration-form.component.scss']
})
export class ConfigurationFormComponent implements OnInit {
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
    private route: ActivatedRoute,
    private router: Router,
    private configService: ConfigurationService,
    private notificationService: NotificationService,
    private i18n: I18n
  ) {
    this.createForm();
  }

  createForm() {
    const formControls = {
      name: new FormControl({ value: null }),
      desc: new FormControl({ value: null }),
      long_desc: new FormControl({ value: null }),
      values: new FormGroup({}),
      default: new FormControl({ value: null }),
      daemon_default: new FormControl({ value: null }),
      services: new FormControl([])
    };

    this.availSections.forEach((section) => {
      formControls.values.addControl(section, new FormControl(null));
    });

    this.configForm = new CdFormGroup(formControls);
  }

  ngOnInit() {
    this.route.params.subscribe((params: { name: string }) => {
      const configName = params.name;
      this.configService.get(configName).subscribe((resp: ConfigFormModel) => {
        this.setResponse(resp);
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
  }

  getStep(type: string, value: number): number | undefined {
    const numberTypes = ['uint', 'int', 'size', 'secs'];

    if (numberTypes.includes(type)) {
      return 1;
    }

    if (type === 'float') {
      if (value !== null) {
        const stringVal = value.toString();
        if (stringVal.indexOf('.') !== -1) {
          // Value type float and contains decimal characters
          const decimal = value.toString().split('.');
          return Math.pow(10, -decimal[1].length);
        }
      }

      return 0.1;
    }

    return undefined;
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
        this.configForm
          .get('values')
          .get(value.section)
          .setValue(sectionValue);
      });
    }

    this.availSections.forEach((section) => {
      this.configForm
        .get('values')
        .get(section)
        .setValidators(validators);
    });

    const currentType = ConfigOptionTypes.getType(response.type);
    this.type = currentType.name;
    this.inputType = currentType.inputType;
    this.humanReadableType = currentType.humanReadable;
  }

  createRequest(): ConfigFormCreateRequestModel | null {
    const values = [];

    this.availSections.forEach((section) => {
      const sectionValue = this.configForm.getValue(section);
      if (sectionValue) {
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
            this.i18n('Config option {{name}} has been updated.', { name: request.name }),
            this.i18n('Update config option')
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
