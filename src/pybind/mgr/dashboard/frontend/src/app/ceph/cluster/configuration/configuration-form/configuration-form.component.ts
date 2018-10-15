import { Component, OnInit } from '@angular/core';
import { FormControl, FormGroup, ValidatorFn, Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';

import * as _ from 'lodash';

import { ConfigurationService } from '../../../../shared/api/configuration.service';
import { NotificationType } from '../../../../shared/enum/notification-type.enum';
import { CdFormGroup } from '../../../../shared/forms/cd-form-group';
import { CdValidators } from '../../../../shared/forms/cd-validators';
import { NotificationService } from '../../../../shared/services/notification.service';
import { ConfigFormCreateRequestModel } from './configuration-form-create-request.model';
import { ConfigFormModel } from './configuration-form.model';

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
    private notificationService: NotificationService
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
    this.configForm._filterValue = (value) => {
      return value;
    };
  }

  ngOnInit() {
    this.route.params.subscribe((params: { name: string }) => {
      const configName = params.name;
      this.configService.get(configName).subscribe((resp: ConfigFormModel) => {
        this.setResponse(resp);
      });
    });
  }

  getType(type: string): any {
    const knownTypes = [
      {
        name: 'uint64_t',
        inputType: 'number',
        humanReadable: 'Positive integer value',
        defaultMin: 0,
        patternHelpText: 'The entered value needs to be a positive number.',
        isNumberType: true,
        allowsNegative: false
      },
      {
        name: 'int64_t',
        inputType: 'number',
        humanReadable: 'Integer value',
        patternHelpText: 'The entered value needs to be a number.',
        isNumberType: true,
        allowsNegative: true
      },
      {
        name: 'size_t',
        inputType: 'number',
        humanReadable: 'Positive integer value (size)',
        defaultMin: 0,
        patternHelpText: 'The entered value needs to be a positive number.',
        isNumberType: true,
        allowsNegative: false
      },
      {
        name: 'secs',
        inputType: 'number',
        humanReadable: 'Positive integer value (secs)',
        defaultMin: 1,
        patternHelpText: 'The entered value needs to be a positive number.',
        isNumberType: true,
        allowsNegative: false
      },
      {
        name: 'double',
        inputType: 'number',
        humanReadable: 'Decimal value',
        patternHelpText: 'The entered value needs to be a number or decimal.',
        isNumberType: true,
        allowsNegative: true
      },
      { name: 'std::string', inputType: 'text', humanReadable: 'Text', isNumberType: false },
      {
        name: 'entity_addr_t',
        inputType: 'text',
        humanReadable: 'IPv4 or IPv6 address',
        patternHelpText: 'The entered value needs to be a valid IP address.',
        isNumberType: false
      },
      {
        name: 'uuid_d',
        inputType: 'text',
        humanReadable: 'UUID',
        patternHelpText:
          'The entered value is not a valid UUID, e.g.: 67dcac9f-2c03-4d6c-b7bd-1210b3a259a8',
        isNumberType: false
      },
      { name: 'bool', inputType: 'checkbox', humanReadable: 'Boolean value', isNumberType: false }
    ];

    let currentType = null;

    knownTypes.forEach((knownType) => {
      if (knownType.name === type) {
        currentType = knownType;
      }
    });

    if (currentType !== null) {
      return currentType;
    }

    throw new Error('Found unknown type "' + type + '" for config option.');
  }

  getValidators(configOption: any): ValidatorFn[] {
    const typeParams = this.getType(configOption.type);
    this.patternHelpText = typeParams.patternHelpText;

    if (typeParams.isNumberType) {
      const validators = [];

      if (configOption.max && configOption.max !== '') {
        this.maxValue = configOption.max;
        validators.push(Validators.max(configOption.max));
      }

      if ('min' in configOption && configOption.min !== '') {
        this.minValue = configOption.min;
        validators.push(Validators.min(configOption.min));
      } else if ('defaultMin' in typeParams) {
        this.minValue = typeParams.defaultMin;
        validators.push(Validators.min(typeParams.defaultMin));
      }

      if (configOption.type === 'double') {
        validators.push(CdValidators.decimalNumber());
      } else {
        validators.push(CdValidators.number(typeParams.allowsNegative));
      }

      return validators;
    } else if (configOption.type === 'entity_addr_t') {
      return [CdValidators.ip()];
    } else if (configOption.type === 'uuid_d') {
      return [CdValidators.uuid()];
    }
  }

  getStep(type: string, value: number): number | undefined {
    const numberTypes = ['uint64_t', 'int64_t', 'size_t', 'secs'];

    if (numberTypes.includes(type)) {
      return 1;
    }

    if (type === 'double') {
      if (value !== null) {
        const stringVal = value.toString();
        if (stringVal.indexOf('.') !== -1) {
          // Value type double and contains decimal characters
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

    const currentType = this.getType(response.type);
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
            'Config option ' + request.name + ' has been updated.',
            'Update config option'
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
