import { Component, Input, OnInit } from '@angular/core';
import { UntypedFormControl, NgForm } from '@angular/forms';

import _ from 'lodash';

import { ConfigurationService } from '~/app/shared/api/configuration.service';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { ConfigOptionTypes } from './config-option.types';

@Component({
  selector: 'cd-config-option',
  templateUrl: './config-option.component.html',
  styleUrls: ['./config-option.component.scss']
})
export class ConfigOptionComponent implements OnInit {
  @Input()
  optionNames: Array<string> = [];
  @Input()
  optionsForm: CdFormGroup = new CdFormGroup({});
  @Input()
  optionsFormDir: NgForm = new NgForm([], []);
  @Input()
  optionsFormGroupName = '';
  @Input()
  optionsFormShowReset = true;

  icons = Icons;
  options: Array<any> = [];
  optionsFormGroup: CdFormGroup = new CdFormGroup({});

  constructor(private configService: ConfigurationService) {}

  private static optionNameToText(optionName: string): string {
    const sections = ['mon', 'mgr', 'osd', 'mds', 'client'];
    return optionName
      .split('_')
      .filter((c, index) => index !== 0 || !sections.includes(c))
      .map((c) => c.charAt(0).toUpperCase() + c.substring(1))
      .join(' ');
  }

  ngOnInit() {
    this.createForm();
    this.loadStoredData();
  }

  private createForm() {
    this.optionsForm.addControl(this.optionsFormGroupName, this.optionsFormGroup);
    this.optionNames.forEach((optionName) => {
      this.optionsFormGroup.addControl(optionName, new UntypedFormControl(null));
    });
  }

  getStep(type: string, value: any): number | undefined {
    return ConfigOptionTypes.getTypeStep(type, value);
  }

  private loadStoredData() {
    this.configService.filter(this.optionNames).subscribe((data: any) => {
      this.options = data.map((configOption: any) => {
        const formControl = this.optionsForm.get(configOption.name);
        const typeValidators = ConfigOptionTypes.getTypeValidators(configOption);
        configOption.additionalTypeInfo = ConfigOptionTypes.getType(configOption.type);

        // Set general information and value
        configOption.text = ConfigOptionComponent.optionNameToText(configOption.name);
        configOption.value = _.find(configOption.value, (p) => {
          return p.section === 'osd'; // TODO: Can handle any other section
        });
        if (configOption.value) {
          if (configOption.additionalTypeInfo.name === 'bool') {
            formControl.setValue(configOption.value.value === 'true');
          } else {
            formControl.setValue(configOption.value.value);
          }
        }

        // Set type information and validators
        if (typeValidators) {
          configOption.patternHelpText = typeValidators.patternHelpText;
          if ('max' in typeValidators && typeValidators.max !== '') {
            configOption.maxValue = typeValidators.max;
          }
          if ('min' in typeValidators && typeValidators.min !== '') {
            configOption.minValue = typeValidators.min;
          }
          formControl.setValidators(typeValidators.validators);
        }

        return configOption;
      });
    });
  }

  saveValues() {
    const options = {};
    this.optionNames.forEach((optionName) => {
      const optionValue = this.optionsForm.getValue(optionName);
      if (optionValue !== null && optionValue !== '') {
        options[optionName] = {
          section: 'osd', // TODO: Can handle any other section
          value: optionValue
        };
      }
    });

    return this.configService.bulkCreate({ options: options });
  }

  resetValue(optionName: string) {
    this.configService.delete(optionName, 'osd').subscribe(
      // TODO: Can handle any other section
      () => {
        const formControl = this.optionsForm.get(optionName);
        formControl.reset();
      }
    );
  }
}
