import { Validators } from '@angular/forms';

import * as _ from 'lodash';

import { CdValidators } from '../../forms/cd-validators';
import { ConfigFormModel } from './config-option.model';

export class ConfigOptionTypes {
  // TODO: I18N
  private static knownTypes: Array<any> = [
    {
      name: 'uint',
      inputType: 'number',
      humanReadable: 'Unsigned integer value',
      defaultMin: 0,
      patternHelpText: 'The entered value needs to be an unsigned number.',
      isNumberType: true,
      allowsNegative: false
    },
    {
      name: 'int',
      inputType: 'number',
      humanReadable: 'Integer value',
      patternHelpText: 'The entered value needs to be a number.',
      isNumberType: true,
      allowsNegative: true
    },
    {
      name: 'size',
      inputType: 'number',
      humanReadable: 'Unsigned integer value (>=16bit)',
      defaultMin: 0,
      patternHelpText: 'The entered value needs to be a unsigned number.',
      isNumberType: true,
      allowsNegative: false
    },
    {
      name: 'secs',
      inputType: 'number',
      humanReadable: 'Number of seconds',
      defaultMin: 1,
      patternHelpText: 'The entered value needs to be a number >= 1.',
      isNumberType: true,
      allowsNegative: false
    },
    {
      name: 'float',
      inputType: 'number',
      humanReadable: 'Double value',
      patternHelpText: 'The entered value needs to be a number or decimal.',
      isNumberType: true,
      allowsNegative: true
    },
    { name: 'str', inputType: 'text', humanReadable: 'Text', isNumberType: false },
    {
      name: 'addr',
      inputType: 'text',
      humanReadable: 'IPv4 or IPv6 address',
      patternHelpText: 'The entered value needs to be a valid IP address.',
      isNumberType: false
    },
    {
      name: 'uuid',
      inputType: 'text',
      humanReadable: 'UUID',
      patternHelpText:
        'The entered value is not a valid UUID, e.g.: 67dcac9f-2c03-4d6c-b7bd-1210b3a259a8',
      isNumberType: false
    },
    { name: 'bool', inputType: 'checkbox', humanReadable: 'Boolean value', isNumberType: false }
  ];

  public static getType(type: string): any {
    const currentType = _.find(this.knownTypes, (t) => {
      return t.name === type;
    });

    if (currentType !== undefined) {
      return currentType;
    }

    throw new Error('Found unknown type "' + type + '" for config option.');
  }

  public static getTypeValidators(configOption: ConfigFormModel): any {
    const typeParams = ConfigOptionTypes.getType(configOption.type);

    if (typeParams.name === 'bool' || typeParams.name === 'str') {
      return;
    }

    const typeValidators = { validators: [], patternHelpText: typeParams.patternHelpText };

    if (typeParams.isNumberType) {
      if (configOption.max && configOption.max !== '') {
        typeValidators['max'] = configOption.max;
        typeValidators.validators.push(Validators.max(configOption.max));
      }

      if (configOption.min && configOption.min !== '') {
        typeValidators['min'] = configOption.min;
        typeValidators.validators.push(Validators.min(configOption.min));
      } else if ('defaultMin' in typeParams) {
        typeValidators['min'] = typeParams.defaultMin;
        typeValidators.validators.push(Validators.min(typeParams.defaultMin));
      }

      if (configOption.type === 'float') {
        typeValidators.validators.push(CdValidators.decimalNumber());
      } else {
        typeValidators.validators.push(CdValidators.number(typeParams.allowsNegative));
      }
    } else if (configOption.type === 'addr') {
      typeValidators.validators = [CdValidators.ip()];
    } else if (configOption.type === 'uuid') {
      typeValidators.validators = [CdValidators.uuid()];
    }

    return typeValidators;
  }

  public static getTypeStep(type: string, value: number): number | undefined {
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
}
