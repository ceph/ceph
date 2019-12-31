import { ValidatorFn } from '@angular/forms';

export class CdFormModalFieldConfig {
  name: string;
  // 'binary' will use cdDimlessBinary directive on input element
  // 'select' will use select element
  type: 'number' | 'text' | 'binary' | 'select';
  label?: string;
  required?: boolean;
  value?: any;
  errors?: { [errorName: string]: string };
  validators: ValidatorFn[];
  // only for type select
  placeholder?: string;
  options?: Array<{
    text: string;
    value: any;
  }>;
}
