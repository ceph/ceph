import { ValidatorFn } from '@angular/forms';

export class CdFormModalFieldConfig {
  // --- Generic field properties ---
  name: string;
  // 'binary' will use cdDimlessBinary directive on input element
  // 'select' will use select element
  type: 'number' | 'text' | 'binary' | 'select' | 'select-badges';
  label?: string;
  required?: boolean;
  value?: any;
  errors?: { [errorName: string]: string };
  validators: ValidatorFn[];

  // --- Specific field properties ---
  typeConfig?: {
    [prop: string]: any;
    // 'select':
    // ---------
    // placeholder?: string;
    // options?: Array<{
    //   text: string;
    //   value: any;
    // }>;
    //
    // 'select-badges':
    // ----------------
    // customBadges: boolean;
    // options: Array<SelectOption>;
    // messages: SelectMessages;
  };
}
