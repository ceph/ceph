import { Component } from '@angular/core';
import { FieldType, FieldTypeConfig } from '@ngx-formly/core';
import { getFieldState } from '../helpers';

export interface FormlySelectOption {
  label: string;
  value: string;
}

@Component({
  selector: 'cd-formly-select-type',
  templateUrl: './formly-select-type.component.html',
  styleUrls: ['./formly-select-type.component.scss'],
  standalone: false
})
export class FormlySelectTypeComponent extends FieldType<FieldTypeConfig> {
  get selectOptions(): FormlySelectOption[] {
    const options = this.props?.options;
    if (!Array.isArray(options)) {
      return [];
    }
    return options.map((opt: string | FormlySelectOption) => {
      if (typeof opt === 'object') {
        return { label: opt.label, value: String(opt.value) };
      }
      return { label: opt, value: opt };
    });
  }

  get helper(): string {
    return getFieldState(this.field)?.help || '';
  }

  get requiredLabel(): string {
    if (this.props?.required && this.props.hideRequiredMarker !== true) {
      return this.props.label || '';
    }
    return '';
  }
}
