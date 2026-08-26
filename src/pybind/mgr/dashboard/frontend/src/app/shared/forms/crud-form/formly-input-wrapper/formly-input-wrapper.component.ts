import { Component } from '@angular/core';
import { FieldWrapper } from '@ngx-formly/core';
import { getFieldState } from '../helpers';

@Component({
  selector: 'cd-formly-input-wrapper',
  templateUrl: './formly-input-wrapper.component.html',
  standalone: false
})
export class FormlyInputWrapperComponent extends FieldWrapper {
  get helper(): string {
    const fieldState = getFieldState(this.field);
    return fieldState?.help || '';
  }

  get requiredLabel(): string {
    if (this.props?.required && this.props.hideRequiredMarker !== true) {
      return this.props.label || '';
    }
    return '';
  }
}
