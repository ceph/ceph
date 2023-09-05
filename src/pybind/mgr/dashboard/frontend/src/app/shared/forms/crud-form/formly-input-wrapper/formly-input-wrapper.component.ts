import { Component } from '@angular/core';
import { FieldWrapper } from '@ngx-formly/core';
import { getFieldState } from '../helpers';

@Component({
  selector: 'cd-formly-input-wrapper',
  templateUrl: './formly-input-wrapper.component.html',
  styleUrls: ['./formly-input-wrapper.component.scss']
})
export class FormlyInputWrapperComponent extends FieldWrapper {
  get helper(): string {
    const fieldState = getFieldState(this.field);
    return fieldState?.help || '';
  }
}
