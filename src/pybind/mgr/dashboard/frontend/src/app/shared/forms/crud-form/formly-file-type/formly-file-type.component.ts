import { Component } from '@angular/core';
import { FieldType, FieldTypeConfig } from '@ngx-formly/core';
import { getFieldHelper, getFieldRequiredLabel } from '../helpers';

@Component({
  selector: 'cd-formly-file-type',
  templateUrl: './formly-file-type.component.html',
  standalone: false
})
export class FormlyFileTypeComponent extends FieldType<FieldTypeConfig> {
  get helper(): string {
    return getFieldHelper(this.field);
  }

  get requiredLabel(): string {
    return getFieldRequiredLabel(this.field);
  }
}
