import { Component } from '@angular/core';
import { FieldType, FieldTypeConfig } from '@ngx-formly/core';

@Component({
  selector: 'cd-formly-input-type',
  templateUrl: './formly-input-type.component.html',
  styleUrls: ['./formly-input-type.component.scss']
})
export class FormlyInputTypeComponent extends FieldType<FieldTypeConfig> {}
