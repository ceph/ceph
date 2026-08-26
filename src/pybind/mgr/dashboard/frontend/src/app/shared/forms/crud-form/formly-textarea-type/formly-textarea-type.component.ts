import { Component, ViewChild, ElementRef } from '@angular/core';
import { FieldType, FieldTypeConfig } from '@ngx-formly/core';
import { TextAreaJsonFormatterService } from '~/app/shared/services/text-area-json-formatter.service';
import { getFieldHelper, getFieldRequiredLabel } from '../helpers';

@Component({
  selector: 'cd-formly-textarea-type',
  templateUrl: './formly-textarea-type.component.html',
  standalone: false
})
export class FormlyTextareaTypeComponent extends FieldType<FieldTypeConfig> {
  @ViewChild('textArea')
  public textArea: ElementRef<any>;

  constructor(private textAreaJsonFormatterService: TextAreaJsonFormatterService) {
    super();
  }

  get helper(): string {
    return getFieldHelper(this.field);
  }

  get requiredLabel(): string {
    return getFieldRequiredLabel(this.field);
  }

  onChange() {
    this.textAreaJsonFormatterService.format(this.textArea);
  }
}
