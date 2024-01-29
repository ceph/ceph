import { Component, ViewChild, ElementRef } from '@angular/core';
import { FieldType, FieldTypeConfig } from '@ngx-formly/core';
import { TextAreaJsonFormatterService } from '~/app/shared/services/text-area-json-formatter.service';

@Component({
  selector: 'cd-formly-textarea-type',
  templateUrl: './formly-textarea-type.component.html',
  styleUrls: ['./formly-textarea-type.component.scss']
})
export class FormlyTextareaTypeComponent extends FieldType<FieldTypeConfig> {
  @ViewChild('textArea')
  public textArea: ElementRef<any>;

  constructor(private textAreaJsonFormatterService: TextAreaJsonFormatterService) {
    super();
  }

  onChange() {
    this.textAreaJsonFormatterService.format(this.textArea);
  }
}
