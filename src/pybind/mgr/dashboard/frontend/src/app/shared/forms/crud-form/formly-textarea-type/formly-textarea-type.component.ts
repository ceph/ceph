import { Component, ViewChild, ElementRef } from '@angular/core';
import { FieldType, FieldTypeConfig } from '@ngx-formly/core';

@Component({
  selector: 'cd-formly-textarea-type',
  templateUrl: './formly-textarea-type.component.html',
  styleUrls: ['./formly-textarea-type.component.scss']
})
export class FormlyTextareaTypeComponent extends FieldType<FieldTypeConfig> {
  @ViewChild('textArea')
  public textArea: ElementRef<any>;

  onChange() {
    const value = this.textArea.nativeElement.value;
    try {
      const formatted = JSON.stringify(JSON.parse(value), null, 2);
      this.textArea.nativeElement.value = formatted;
      this.textArea.nativeElement.style.height = 'auto';
      const lineNumber = formatted.split('\n').length;
      const pixelPerLine = 25;
      const pixels = lineNumber * pixelPerLine;
      this.textArea.nativeElement.style.height = pixels + 'px';
    } catch (e) {}
  }
}
