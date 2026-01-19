import { Component, TemplateRef, ViewChild } from '@angular/core';

@Component({
  selector: 'cd-tearsheet-step',
  templateUrl: './tearsheet-step.component.html',
  styleUrls: ['./tearsheet-step.component.scss']
})
export class TearsheetStepComponent {
  @ViewChild(TemplateRef, { static: true })
  template!: TemplateRef<any>;
}
