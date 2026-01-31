import { Component, ContentChild, TemplateRef, ViewChild } from '@angular/core';
import { TearsheetStep } from '../../models/tearsheet-step';

@Component({
  selector: 'cd-tearsheet-step',
  standalone: false,
  templateUrl: './tearsheet-step.component.html',
  styleUrls: ['./tearsheet-step.component.scss']
})
export class TearsheetStepComponent {
  @ViewChild(TemplateRef, { static: true })
  template!: TemplateRef<any>;

  @ContentChild('tearsheetStep')
  stepComponent!: TearsheetStep;
}
