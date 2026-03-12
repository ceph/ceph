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

  get rightInfluencer(): TemplateRef<any> | null {
    return this.stepComponent?.rightInfluencer ?? null;
  }

  get showRightInfluencer(): boolean {
    return this.stepComponent?.showRightInfluencer
      ? this.stepComponent.showRightInfluencer()
      : false;
  }
}
