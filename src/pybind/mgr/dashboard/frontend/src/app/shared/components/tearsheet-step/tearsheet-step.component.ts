import {
  Component,
  ContentChild,
  Input,
  OnChanges,
  SimpleChanges,
  TemplateRef,
  ViewChild
} from '@angular/core';
import { FormGroupDirective } from '@angular/forms';
import { Subject } from 'rxjs';
import { TearsheetStep } from '../../models/tearsheet-step';

@Component({
  selector: 'cd-tearsheet-step',
  standalone: false,
  templateUrl: './tearsheet-step.component.html',
  styleUrls: ['./tearsheet-step.component.scss']
})
export class TearsheetStepComponent implements OnChanges {
  @ViewChild(TemplateRef, { static: true })
  template!: TemplateRef<any>;

  @ContentChild('tearsheetStep')
  stepComponent!: TearsheetStep;

  /**
   * Optional fallback when the step uses an inline `<form [formGroup]>` without
   * `#tearsheetStep`. Off by default so existing steps (e.g. OSD form) keep
   * previous canProceed behavior.
   */
  @ContentChild(FormGroupDirective)
  formGroupDirective?: FormGroupDirective;

  @Input() stepValid: boolean | null = null;

  /** Opt in to using an inline FormGroupDirective for Next/Submit validity. */
  @Input() autoDetectFormGroup = false;

  readonly validityChange$ = new Subject<boolean>();

  ngOnChanges(changes: SimpleChanges): void {
    if ('stepValid' in changes || 'autoDetectFormGroup' in changes) {
      this.validityChange$.next(this.canProceed);
    }
  }

  get resolvedFormGroup() {
    const projectedFormGroup = this.stepComponent?.formGroup ?? null;
    if (projectedFormGroup) {
      return projectedFormGroup;
    }
    if (this.autoDetectFormGroup) {
      return this.formGroupDirective?.form ?? null;
    }
    return null;
  }

  get canProceed(): boolean {
    if (this.stepValid !== null) return this.stepValid;
    if (this.resolvedFormGroup) return this.resolvedFormGroup.valid;
    return true;
  }

  get rightInfluencer(): TemplateRef<any> | null {
    return this.stepComponent?.rightInfluencer ?? null;
  }

  get showRightInfluencer(): boolean {
    return this.stepComponent?.showRightInfluencer
      ? this.stepComponent.showRightInfluencer()
      : false;
  }
}
