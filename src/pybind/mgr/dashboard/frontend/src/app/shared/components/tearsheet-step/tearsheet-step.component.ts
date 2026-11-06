import {
  Component,
  ContentChild,
  Input,
  OnChanges,
  SimpleChanges,
  TemplateRef,
  ViewChild
} from '@angular/core';
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

  @Input() stepValid: boolean | null = null;

  readonly validityChange$ = new Subject<boolean>();

  ngOnChanges(changes: SimpleChanges): void {
    if ('stepValid' in changes) {
      this.validityChange$.next(this.canProceed);
    }
  }

  get resolvedFormGroup() {
    return this.stepComponent?.formGroup ?? null;
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
