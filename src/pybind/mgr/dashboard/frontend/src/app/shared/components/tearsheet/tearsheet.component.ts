import {
  Component,
  ContentChildren,
  EventEmitter,
  Input,
  OnInit,
  Output,
  QueryList,
  AfterViewInit,
  DestroyRef,
  OnDestroy
} from '@angular/core';
import { FormBuilder } from '@angular/forms';
import { Step } from 'carbon-components-angular';
import { TearsheetStepComponent } from '../tearsheet-step/tearsheet-step.component';
import { ModalCdsService } from '../../services/modal-cds.service';
import { ActivatedRoute } from '@angular/router';
import { Location } from '@angular/common';
import { ConfirmationModalComponent } from '../confirmation-modal/confirmation-modal.component';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { Subject } from 'rxjs';

/**
<cd-tearsheet
    [steps]="steps"
    [title]="title"
    [isSubmitLoading]="isSubmitLoading"
    [description]="description"
    (submitRequested)="onSubmit()">
  <cd-tearsheet-step>
      <cd-step #tearsheetStep>
      </cds-step>
  </cd-tearsheet-step>
   <cd-tearsheet-step>
      step 2 form
  <cd-tearsheet-step>
</cd-tearsheet>

-----------------

@Component({
  selector: 'cd-step',
  template: `<form></form>,
  standalone: false
})
export class StepComponent implements TearsheetStep {
formgroup: CdFormGroup;
}
**/
@Component({
  selector: 'cd-tearsheet',
  standalone: false,
  templateUrl: './tearsheet.component.html',
  styleUrls: ['./tearsheet.component.scss']
})
export class TearsheetComponent implements OnInit, AfterViewInit, OnDestroy {
  @Input() title!: string;
  @Input() steps!: Array<Step>;
  @Input() description!: string;
  @Input() type: 'full' | 'wide' = 'wide';
  @Input() submitButtonLabel: string = $localize`Create`;
  @Input() submitButtonLoadingLabel: string = $localize`Creating`;
  @Input() isSubmitLoading: boolean = true;

  @Output() submitRequested = new EventEmitter<any[]>();

  @ContentChildren(TearsheetStepComponent)
  stepContents!: QueryList<TearsheetStepComponent>;

  get activeStepTemplate() {
    return this.stepContents?.toArray()[this.currentStep]?.template;
  }

  currentStep: number = 0;
  lastStep: number = null;
  isOpen: boolean = true;
  hasModalOutlet: boolean = false;
  private destroy$ = new Subject<void>();

  constructor(
    protected formBuilder: FormBuilder,
    private cdsModalService: ModalCdsService,
    private route: ActivatedRoute,
    private location: Location,
    private destroyRef: DestroyRef
  ) {}

  ngOnInit() {
    this.lastStep = this.steps.length - 1;
    this.hasModalOutlet = this.route.outlet === 'modal';
  }

  private _updateStepInvalid(index: number, invalid: boolean) {
    this.steps = this.steps.map((step, i) => (i === index ? { ...step, invalid } : step));
  }

  onStepSelect(event: { step: Step; index: number }) {
    this.currentStep = event.index;
  }

  closeTearsheet() {
    if (this.type === 'full') {
      this.closeFullTearsheet();
    } else {
      this.closeWideTearsheet();
    }
  }

  closeWideTearsheet() {
    this.isOpen = false;
    if (this.hasModalOutlet) {
      this.location.back();
    } else {
      this.cdsModalService.dismissAll();
    }
  }

  onPrevious() {
    if (this.currentStep !== 0) {
      this.currentStep = this.currentStep - 1;
    }
  }

  onNext() {
    if (this.currentStep !== this.lastStep && !this.steps[this.currentStep].invalid) {
      this.currentStep = this.currentStep + 1;
    }
  }

  getMergedPayload(): any {
    return this.stepContents.toArray().reduce((acc, wrapper) => {
      const stepFormValue = wrapper.stepComponent.formGroup.value;
      return { ...acc, ...stepFormValue };
    }, {});
  }

  handleSubmit() {
    if (this.steps[this.currentStep].invalid) return;

    const mergedPayloads = this.getMergedPayload();

    this.submitRequested.emit(mergedPayloads);
  }

  closeFullTearsheet() {
    this.cdsModalService.show(ConfirmationModalComponent, {
      titleText: $localize`Are you sure you want to cancel ?`,
      description: $localize`If you cancel, the information you have entered won't be saved.`,
      buttonText: $localize`Cancel`,
      cancelText: $localize`Return to form`,
      onSubmit: () => {
        this.isOpen = false;
        this.cdsModalService.dismissAll();
        this.location.back();
      },
      submitBtnType: 'danger',
      showCancel: true
    });
  }

  ngAfterViewInit() {
    if (!this.stepContents?.length) return;

    this.stepContents.forEach((wrapper, index) => {
      const form = wrapper.stepComponent?.formGroup;
      if (!form) return;

      // initial state
      this._updateStepInvalid(index, form.invalid);

      form.statusChanges.pipe(takeUntilDestroyed(this.destroyRef)).subscribe(() => {
        this._updateStepInvalid(index, form.invalid);
      });
    });
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
