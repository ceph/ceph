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
  OnDestroy,
  ChangeDetectionStrategy,
  TemplateRef
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
  styleUrls: ['./tearsheet.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class TearsheetComponent implements OnInit, AfterViewInit, OnDestroy {
  @Input() title!: string;
  @Input() steps!: Array<Step>;
  @Input() description!: string;
  @Input() type: 'full' | 'wide' = 'wide';
  @Input() size: 'xs' | 'sm' | 'md' | 'lg' = 'lg';
  @Input() submitButtonLabel: string = $localize`Create`;
  @Input() submitButtonLoadingLabel: string = $localize`Creating`;
  @Input() isSubmitLoading: boolean = true;

  @Output() submitRequested = new EventEmitter<void>();
  @Output() closeRequested = new EventEmitter<void>();
  @Output() stepChanged = new EventEmitter<{ current: number }>();

  @ContentChildren(TearsheetStepComponent)
  stepContents!: QueryList<TearsheetStepComponent>;

  get activeStepTemplate() {
    return this.stepContents?.toArray()[this.currentStep]?.template;
  }

  get rightInfluencerTemplate(): TemplateRef<any> | null {
    return this.stepContents?.toArray()[this.currentStep]?.rightInfluencer ?? null;
  }

  get showRightInfluencer(): boolean {
    return this.stepContents?.toArray()[this.currentStep]?.showRightInfluencer;
  }

  getStepValue<T = any>(index: number): T | null {
    const wrapper = this.stepContents?.toArray()?.[index];
    return wrapper?.stepComponent?.formGroup?.value ?? null;
  }

  getStepIndexByLabel(label: string): number {
    return this.steps?.findIndex((s) => s.label === label) ?? -1;
  }

  getStepValueByLabel<T = any>(label: string): T | null {
    const idx = this.getStepIndexByLabel(label);
    if (idx < 0) return null;
    return this.getStepValue<T>(idx);
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
    this.stepChanged.emit({ current: this.currentStep });
  }

  closeTearsheet() {
    if (this.type === 'full') {
      this.closeFullTearsheet();
    } else {
      this.closeWideTearsheet();
    }
  }

  closeWideTearsheet() {
    this.closeRequested.emit();
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
      this.stepChanged.emit({ current: this.currentStep });
    }
  }

  onNext() {
    const formEl = document.querySelector('form');
    formEl?.dispatchEvent(new Event('submit', { bubbles: true }));
    if (this.currentStep !== this.lastStep && !this.steps[this.currentStep].invalid) {
      this.currentStep = this.currentStep + 1;
      this.stepChanged.emit({ current: this.currentStep });
    }
  }

  getMergedPayload(): any {
    return this.stepContents.toArray().reduce((acc, wrapper) => {
      const stepFormValue = wrapper.stepComponent.formGroup.value;
      return { ...acc, ...stepFormValue };
    }, {});
  }

  handleSubmit() {
    if (this.steps.some((step) => step?.invalid)) return;

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
    const setup = () => {
      // keep lastStep in sync with steps input
      this.lastStep = this.steps.length - 1;

      // clamp currentStep so template lookup never goes out of range
      if (this.currentStep > this.lastStep) {
        this.currentStep = this.lastStep;
      }

      // subscribe to each form statusChanges
      this.stepContents.forEach((wrapper, index) => {
        const form = wrapper.stepComponent?.formGroup;
        if (!form) return;

        form.statusChanges
          .pipe(takeUntilDestroyed(this.destroyRef))
          .subscribe(() => this._updateStepInvalid(index, form.invalid));
      });
    };

    setup();

    this.stepContents.changes.pipe(takeUntilDestroyed(this.destroyRef)).subscribe(() => setup());
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
