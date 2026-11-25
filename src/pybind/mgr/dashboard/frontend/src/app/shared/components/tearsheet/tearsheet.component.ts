import {
  ChangeDetectorRef,
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
  OnChanges,
  SimpleChanges,
  ChangeDetectionStrategy,
  TemplateRef,
  ViewEncapsulation
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
import { takeUntil } from 'rxjs/operators';

export type TearsheetOverflowScroll = 'auto' | 'hidden' | 'visible' | 'scroll';

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
  changeDetection: ChangeDetectionStrategy.OnPush,
  encapsulation: ViewEncapsulation.None
})
export class TearsheetComponent implements OnInit, AfterViewInit, OnDestroy, OnChanges {
  @Input() title!: string;
  @Input() steps!: Array<Step>;
  @Input() description!: string;
  @Input() type: 'full' | 'wide' = 'wide';
  @Input() size: 'xs' | 'sm' | 'md' | 'lg' = 'lg';
  @Input() submitButtonLabel: string = $localize`Create`;
  @Input() submitButtonLoadingLabel: string = $localize`Creating`;
  @Input() isSubmitLoading: boolean = false;
  /** When set, applies `overflow` on the tearsheet content area; omit to use stylesheet defaults. */
  @Input() overflowScroll?: TearsheetOverflowScroll;
  @Input() hideInfluencer: boolean = false;
  @Input() successIcon: boolean = false;
  @Input() headerTestId?: string;

  @Output() submitRequested = new EventEmitter<void>();
  @Output() closeRequested = new EventEmitter<void>();
  @Output() stepChanged = new EventEmitter<{ current: number }>();
  @Output() validateStep = new EventEmitter<{ step: number }>();

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

  get contentOverflowStyle(): { overflow: TearsheetOverflowScroll } | null {
    if (!this.overflowScroll) {
      return null;
    }
    return { overflow: this.overflowScroll };
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
  lastStep: number | null = null;
  isOpen: boolean = true;
  hasModalOutlet: boolean = false;
  private destroy$ = new Subject<void>();
  private setupTeardown$ = new Subject<void>();

  constructor(
    protected formBuilder: FormBuilder,
    private cdsModalService: ModalCdsService,
    private route: ActivatedRoute,
    private location: Location,
    private destroyRef: DestroyRef,
    private cdr: ChangeDetectorRef
  ) {}

  ngOnInit() {
    this.lastStep = this.steps.length - 1;
    this.hasModalOutlet = this.route.outlet === 'modal';
  }

  ngOnChanges(changes: SimpleChanges) {
    if (changes['steps']) {
      this.lastStep = this.steps.length - 1;
      if (this.currentStep > this.lastStep) {
        this.currentStep = this.lastStep;
      }
      this.cdr.markForCheck();
    }
  }

  private _updateStepInvalid(index: number, invalid: boolean) {
    this.steps = this.steps.map((step, i) => (i === index ? { ...step, invalid } : step));
  }

  onStepSelect(event: { step: Step; index: number }) {
    if (this.isStepNavBlocked(event.index)) {
      return;
    }
    this.currentStep = event.index;
    this.stepChanged.emit({ current: this.currentStep });
    this.cdr.markForCheck();
  }

  private isStepNavBlocked(index: number): boolean {
    if (this.steps[index]?.disabled) {
      return true;
    }
    if (index > this.currentStep && this.steps[this.currentStep]?.invalid) {
      return true;
    }
    for (let i = 0; i < index; i++) {
      if (this.steps[i]?.invalid || this.steps[i]?.disabled) {
        return true;
      }
    }
    return false;
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
    if (this.closeRequested.observers.length > 0) {
      this.closeRequested.emit();
      return;
    }
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
      this.cdr.markForCheck();
    }
  }

  onNext() {
    this.validateStep.emit({ step: this.currentStep });

    const wrapper = this.stepContents?.toArray()?.[this.currentStep];
    const legacyForm = wrapper?.resolvedFormGroup;
    if (legacyForm) {
      legacyForm.markAllAsTouched();
      legacyForm.updateValueAndValidity({ emitEvent: true });
      this._updateStepInvalid(this.currentStep, legacyForm.invalid);
    }

    const canAdvance = wrapper ? wrapper.canProceed : true;
    this._updateStepInvalid(this.currentStep, !canAdvance);
    if (this.currentStep !== this.lastStep && canAdvance) {
      this.currentStep = this.currentStep + 1;
      this.stepChanged.emit({ current: this.currentStep });
      this.cdr.markForCheck();
    } else if (!canAdvance) {
      this.cdr.markForCheck();
    }
  }

  getMergedPayload(): any {
    return this.stepContents.toArray().reduce((acc, wrapper) => {
      const stepFormValue = wrapper.stepComponent?.formGroup?.value;
      return { ...acc, ...stepFormValue };
    }, {});
  }

  onSubmit() {
    this.stepContents?.forEach((wrapper, index) => {
      const form = wrapper.resolvedFormGroup;
      if (!form) return;
      form.markAllAsTouched();
      form.updateValueAndValidity({ emitEvent: true });
      this._updateStepInvalid(index, form.invalid);
    });

    const wrappers = this.stepContents?.toArray() ?? [];
    const anyStepInvalid = this.steps.some(
      (step, index) => step?.invalid || (wrappers[index] ? !wrappers[index].canProceed : false)
    );
    if (anyStepInvalid) return;

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
      // Cancel all subscriptions created by the previous setup run before
      // re-subscribing, so that removed steps do not retain observers.
      this.setupTeardown$.next();

      this.lastStep = this.steps.length - 1;

      if (this.currentStep > this.lastStep) {
        this.currentStep = this.lastStep;
      }

      this.stepContents.forEach((wrapper, index) => {
        // Path 1: step uses a formGroup via #tearsheetStep — subscribe to its
        // statusChanges so the flag stays in sync as the user types.
        // Initial state is NOT seeded here: these forms intentionally start
        // with Next enabled so the user can navigate freely before touching fields.
        const form = wrapper.resolvedFormGroup;
        if (form) {
          form.statusChanges
            .pipe(takeUntil(this.setupTeardown$))
            .subscribe(() => this._updateStepInvalid(index, form.invalid));
        }

        // Path 2: step uses [stepValid] input binding (no formGroup reference).
        // Always subscribe to validityChange$ so any future [stepValid] binding
        // is tracked. When stepValid is already set at setup time, also seed the
        // initial invalid state so Next is correctly disabled from first render.
        if (wrapper.stepValid !== null) {
          this._updateStepInvalid(index, !wrapper.canProceed);
        }
        wrapper.validityChange$.pipe(takeUntil(this.setupTeardown$)).subscribe((canProceed) => {
          this._updateStepInvalid(index, !canProceed);
          this.cdr.markForCheck();
        });
      });

      // After seeding stepValid-based steps, force OnPush to re-render.
      this.cdr.markForCheck();
    };

    setup();

    this.stepContents.changes.pipe(takeUntilDestroyed(this.destroyRef)).subscribe(() => setup());
  }

  ngOnDestroy() {
    this.setupTeardown$.next();
    this.setupTeardown$.complete();
    this.destroy$.next();
    this.destroy$.complete();
  }
}
