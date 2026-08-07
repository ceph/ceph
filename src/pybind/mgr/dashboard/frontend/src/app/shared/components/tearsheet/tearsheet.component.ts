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
  ChangeDetectorRef,
  TemplateRef,
  ViewEncapsulation
} from '@angular/core';
import { AbstractControl, FormArray, FormBuilder, FormGroup } from '@angular/forms';
import { Step } from 'carbon-components-angular';
import { TearsheetStepComponent } from '../tearsheet-step/tearsheet-step.component';
import { ModalCdsService } from '../../services/modal-cds.service';
import { ActivatedRoute } from '@angular/router';
import { Location } from '@angular/common';
import { ConfirmationModalComponent } from '../confirmation-modal/confirmation-modal.component';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { forkJoin, Subject } from 'rxjs';
import { filter, finalize, startWith, take, takeUntil } from 'rxjs/operators';

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
  templateUrl: './tearsheet.component.html',
  styleUrls: ['./tearsheet.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  encapsulation: ViewEncapsulation.None
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

  /** Merged step form values for consumers that bind `(submitRequested)="onSubmit($event)"`. */
  @Output() submitRequested = new EventEmitter<Record<string, unknown>>();
  @Output() closeRequested = new EventEmitter<void>();
  @Output() stepChanged = new EventEmitter<{ current: number }>();

  @ContentChildren(TearsheetStepComponent)
  stepContents!: QueryList<TearsheetStepComponent>;

  private advancingDueToAsync = false;
  private submittingDueToAsync = false;
  /** Snapshot of each step's form value taken when leaving the step. */
  private stepValueCache = new WeakMap<TearsheetStepComponent, Record<string, unknown>>();

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
  /** Tears down per-setup subscriptions when ContentChildren change. */
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

  private _updateStepInvalid(index: number, invalid: boolean) {
    this.steps = this.steps.map((step, i) => (i === index ? { ...step, invalid } : step));
    // statusChanges / async validators run outside the OnPush event path;
    // mark dirty so UI state re-renders.
    this.cdr.markForCheck();
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

  private getStepForm(wrapper: TearsheetStepComponent | undefined): FormGroup | null {
    return wrapper?.stepComponent?.formGroup ?? null;
  }

  onNext() {
    const wrapper = this.stepContents?.toArray()?.[this.currentStep];
    const currentForm = this.getStepForm(wrapper);
    // Touch for error display, then refresh each control so cdValidate /
    // Carbon invalid bindings update. Do NOT markAsDirty — that re-triggers
    // pristine-skipping async validators (e.g. NQN unique).
    currentForm?.markAllAsTouched();
    this.refreshControlValidity(currentForm);

    // If an async validator is already in-flight (user edited NQN), wait for it.
    if (currentForm?.pending) {
      if (this.advancingDueToAsync) {
        return;
      }
      this.advancingDueToAsync = true;
      // Snapshot the step index now; the user may navigate away before the
      // validator settles, so we must re-check both the index and the active
      // wrapper on arrival and skip the advance if either has changed.
      const stepBeingValidated = this.currentStep;
      currentForm.statusChanges
        .pipe(
          startWith(currentForm.status),
          filter((status) => status !== 'PENDING'),
          take(1),
          takeUntil(this.setupTeardown$),
          finalize(() => {
            this.advancingDueToAsync = false;
          })
        )
        .subscribe(() => {
          const activeWrapper = this.stepContents?.toArray()?.[stepBeingValidated];
          if (this.currentStep === stepBeingValidated && activeWrapper === wrapper) {
            this.advanceFromCurrentStep(wrapper);
          }
        });
      return;
    }

    this.advanceFromCurrentStep(wrapper);
  }

  /**
   * Re-run validators and emit statusChanges on every control without marking
   * them dirty. Needed so cdValidate picks up touched+invalid after Next.
   */
  private refreshControlValidity(control: AbstractControl | null) {
    if (!control) {
      return;
    }
    if (control instanceof FormGroup || control instanceof FormArray) {
      Object.values(control.controls).forEach((child) => this.refreshControlValidity(child));
    }
    control.updateValueAndValidity({ onlySelf: true, emitEvent: true });
  }

  private advanceFromCurrentStep(wrapper: TearsheetStepComponent | undefined) {
    // Prefer live form validity; fall back to steps[].invalid for formless steps.
    // Next stays enabled; we only show field errors and refuse to leave the step.
    const form = this.getStepForm(wrapper);
    const canAdvance = form ? form.valid : !this.steps[this.currentStep]?.invalid;
    if (this.currentStep !== this.lastStep && canAdvance) {
      this._updateStepInvalid(this.currentStep, false);
      if (wrapper) {
        this.cacheStepValue(wrapper);
      }
      this.currentStep = this.currentStep + 1;
      this.stepChanged.emit({ current: this.currentStep });
      this.cdr.markForCheck();
    } else if (form) {
      this._updateStepInvalid(this.currentStep, form.invalid);
    }
  }

  private cacheStepValue(wrapper: TearsheetStepComponent) {
    const value = wrapper.stepComponent?.formGroup?.value as Record<string, unknown> | null;
    if (value) {
      this.stepValueCache.set(wrapper, { ...value });
    }
  }

  getMergedPayload(): any {
    return this.stepContents.toArray().reduce((acc, wrapper) => {
      const liveValue = wrapper.stepComponent?.formGroup?.value;
      const cachedValue = this.stepValueCache.get(wrapper);
      return { ...acc, ...(liveValue ?? cachedValue ?? {}) };
    }, {});
  }

  // Wide tearsheet template binds submit to handleSubmit().
  handleSubmit() {
    this.onSubmit();
  }

  onSubmit() {
    if (this.submittingDueToAsync) {
      return;
    }

    // Cache whatever is still mounted before validating/submitting.
    this.stepContents?.forEach((wrapper) => this.cacheStepValue(wrapper));

    const wrappers = this.stepContents?.toArray() ?? [];
    wrappers.forEach((wrapper) => {
      const form = this.getStepForm(wrapper);
      if (!form) return;
      form.markAllAsTouched();
      this.refreshControlValidity(form);
    });

    this.finishSubmit();
  }

  private waitForFormsToSettle(forms: FormGroup[], onSettled: () => void) {
    if (!forms.length) {
      onSettled();
      return;
    }
    this.submittingDueToAsync = true;
    forkJoin(
      forms.map((form) =>
        form.statusChanges.pipe(
          startWith(form.status),
          filter((status) => status !== 'PENDING'),
          take(1)
        )
      )
    )
      .pipe(
        takeUntil(this.setupTeardown$),
        finalize(() => {
          this.submittingDueToAsync = false;
        })
      )
      .subscribe(() => onSettled());
  }

  private finishSubmit() {
    const wrappers = this.stepContents?.toArray() ?? [];
    const forms = wrappers
      .map((wrapper) => this.getStepForm(wrapper))
      .filter((form): form is FormGroup => !!form);

    const pendingForms = forms.filter((form) => form.pending);
    if (pendingForms.length) {
      this.waitForFormsToSettle(pendingForms, () => this.finishSubmit());
      return;
    }

    let firstInvalid = -1;
    wrappers.forEach((wrapper, index) => {
      const form = this.getStepForm(wrapper);
      if (form) {
        this._updateStepInvalid(index, form.invalid);
        if (form.invalid && firstInvalid < 0) {
          firstInvalid = index;
        }
      } else if (this.steps[index]?.invalid) {
        if (firstInvalid < 0) {
          firstInvalid = index;
        }
      } else {
        // Form not currently resolvable (step content unmounted). Trust cache /
        // earlier navigation — do not block Create on a stale steps[].invalid flag.
        this._updateStepInvalid(index, false);
      }
    });

    if (firstInvalid >= 0) {
      this.currentStep = firstInvalid;
      this.stepChanged.emit({ current: this.currentStep });
      this.cdr.markForCheck();
      return;
    }

    this.submitRequested.emit(this.getMergedPayload());
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

      // keep lastStep in sync with steps input
      this.lastStep = this.steps.length - 1;

      // clamp currentStep so template lookup never goes out of range
      if (this.currentStep > this.lastStep) {
        this.currentStep = this.lastStep;
      }

      // subscribe to each form statusChanges
      this.stepContents.forEach((wrapper, index) => {
        // Do not seed or sync form.invalid onto the Next button — Next stays
        // enabled so users can click it, see field errors (e.g. subnet-mask),
        // fix them, and click Next again. Advance is still gated in onNext().
        const form = this.getStepForm(wrapper);
        if (!form) return;

        form.statusChanges.pipe(takeUntil(this.setupTeardown$)).subscribe(() => {
          if (form.pending) {
            return;
          }
          // Clear step invalid once the form becomes valid again after a
          // failed Next attempt (field-level errors are handled by cdValidate).
          if (form.valid) {
            this._updateStepInvalid(index, false);
          }
        });
      });

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
