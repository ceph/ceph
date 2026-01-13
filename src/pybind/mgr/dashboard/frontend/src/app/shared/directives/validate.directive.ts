import { Directive, OnInit, OnDestroy, ChangeDetectorRef, Optional, Self } from '@angular/core';
import { NgControl, FormGroupDirective } from '@angular/forms';
import { Subject, merge } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

/**
 * This directive validates form inputs for reactive forms.
 * The form should have [formGroup] directive.
 *
 * Example:
 *
 *  <cds-password-label
      [invalid]="oldPasswordRef.showError">
      Old Password
      <input cdsPassword
             cdValidate
             #oldPasswordRef="cdValidate"
             formControlName="oldpassword"
             [invalid]="oldPasswordRef.showError">
 */
@Directive({
  selector: '[cdValidate]',
  exportAs: 'cdValidate',
  standalone: false
})
export class ValidateDirective implements OnInit, OnDestroy {
  private destroy$ = new Subject<void>();
  isInvalid = false;

  constructor(
    @Optional() @Self() private ngControl: NgControl,
    @Optional() private formGroupDir: FormGroupDirective,
    private cdr: ChangeDetectorRef
  ) {}

  ngOnInit() {
    if (!this.ngControl?.control) return;

    const submit$ = this.formGroupDir ? this.formGroupDir.ngSubmit : new Subject();

    merge(this.ngControl.control.statusChanges, submit$)
      .pipe(takeUntil(this.destroy$))
      .subscribe(() => {
        this.updateState();
      });
  }

  private updateState() {
    const control = this.ngControl.control;
    if (!control) return;

    const isInvalid = control.invalid;

    const wasSubmitted = this.formGroupDir?.submitted;
    const userInteracted = control.dirty || control.touched;

    this.isInvalid = !!(isInvalid && (userInteracted || wasSubmitted));

    this.cdr.markForCheck();
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
