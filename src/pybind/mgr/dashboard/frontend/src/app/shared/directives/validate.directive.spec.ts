import { Component, DebugElement, ChangeDetectionStrategy } from '@angular/core';
import { TestBed, ComponentFixture } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import {
  ReactiveFormsModule,
  FormControl,
  Validators,
  FormGroup,
  FormGroupDirective
} from '@angular/forms';
import { ValidateDirective } from './validate.directive';

// A test host component to simulate directive usage with a Reactive Form
@Component({
  template: `
    <form [formGroup]="form" (ngSubmit)="onSubmit()">
      <input formControlName="testField" cdValidate #cdValidateRef="cdValidate" />
      <button type="submit">Submit</button>
    </form>
  `,
  standalone: false,
  // Using OnPush strategy to properly test cdr.markForCheck()
  changeDetection: ChangeDetectionStrategy.OnPush
})
class TestHostComponent {
  form = new FormGroup({
    testField: new FormControl('', Validators.required)
  });

  onSubmit() {}
}

describe('ValidateDirective', () => {
  let fixture: ComponentFixture<TestHostComponent>;
  let hostComponent: TestHostComponent;
  let inputElement: DebugElement;
  let directiveInstance: ValidateDirective;
  let formGroupDir: FormGroupDirective;
  let control: FormControl;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ValidateDirective, TestHostComponent],
      imports: [ReactiveFormsModule]
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(TestHostComponent);
    hostComponent = fixture.componentInstance;
    inputElement = fixture.debugElement.query(By.directive(ValidateDirective));

    directiveInstance = inputElement.injector.get(ValidateDirective);
    formGroupDir = inputElement.injector.get(FormGroupDirective);

    control = hostComponent.form.get('testField') as FormControl;

    fixture.detectChanges();
  });

  it('should create an instance', () => {
    expect(directiveInstance).toBeTruthy();
  });

  it('should not show error initially (clean and untouched)', () => {
    expect(directiveInstance.isInvalid).toBeFalsy();
  });

  it('should show error when control is invalid and touched', () => {
    control.markAsTouched();
    control.setValue('');
    fixture.detectChanges();
    expect(directiveInstance.isInvalid).toBeTruthy();
  });

  it('should show error when control is invalid and dirty', () => {
    control.markAsDirty();
    control.setValue('');
    fixture.detectChanges();

    expect(directiveInstance.isInvalid).toBeTruthy();
  });

  it('should show error when form is submitted, even if control is untouched/clean', () => {
    control.setValue('');
    // Trigger the actual form submit event via button click
    const submitButton = fixture.debugElement.query(By.css('button[type="submit"]'));
    submitButton.nativeElement.click();
    fixture.detectChanges();
    expect(formGroupDir.submitted).toBeTruthy();
    expect(directiveInstance.isInvalid).toBeTruthy();
  });

  it('should hide error when control becomes valid', () => {
    // Mark form invalid
    control.markAsTouched();
    control.setValue('');
    fixture.detectChanges();
    expect(directiveInstance.isInvalid).toBeTruthy();

    // Mark form valid
    control.setValue('a valid password');
    fixture.detectChanges();

    expect(directiveInstance.isInvalid).toBeFalsy();
  });

  it('should use cdr.markForCheck() to ensure view updates in OnPush components', () => {
    const cdrSpy = spyOn((directiveInstance as any).cdr, 'markForCheck').and.callThrough();

    // Triggers updateState() internally
    control.markAsTouched();
    control.setValue('');

    expect(cdrSpy).toHaveBeenCalled();
  });

  it('should clean up subscriptions on ngOnDestroy', () => {
    const destroySpy = spyOn((directiveInstance as any).destroy$, 'complete').and.callThrough();
    fixture.destroy();
    expect(destroySpy).toHaveBeenCalled();
  });
});
