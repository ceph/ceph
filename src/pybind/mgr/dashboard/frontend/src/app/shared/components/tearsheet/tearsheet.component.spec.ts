import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { Component, ViewChild } from '@angular/core';
import { By } from '@angular/platform-browser';
import { AbstractControl, FormControl, FormGroup, Validators } from '@angular/forms';
import { SharedModule } from '../../shared.module';
import { TearsheetStepComponent } from '../tearsheet-step/tearsheet-step.component';
import { TearsheetComponent } from './tearsheet.component';
import { ActivatedRoute } from '@angular/router';
import { Observable, of } from 'rxjs';
import { delay } from 'rxjs/operators';

// Mock Component that uses tearsheet
@Component({
  template: `
    <cd-tearsheet
      [steps]="steps"
      [title]="title"
      [description]="description"
      (submitRequested)="onSubmit()"
    >
      <cd-tearsheet-step>
        <div class="step-1-content">Step 1 Content</div>
      </cd-tearsheet-step>
      <cd-tearsheet-step>
        <div class="step-2-content">Step 2 Content</div>
      </cd-tearsheet-step>
      <cd-tearsheet-step>
        <div class="step-3-content">Step 3 Content</div>
      </cd-tearsheet-step>
    </cd-tearsheet>
  `,
  standalone: false
})
class MockHostComponent {
  steps = [
    {
      label: 'Step 1',
      complete: false,
      invalid: false
    },
    {
      label: 'Step 2',
      complete: false
    },
    {
      label: 'Step 3',
      complete: false
    }
  ];
  title = 'Test Title';
  description = 'Test Description';

  onSubmit() {}

  @ViewChild(TearsheetComponent)
  tearsheet!: TearsheetComponent;
}

@Component({
  selector: 'cd-mock-form-step',
  template: '',
  standalone: false
})
class MockFormStepComponent {
  formGroup = new FormGroup({
    parent: new FormGroup({
      child: new FormControl('', Validators.required)
    })
  });
}

@Component({
  selector: 'cd-mock-async-form-step',
  template: '',
  standalone: false
})
class MockAsyncFormStepComponent {
  formGroup = new FormGroup({
    name: new FormControl('default-name', {
      validators: [Validators.required],
      asyncValidators: [
        (_control: AbstractControl): Observable<{ notUnique: boolean } | null> =>
          of(null).pipe(delay(10))
      ]
    }),
    requiredField: new FormControl('', Validators.required)
  });
}

@Component({
  template: `
    <cd-tearsheet [steps]="steps" [title]="title" [description]="description">
      <cd-tearsheet-step>
        <cd-mock-async-form-step #tearsheetStep></cd-mock-async-form-step>
      </cd-tearsheet-step>
      <cd-tearsheet-step>
        <div>Step 2</div>
      </cd-tearsheet-step>
    </cd-tearsheet>
  `,
  standalone: false
})
class MockAsyncFormHostComponent {
  steps = [
    { label: 'Step 1', complete: false },
    { label: 'Step 2', complete: false }
  ];
  title = 'Async Form Host';
  description = 'Async Form Host Description';

  @ViewChild(TearsheetComponent)
  tearsheet!: TearsheetComponent;

  @ViewChild(MockAsyncFormStepComponent)
  formStep!: MockAsyncFormStepComponent;
}

@Component({
  template: `
    <cd-tearsheet [steps]="steps" [title]="title" [description]="description">
      <cd-tearsheet-step>
        <cd-mock-form-step #tearsheetStep></cd-mock-form-step>
      </cd-tearsheet-step>
      <cd-tearsheet-step>
        <div>Step 2</div>
      </cd-tearsheet-step>
    </cd-tearsheet>
  `,
  standalone: false
})
class MockFormHostComponent {
  steps = [
    { label: 'Step 1', complete: false },
    { label: 'Step 2', complete: false }
  ];
  title = 'Form Host';
  description = 'Form Host Description';

  @ViewChild(TearsheetComponent)
  tearsheet!: TearsheetComponent;

  @ViewChild(MockFormStepComponent)
  formStep!: MockFormStepComponent;
}

describe('TearsheetComponent', () => {
  let hostFixture: ComponentFixture<MockHostComponent>;
  let hostComponent: MockHostComponent;
  let tearsheetComponent: TearsheetComponent;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [
        TearsheetComponent,
        TearsheetStepComponent,
        MockHostComponent,
        MockFormStepComponent,
        MockFormHostComponent,
        MockAsyncFormStepComponent,
        MockAsyncFormHostComponent
      ],
      imports: [SharedModule],
      providers: [
        {
          provide: ActivatedRoute,
          useValue: { outlet: 'modal' }
        }
      ]
    }).compileComponents();
  });

  beforeEach(() => {
    hostFixture = TestBed.createComponent(MockHostComponent);
    hostComponent = hostFixture.componentInstance;
    hostFixture.detectChanges();
    tearsheetComponent = hostComponent.tearsheet;
  });

  it('should create component', () => {
    expect(tearsheetComponent).toBeTruthy();
  });

  it('should have 3 steps from input', () => {
    expect(tearsheetComponent.steps.length).toBe(3);
  });

  it('should have title from input', () => {
    expect(tearsheetComponent.title).toBe('Test Title');
  });

  it('should have description from input', () => {
    expect(tearsheetComponent.description).toBe('Test Description');
  });

  it('should detect 3 step children via ContentChildren', () => {
    expect(tearsheetComponent.stepContents).toBeDefined();
    expect(tearsheetComponent.stepContents.length).toBe(3);
  });

  it('should have first step selected by default', () => {
    expect(tearsheetComponent.currentStep).toBe(0);
    const firstStep = tearsheetComponent.stepContents.first;
    expect(firstStep).toBeDefined();
  });

  it('should render step content', () => {
    const step1Content = hostFixture.debugElement.query(By.css('.step-1-content'));
    expect(step1Content).toBeTruthy();
    expect(step1Content.nativeElement.textContent).toContain('Step 1 Content');
  });

  it('should emit submitRequested event', () => {
    spyOn(hostComponent, 'onSubmit');

    tearsheetComponent.submitRequested.emit();

    expect(hostComponent.onSubmit).toHaveBeenCalled();
  });

  describe('step navigation', () => {
    it('should go to next step', () => {
      tearsheetComponent.onNext();
      expect(tearsheetComponent.currentStep).toBe(1);
    });

    it('should go to previous step', () => {
      tearsheetComponent.currentStep = 2;
      tearsheetComponent.onPrevious();
      expect(tearsheetComponent.currentStep).toBe(1);
    });

    it('should not go beyond last step', () => {
      tearsheetComponent.currentStep = 2;
      tearsheetComponent.onNext();
      expect(tearsheetComponent.currentStep).toBe(2);
    });

    it('should not go before first step', () => {
      tearsheetComponent.currentStep = 0;
      tearsheetComponent.onPrevious();
      expect(tearsheetComponent.currentStep).toBe(0);
    });

    it('should not go to next step on invalid', () => {
      tearsheetComponent.currentStep = 0;
      hostComponent.steps[0].invalid = true;
      tearsheetComponent.onNext();
      expect(tearsheetComponent.currentStep).toBe(0);
    });

    it('should keep Next enabled when current step is invalid so users can retry', () => {
      hostComponent.steps = hostComponent.steps.map((step, i) =>
        i === 0 ? { ...step, invalid: true } : step
      );
      hostFixture.detectChanges();
      const buttons = hostFixture.debugElement.queryAll(
        By.css('.tearsheet-footer button[cdsButton="primary"]')
      );
      const nextBtn = buttons.find((btn) => btn.nativeElement.textContent.trim() === 'Next');
      expect(nextBtn).toBeTruthy();
      expect(nextBtn?.nativeElement.disabled).toBe(false);
    });
  });

  describe('nested form validation on next', () => {
    it('should mark nested controls touched on next without dirtying them', () => {
      const formHostFixture = TestBed.createComponent(MockFormHostComponent);
      formHostFixture.detectChanges();
      const formHost = formHostFixture.componentInstance;

      const childControl = formHost.formStep.formGroup.get('parent.child');
      expect(childControl).toBeTruthy();
      expect(childControl?.dirty).toBe(false);
      expect(childControl?.touched).toBe(false);

      formHost.tearsheet.onNext();

      // Touch shows errors; do not mark dirty (that re-triggers async validators).
      expect(childControl?.dirty).toBe(false);
      expect(childControl?.touched).toBe(true);
      expect(childControl?.hasError('required')).toBe(true);
      expect(formHost.tearsheet.currentStep).toBe(0);
    });

    it('should advance after async validators complete when required fields are filled', fakeAsync(() => {
      const formHostFixture = TestBed.createComponent(MockAsyncFormHostComponent);
      formHostFixture.detectChanges();
      const formHost = formHostFixture.componentInstance;

      formHost.formStep.formGroup.get('requiredField')?.setValue('filled');
      formHostFixture.detectChanges();

      formHost.tearsheet.onNext();
      expect(formHost.tearsheet.currentStep).toBe(0);

      tick(10);
      formHostFixture.detectChanges();

      expect(formHost.tearsheet.currentStep).toBe(1);
      expect(formHost.tearsheet.steps[0].invalid).toBe(false);
    }));

    it('should stay on step when invalid then advance after the field is fixed', fakeAsync(() => {
      const formHostFixture = TestBed.createComponent(MockAsyncFormHostComponent);
      formHostFixture.detectChanges();
      const formHost = formHostFixture.componentInstance;

      // First Next with empty required field: stay on step; Next remains usable.
      formHost.tearsheet.onNext();
      tick(10);
      formHostFixture.detectChanges();
      expect(formHost.tearsheet.currentStep).toBe(0);

      formHost.formStep.formGroup.get('requiredField')?.setValue('filled');
      tick(10);
      formHostFixture.detectChanges();

      formHost.tearsheet.onNext();
      tick(10);
      formHostFixture.detectChanges();
      expect(formHost.tearsheet.currentStep).toBe(1);
    }));

    it('should submit after validity refresh settles async validators', fakeAsync(() => {
      const formHostFixture = TestBed.createComponent(MockAsyncFormHostComponent);
      formHostFixture.detectChanges();
      const formHost = formHostFixture.componentInstance;
      const submitSpy = jest.spyOn(formHost.tearsheet.submitRequested, 'emit');

      formHost.formStep.formGroup.get('requiredField')?.setValue('filled');
      // Settle the initial async validator from control creation.
      tick(10);
      formHostFixture.detectChanges();

      formHost.tearsheet.currentStep = 1;
      formHostFixture.detectChanges();

      formHost.tearsheet.onSubmit();
      // refreshControlValidity re-runs async validators; wait for them to settle.
      expect(submitSpy).not.toHaveBeenCalled();
      tick(10);
      formHostFixture.detectChanges();

      expect(submitSpy).toHaveBeenCalledTimes(1);
      expect(submitSpy).toHaveBeenCalledWith(
        expect.objectContaining({ name: 'default-name', requiredField: 'filled' })
      );
    }));

    it('should navigate to the first invalid step instead of silently blocking Create', fakeAsync(() => {
      const formHostFixture = TestBed.createComponent(MockAsyncFormHostComponent);
      formHostFixture.detectChanges();
      const formHost = formHostFixture.componentInstance;
      const submitSpy = jest.spyOn(formHost.tearsheet.submitRequested, 'emit');

      tick(10);
      formHost.tearsheet.currentStep = 1;
      formHostFixture.detectChanges();

      formHost.tearsheet.onSubmit();
      tick(10);
      formHostFixture.detectChanges();

      expect(submitSpy).not.toHaveBeenCalled();
      expect(formHost.tearsheet.currentStep).toBe(0);
      expect(formHost.tearsheet.steps[0].invalid).toBe(true);
    }));

    it('should not advance if the user navigated away before the async validator settled', fakeAsync(() => {
      const formHostFixture = TestBed.createComponent(MockAsyncFormHostComponent);
      formHostFixture.detectChanges();
      const formHost = formHostFixture.componentInstance;

      // Fill the required field so the form would be valid once async settles.
      formHost.formStep.formGroup.get('requiredField')?.setValue('filled');
      formHostFixture.detectChanges();

      // Trigger Next — async validator is in-flight, step stays at 0.
      formHost.tearsheet.onNext();
      expect(formHost.tearsheet.currentStep).toBe(0);

      // User navigates back to step 0 then clicks step 1 directly (or onStepSelect),
      // simulating navigation away while the validator is still pending.
      formHost.tearsheet.currentStep = 1;
      formHostFixture.detectChanges();

      // Async validator settles — callback must not advance further (step should stay at 1).
      tick(10);
      formHostFixture.detectChanges();

      expect(formHost.tearsheet.currentStep).toBe(1);
    }));
  });
});
