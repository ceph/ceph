import { ComponentFixture, TestBed } from '@angular/core/testing';
import { Component, ViewChild } from '@angular/core';
import { By } from '@angular/platform-browser';
import { SharedModule } from '../../shared.module';
import { TearsheetStepComponent } from '../tearsheet-step/tearsheet-step.component';
import { TearsheetComponent, TearsheetOverflowScroll } from './tearsheet.component';
import { ActivatedRoute } from '@angular/router';

// Mock Component that uses tearsheet
@Component({
  template: `
    <cd-tearsheet
      [steps]="steps"
      [title]="title"
      [description]="description"
      [overflowScroll]="overflowScroll"
      (submitRequested)="onSubmit()"
    >
      <cd-tearsheet-step [stepValid]="step1Valid">
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
    { label: 'Step 1', complete: false },
    { label: 'Step 2', complete: false },
    { label: 'Step 3', complete: false }
  ];
  title = 'Test Title';
  description = 'Test Description';
  overflowScroll?: TearsheetOverflowScroll;
  /** null = no validation (default); false = force invalid; true = force valid */
  step1Valid: boolean | null = null;

  onSubmit() {}

  @ViewChild(TearsheetComponent)
  tearsheet!: TearsheetComponent;
}

describe('TearsheetComponent', () => {
  let hostFixture: ComponentFixture<MockHostComponent>;
  let hostComponent: MockHostComponent;
  let tearsheetComponent: TearsheetComponent;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [TearsheetComponent, TearsheetStepComponent, MockHostComponent],
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

  describe('overflowScroll', () => {
    it('should not set inline overflow when overflowScroll is unset', () => {
      const content = hostFixture.debugElement.query(By.css('.tearsheet-content'));
      expect(content.nativeElement.style.overflow).toBe('');
    });

    it('should apply overflow style when overflowScroll is set', () => {
      hostComponent.overflowScroll = 'hidden';
      hostFixture.detectChanges();
      const content = hostFixture.debugElement.query(By.css('.tearsheet-content'));
      expect(content.nativeElement.style.overflow).toBe('hidden');
    });

    it('should enable scrolling when overflowScroll is auto', () => {
      hostComponent.overflowScroll = 'auto';
      hostFixture.detectChanges();
      const content = hostFixture.debugElement.query(By.css('.tearsheet-content'));
      expect(content.nativeElement.style.overflow).toBe('auto');
    });
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
      hostComponent.step1Valid = false;
      hostFixture.detectChanges();
      tearsheetComponent.onNext();
      expect(tearsheetComponent.currentStep).toBe(0);
    });

    it('should disable next button when current step is invalid', () => {
      hostComponent.steps = hostComponent.steps.map((step, i) =>
        i === 0 ? { ...step, invalid: true } : step
      );
      hostFixture.detectChanges();
      const buttons = hostFixture.debugElement.queryAll(
        By.css('.tearsheet-footer button[cdsButton="primary"]')
      );
      const nextBtn = buttons.find((btn) => btn.nativeElement.textContent.trim() === 'Next');
      expect(nextBtn).toBeTruthy();
      expect(nextBtn?.nativeElement.disabled).toBe(true);
    });
  });

  describe('[stepValid] seeding and live sync', () => {
    it('should seed step as invalid immediately when stepValid starts false', () => {
      hostComponent.step1Valid = false;
      hostFixture.detectChanges();
      // After detectChanges the tearsheet re-runs setup() which seeds the flag.
      expect(tearsheetComponent.steps[0].invalid).toBe(true);
    });

    it('should seed step as valid when stepValid starts true', () => {
      hostComponent.step1Valid = true;
      hostFixture.detectChanges();
      expect(tearsheetComponent.steps[0].invalid).toBe(false);
    });

    it('should clear invalid flag when stepValid changes from false to true', () => {
      hostComponent.step1Valid = false;
      hostFixture.detectChanges();
      expect(tearsheetComponent.steps[0].invalid).toBe(true);

      hostComponent.step1Valid = true;
      hostFixture.detectChanges();
      expect(tearsheetComponent.steps[0].invalid).toBe(false);
    });

    it('should set invalid flag when stepValid changes from true to false', () => {
      hostComponent.step1Valid = true;
      hostFixture.detectChanges();
      expect(tearsheetComponent.steps[0].invalid).toBe(false);

      hostComponent.step1Valid = false;
      hostFixture.detectChanges();
      expect(tearsheetComponent.steps[0].invalid).toBe(true);
    });

    it('should not seed invalid for steps with stepValid null (default)', () => {
      // Steps 2 and 3 have stepValid=null, so they must never be seeded invalid.
      expect(tearsheetComponent.steps[1].invalid).toBeFalsy();
      expect(tearsheetComponent.steps[2].invalid).toBeFalsy();
    });

    it('should keep Next enabled for steps that never use [stepValid]', () => {
      // steps[1] has no [stepValid] binding and no #tearsheetStep form —
      // Next must stay enabled regardless.
      tearsheetComponent.currentStep = 1;
      hostFixture.detectChanges();
      const buttons = hostFixture.debugElement.queryAll(
        By.css('.tearsheet-footer button[cdsButton="primary"]')
      );
      const nextBtn = buttons.find((btn) => btn.nativeElement.textContent.trim() === 'Next');
      expect(nextBtn?.nativeElement.disabled).toBe(false);
    });
  });
});
