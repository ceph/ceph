import { ComponentFixture, TestBed } from '@angular/core/testing';
import { Component, ViewChild } from '@angular/core';
import { By } from '@angular/platform-browser';
import { SharedModule } from '../../shared.module';
import { TearsheetStepComponent } from '../tearsheet-step/tearsheet-step.component';
import { TearsheetComponent } from './tearsheet.component';
import { ActivatedRoute } from '@angular/router';

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
  });
});
