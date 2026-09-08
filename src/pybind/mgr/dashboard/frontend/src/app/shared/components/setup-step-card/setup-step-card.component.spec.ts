import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SetupStepCardComponent } from './setup-step-card.component';

describe('SetupStepCardComponent', () => {
  let component: SetupStepCardComponent;
  let fixture: ComponentFixture<SetupStepCardComponent>;

  const STEP_TITLE = 'Example step';
  const STEP_DESCRIPTION = 'Example step description.';
  const SUCCESS_MESSAGE = 'Example configured successfully.';
  const INFO_MESSAGE = 'Example not configured yet.';

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [SetupStepCardComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(SetupStepCardComponent);
    component = fixture.componentInstance;
    component.stepNumber = 1;
    component.title = STEP_TITLE;
    component.description = STEP_DESCRIPTION;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should render step number, title, and description', () => {
    const element = fixture.nativeElement;
    expect(element.textContent).toContain('1.');
    expect(element.textContent).toContain(STEP_TITLE);
    expect(element.textContent).toContain(STEP_DESCRIPTION);
  });

  describe('statusMessage', () => {
    it('should return successMessage when configured', () => {
      component.isConfigured = true;
      component.successMessage = SUCCESS_MESSAGE;

      expect(component.statusMessage).toBe(SUCCESS_MESSAGE);
    });

    it('should return infoMessage when not configured', () => {
      component.isConfigured = false;
      component.infoMessage = INFO_MESSAGE;

      expect(component.statusMessage).toBe(INFO_MESSAGE);
    });

    it('should return default success text when configured without successMessage', () => {
      component.isConfigured = true;
      component.successMessage = undefined;

      expect(component.statusMessage).toBe('Configured successfully.');
    });

    it('should return default info text when not configured without infoMessage', () => {
      component.isConfigured = false;
      component.infoMessage = undefined;

      expect(component.statusMessage).toBe('Not configured yet.');
    });

    it('should prefer successMessage over infoMessage when configured', () => {
      component.isConfigured = true;
      component.successMessage = SUCCESS_MESSAGE;
      component.infoMessage = INFO_MESSAGE;

      expect(component.statusMessage).toBe(SUCCESS_MESSAGE);
    });
  });

  describe('template', () => {
    it('should render the status message when not configured', () => {
      component.isConfigured = false;
      component.infoMessage = INFO_MESSAGE;
      fixture.detectChanges();

      const message = fixture.nativeElement.querySelector('.setup-step-card__info span');
      expect(message.textContent.trim()).toBe(INFO_MESSAGE);
    });

    it('should render the status message when configured', () => {
      component.isConfigured = true;
      component.successMessage = SUCCESS_MESSAGE;
      fixture.detectChanges();

      const message = fixture.nativeElement.querySelector('.setup-step-card__info span');
      expect(message.textContent.trim()).toBe(SUCCESS_MESSAGE);
    });

    it('should render a success icon when configured', () => {
      component.isConfigured = true;
      component.successMessage = SUCCESS_MESSAGE;
      fixture.detectChanges();

      const icon = fixture.nativeElement.querySelector('cd-icon');
      expect(icon.getAttribute('ng-reflect-type')).toBe('success');
    });

    it('should render an info icon when not configured', () => {
      component.isConfigured = false;
      component.infoMessage = INFO_MESSAGE;
      fixture.detectChanges();

      const icon = fixture.nativeElement.querySelector('cd-icon');
      expect(icon.getAttribute('ng-reflect-type')).toBe('infoCircle');
    });

    it('should render loading panel when isLoading is true', () => {
      component.isLoading = true;
      fixture.detectChanges();

      const loadingPanel = fixture.nativeElement.querySelector('cd-loading-panel');
      expect(loadingPanel).toBeTruthy();
    });

    it('should return "Loading..." as statusMessage when isLoading is true', () => {
      component.isLoading = true;
      expect(component.statusMessage).toBe('Loading...');
    });

    it('should apply loading class when isLoading is true', () => {
      component.isLoading = true;
      fixture.detectChanges();

      const card = fixture.nativeElement.querySelector('.setup-step-card');
      expect(card.classList.contains('setup-step-card--loading')).toBe(true);
    });
  });
});
