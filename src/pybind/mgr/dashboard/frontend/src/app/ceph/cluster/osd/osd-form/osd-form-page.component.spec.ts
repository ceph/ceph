import { Component, Input, ViewChild } from '@angular/core';
import { FormGroup } from '@angular/forms';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';

import { SharedModule } from '~/app/shared/shared.module';
import { TearsheetStep } from '~/app/shared/models/tearsheet-step';
import { TearsheetComponent } from '~/app/shared/components/tearsheet/tearsheet.component';
import { TearsheetStepComponent } from '~/app/shared/components/tearsheet-step/tearsheet-step.component';
import { ActivatedRoute } from '@angular/router';
import { OsdFormPageComponent } from './osd-form-page.component';

@Component({
  selector: 'cd-osd-form',
  template: '',
  standalone: false
})
class MockOsdFormComponent implements TearsheetStep {
  @Input() hideTitle = false;
  @Input() hideSubmitBtn = false;
  @Input() useCarbonPageLayout = false;

  formGroup = new FormGroup({});
  isSubmitDisabled = false;
  submitLabel = 'Create OSDs';
  submit = jasmine.createSpy('submit');
}

@Component({
  template: '<cd-osd-form-page></cd-osd-form-page>',
  standalone: false
})
class HostComponent {
  @ViewChild(OsdFormPageComponent)
  page!: OsdFormPageComponent;

  @ViewChild(TearsheetComponent)
  tearsheet!: TearsheetComponent;
}

describe('OsdFormPageComponent', () => {
  let fixture: ComponentFixture<HostComponent>;
  let host: HostComponent;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [
        HostComponent,
        MockOsdFormComponent,
        OsdFormPageComponent,
        TearsheetComponent,
        TearsheetStepComponent
      ],
      imports: [SharedModule],
      providers: [
        {
          provide: ActivatedRoute,
          useValue: { outlet: 'primary' }
        }
      ]
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(HostComponent);
    host = fixture.componentInstance;
    fixture.detectChanges();
    host.page.osdForm = getOsdForm() as any;
    fixture.detectChanges();
  });

  const getOsdForm = () =>
    fixture.debugElement.query(By.directive(MockOsdFormComponent)).componentInstance as MockOsdFormComponent;

  it('should render a full-page tearsheet without a progress indicator for a single step', () => {
    const fullTearsheet = fixture.debugElement.query(By.css('.tearsheet--full'));
    expect(fullTearsheet).toBeTruthy();
    expect(fixture.debugElement.query(By.css('cds-progress-indicator'))).toBeFalsy();
  });

  it('should pass embedded-mode flags through to the OSD form', () => {
    const osdForm = getOsdForm();
    expect(osdForm.hideTitle).toBe(true);
    expect(osdForm.hideSubmitBtn).toBe(true);
    expect(osdForm.useCarbonPageLayout).toBe(true);
  });

  it('should use the OSD form submit label on the tearsheet submit button', () => {
    const osdForm = getOsdForm();
    osdForm.submitLabel = 'Preview';
    fixture.detectChanges();

    const submitButton = fixture.debugElement.query(By.css('.tearsheet-footer button[cdsButton="primary"]'));
    expect(submitButton.nativeElement.textContent).toContain('Preview');
  });

  it('should disable the submit button when the OSD form reports a disabled state', () => {
    const osdForm = getOsdForm();
    osdForm.isSubmitDisabled = true;
    fixture.detectChanges();

    const submitButton = fixture.debugElement.query(By.css('.tearsheet-footer button[cdsButton="primary"]'));
    expect(submitButton.nativeElement.disabled).toBe(true);
  });

  it('should delegate submit to the OSD form', () => {
    const osdForm = getOsdForm();
    const submitButton = fixture.debugElement.query(By.css('.tearsheet-footer button[cdsButton="primary"]'));
    submitButton.nativeElement.click();

    expect(osdForm.submit).toHaveBeenCalled();
  });
});
