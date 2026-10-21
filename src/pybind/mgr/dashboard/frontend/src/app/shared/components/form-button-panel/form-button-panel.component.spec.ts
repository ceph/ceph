import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { configureTestBed } from '~/testing/unit-test-helper';
import { FormButtonPanelComponent } from './form-button-panel.component';
import { ModalModule } from 'carbon-components-angular';
import { RouterTestingModule } from '@angular/router/testing';

describe('FormButtonPanelComponent', () => {
  let component: FormButtonPanelComponent;
  let fixture: ComponentFixture<FormButtonPanelComponent>;

  configureTestBed({
    declarations: [FormButtonPanelComponent],
    schemas: [NO_ERRORS_SCHEMA],
    imports: [ModalModule, RouterTestingModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(FormButtonPanelComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('modal footer', () => {
    const submitButton = () => fixture.nativeElement.querySelector('cd-submit-button');

    beforeEach(() => {
      component.modalForm = true;
    });

    it('should make the submit button fill the footer when cancel is shown', () => {
      component.showCancel = true;
      fixture.detectChanges();

      expect(submitButton().classList).toContain('w-100');
      expect(submitButton().classList).not.toContain('w-50');
    });

    it('should make the submit button cover half of the footer when cancel is hidden', () => {
      component.showCancel = false;
      fixture.detectChanges();

      expect(submitButton().classList).toContain('w-50');
      expect(submitButton().classList).not.toContain('w-100');
    });
  });
});
