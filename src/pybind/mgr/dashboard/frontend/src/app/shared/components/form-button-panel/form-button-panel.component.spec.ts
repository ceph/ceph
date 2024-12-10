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
});
