import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { BsModalRef } from 'ngx-bootstrap/modal';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { BackButtonComponent } from '../back-button/back-button.component';
import { ModalComponent } from '../modal/modal.component';
import { SubmitButtonComponent } from '../submit-button/submit-button.component';
import { ConfirmationModalComponent } from './confirmation-modal.component';

describe('ConfirmationModalComponent', () => {
  let component: ConfirmationModalComponent;
  let fixture: ComponentFixture<ConfirmationModalComponent>;

  configureTestBed({
    declarations: [
      ConfirmationModalComponent,
      BackButtonComponent,
      SubmitButtonComponent,
      ModalComponent
    ],
    imports: [ReactiveFormsModule, RouterTestingModule],
    providers: [BsModalRef, i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ConfirmationModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
