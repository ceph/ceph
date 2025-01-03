import { HttpClientTestingModule } from '@angular/common/http/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { of } from 'rxjs';

import { OsdService } from '~/app/shared/api/osd.service';
import { BackButtonComponent } from '~/app/shared/components/back-button/back-button.component';
import { ModalComponent } from '~/app/shared/components/modal/modal.component';
import { SubmitButtonComponent } from '~/app/shared/components/submit-button/submit-button.component';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { configureTestBed } from '~/testing/unit-test-helper';
import { OsdReweightModalComponent } from './osd-reweight-modal.component';

describe('OsdReweightModalComponent', () => {
  let component: OsdReweightModalComponent;
  let fixture: ComponentFixture<OsdReweightModalComponent>;

  configureTestBed({
    imports: [ReactiveFormsModule, HttpClientTestingModule, RouterTestingModule],
    declarations: [
      OsdReweightModalComponent,
      ModalComponent,
      SubmitButtonComponent,
      BackButtonComponent
    ],
    schemas: [NO_ERRORS_SCHEMA],
    providers: [OsdService, NgbActiveModal, CdFormBuilder]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(OsdReweightModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should call OsdService::reweight() on submit', () => {
    component.osdId = 1;
    component.reweightForm.get('weight').setValue(0.5);

    const osdServiceSpy = spyOn(TestBed.inject(OsdService), 'reweight').and.callFake(() =>
      of(true)
    );
    component.reweight();

    expect(osdServiceSpy.calls.count()).toBe(1);
    expect(osdServiceSpy.calls.first().args).toEqual([1, 0.5]);
  });
});
