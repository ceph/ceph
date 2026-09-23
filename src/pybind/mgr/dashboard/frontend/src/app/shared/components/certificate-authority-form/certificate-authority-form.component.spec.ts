import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { expect as jestExpect } from '@jest/globals';

import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CertificateType } from '~/app/shared/models/service.interface';
import { CertificateAuthorityFormComponent } from './certificate-authority-form.component';

describe('CertificateAuthorityFormComponent', () => {
  let component: CertificateAuthorityFormComponent;
  let fixture: ComponentFixture<CertificateAuthorityFormComponent>;
  let formBuilder: CdFormBuilder;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ReactiveFormsModule, CertificateAuthorityFormComponent]
    }).compileComponents();

    formBuilder = new CdFormBuilder();
    fixture = TestBed.createComponent(CertificateAuthorityFormComponent);
    component = fixture.componentInstance;
    component.formGroup = formBuilder.group({
      certificateType: [CertificateType.internal],
      custom_sans: [[]],
      service_type: ['rgw'],
      virtual_host_enabled: [false],
      wildcard_enabled: [false]
    });
  });

  it('should create', () => {
    fixture.detectChanges();
    jestExpect(component).toBeTruthy();
  });

  it('should emit certificateTypeChange', () => {
    const emitSpy = jest.spyOn(component.certificateTypeChange, 'emit');
    component.onCertificateTypeChange(CertificateType.external);
    jestExpect(emitSpy).toHaveBeenCalledWith(CertificateType.external);
  });
});
