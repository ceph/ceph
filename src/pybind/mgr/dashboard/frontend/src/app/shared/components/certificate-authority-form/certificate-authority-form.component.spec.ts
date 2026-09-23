import { CUSTOM_ELEMENTS_SCHEMA, NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { expect as jestExpect } from '@jest/globals';

import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CertificateType, CertMode } from '~/app/shared/models/service.interface';
import { ComponentsModule } from '../components.module';
import { CertificateAuthorityFormComponent } from './certificate-authority-form.component';

describe('CertificateAuthorityFormComponent', () => {
  let component: CertificateAuthorityFormComponent;
  let fixture: ComponentFixture<CertificateAuthorityFormComponent>;
  let formBuilder: CdFormBuilder;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ComponentsModule, CertificateAuthorityFormComponent],
      schemas: [NO_ERRORS_SCHEMA, CUSTOM_ELEMENTS_SCHEMA]
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
    jestExpect(component).toBeTruthy();
  });

  it('should emit certificateTypeChange', () => {
    const emitSpy = jest.spyOn(component.certificateTypeChange, 'emit');
    component.onCertificateTypeChange(CertificateType.external);
    jestExpect(emitSpy).toHaveBeenCalledWith(CertificateType.external);
  });

  describe('certMode input', () => {
    it('should default to CertMode.both', () => {
      jestExpect(component.certMode).toBe(CertMode.both);
    });

    it('should accept custom certMode values', () => {
      component.certMode = CertMode.externalOnly;
      jestExpect(component.certMode).toBe(CertMode.externalOnly);

      component.certMode = CertMode.internalOnly;
      jestExpect(component.certMode).toBe(CertMode.internalOnly);
    });
  });

  describe('certMode HTML rendering', () => {
    it('should render the radio group when certMode is both', () => {
      component.certMode = CertMode.both;
      fixture.detectChanges();
      const radioGroup = fixture.nativeElement.querySelector('cds-radio-group');
      jestExpect(radioGroup).not.toBeNull();
    });

    it('should not render the radio group when certMode is internalOnly', () => {
      component.certMode = CertMode.internalOnly;
      fixture.detectChanges();
      const radioGroup = fixture.nativeElement.querySelector('cds-radio-group');
      jestExpect(radioGroup).toBeNull();
    });

    it('should not render the radio group when certMode is externalOnly', () => {
      component.certMode = CertMode.externalOnly;
      fixture.detectChanges();
      const radioGroup = fixture.nativeElement.querySelector('cds-radio-group');
      jestExpect(radioGroup).toBeNull();
    });

    it('should render the internal-cert panel when certMode is internalOnly', () => {
      component.certMode = CertMode.internalOnly;
      fixture.detectChanges();
      const alertPanel = fixture.nativeElement.querySelector('cd-alert-panel');
      jestExpect(alertPanel).not.toBeNull();
    });

    it('should not render the internal-cert panel when certMode is externalOnly', () => {
      component.certMode = CertMode.externalOnly;
      fixture.detectChanges();
      const radioGroup = fixture.nativeElement.querySelector('cds-radio-group');
      const alertPanel = fixture.nativeElement.querySelector('cd-alert-panel');
      jestExpect(radioGroup).toBeNull();
      jestExpect(alertPanel).toBeNull();
    });

    it('should render the internal-cert panel when certMode is both and internal is selected', () => {
      component.certMode = CertMode.both;
      component.formGroup.controls['certificateType'].setValue(CertificateType.internal);
      fixture.detectChanges();
      const alertPanel = fixture.nativeElement.querySelector('cd-alert-panel');
      jestExpect(alertPanel).not.toBeNull();
    });

    it('should not render the internal-cert panel when certMode is both and external is selected', () => {
      component.certMode = CertMode.both;
      component.formGroup.controls['certificateType'].setValue(CertificateType.external);
      fixture.detectChanges();
      const alertPanels = fixture.nativeElement.querySelectorAll('cd-alert-panel');
      const hasInternalMsg = Array.from(alertPanels).some((el: any) =>
        el.textContent?.includes('generated automatically by Cephadm CA')
      );
      jestExpect(hasInternalMsg).toBe(false);
    });
  });
});
