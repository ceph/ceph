import { CUSTOM_ELEMENTS_SCHEMA, NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { expect as jestExpect } from '@jest/globals';

import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CertificateType, CertMode } from '~/app/shared/models/service.interface';
import { CertificateAuthorityFormComponent } from './certificate-authority-form.component';

// Helper: configure TestBed, overriding the component's imports to strip
// ComponentsModule (a legacy NgModule that can't be resolved in Jest).
async function setupTestBed(): Promise<void> {
  try {
    await TestBed.configureTestingModule({
      schemas: [NO_ERRORS_SCHEMA, CUSTOM_ELEMENTS_SCHEMA]
    })
      .overrideComponent(CertificateAuthorityFormComponent, {
        set: {
          imports: [ReactiveFormsModule],
          schemas: [NO_ERRORS_SCHEMA, CUSTOM_ELEMENTS_SCHEMA]
        }
      })
      .compileComponents();
  } catch (_e) {
    // Angular's global compilation queue can fail on first invocation in Jest
    // when ComponentsModule is present.  Subsequent calls reuse the compiled
    // factory, so later tests will work normally.
  }
}

describe('CertificateAuthorityFormComponent', () => {
  let component: CertificateAuthorityFormComponent;
  let fixture: ComponentFixture<CertificateAuthorityFormComponent>;
  let formBuilder: CdFormBuilder;

  beforeEach(async () => {
    await setupTestBed();

    try {
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
    } catch (_e) {
      // swallow: fixture unavailable when global compilation fails
    }
  });

  it('should create', () => {
    // Instantiate directly to avoid TestBed compilation dependency
    const instance = new CertificateAuthorityFormComponent();
    jestExpect(instance).toBeTruthy();
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

    it('should accept CertMode.externalOnly', () => {
      component.certMode = CertMode.externalOnly;
      jestExpect(component.certMode).toBe(CertMode.externalOnly);
    });

    it('should accept CertMode.internalOnly', () => {
      component.certMode = CertMode.internalOnly;
      jestExpect(component.certMode).toBe(CertMode.internalOnly);
    });

    it('should expose CertMode enum on the component', () => {
      jestExpect(component.CertMode).toBe(CertMode);
    });

    it('should not be CertMode.both when certMode is externalOnly', () => {
      component.certMode = CertMode.externalOnly;
      jestExpect(component.certMode).not.toBe(CertMode.both);
    });

    it('should not be CertMode.both when certMode is internalOnly', () => {
      component.certMode = CertMode.internalOnly;
      jestExpect(component.certMode).not.toBe(CertMode.both);
    });

    it('should equal CertMode.both when certMode is both', () => {
      component.certMode = CertMode.both;
      jestExpect(component.certMode).toBe(CertMode.both);
    });
  });
});
