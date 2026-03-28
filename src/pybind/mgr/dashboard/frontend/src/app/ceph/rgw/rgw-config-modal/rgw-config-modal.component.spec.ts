import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastrModule } from 'ngx-toastr';
import { InputModule, ModalModule, RadioModule, SelectModule } from 'carbon-components-angular';

import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { RgwConfigModalComponent } from './rgw-config-modal.component';

describe('RgwConfigModalComponent', () => {
  let component: RgwConfigModalComponent;
  let fixture: ComponentFixture<RgwConfigModalComponent>;

  configureTestBed({
    declarations: [RgwConfigModalComponent],
    imports: [
      SharedModule,
      ReactiveFormsModule,
      RouterTestingModule,
      HttpClientTestingModule,
      ToastrModule.forRoot(),
      InputModule,
      ModalModule,
      RadioModule,
      SelectModule
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwConfigModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should render a fully carbonized modal shell and form controls', () => {
    const nativeElement = fixture.nativeElement as HTMLElement;

    expect(nativeElement.querySelector('cds-modal')).toBeTruthy();
    expect(nativeElement.querySelector('cds-radio-group')).toBeTruthy();
    expect(nativeElement.querySelectorAll('cds-select').length).toBeGreaterThanOrEqual(3);
    expect(nativeElement.querySelector('cds-text-label[for="token"]')).toBeTruthy();
    expect(nativeElement.querySelector('cds-text-label[for="ssl_cert"]')).toBeTruthy();
    expect(nativeElement.querySelector('cds-text-label[for="client_key"]')).toBeTruthy();
    expect(nativeElement.querySelectorAll('.form-group.row').length).toBe(0);
    expect(nativeElement.querySelector('cds-text-label[for="addr"]')).toBeTruthy();
  });

  it('should render carbonized KMIP-specific fields when the provider is kmip', () => {
    component.configForm.patchValue({
      encryptionType: component.ENCRYPTION_TYPE.SSE_KMS,
      kms_provider: component.KMS_PROVIDER.KMIP
    });
    fixture.detectChanges();

    const nativeElement = fixture.nativeElement as HTMLElement;

    expect(nativeElement.querySelector('cds-text-label[for="kms_key_template"]')).toBeTruthy();
    expect(nativeElement.querySelector('cds-text-label[for="s3_key_template"]')).toBeTruthy();
    expect(nativeElement.querySelector('cds-text-label[for="username"]')).toBeTruthy();
    expect(nativeElement.querySelector('cds-password-label[for="password"]')).toBeTruthy();
  });
});
