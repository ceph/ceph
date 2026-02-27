import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { expect as jestExpect } from '@jest/globals';

import { IconComponent } from '~/app/shared/components/icon/icon.component';
import {
  CephCertificateStatus,
  CephServiceCertificate
} from '~/app/shared/models/service.interface';
import { ServiceCertificateDetailsComponent } from './service-certificate-details.component';

describe('ServiceCertificateDetailsComponent', () => {
  let component: ServiceCertificateDetailsComponent;
  let fixture: ComponentFixture<ServiceCertificateDetailsComponent>;
  const baseCert: CephServiceCertificate = {
    cert_name: 'cert',
    scope: 'SERVICE',
    requires_certificate: true,
    status: CephCertificateStatus.valid,
    days_to_expiration: 0,
    signed_by: 'user',
    has_certificate: true,
    certificate_source: 'user',
    expiry_date: '2025-01-01',
    issuer: 'issuer',
    common_name: 'cn'
  };
  const makeCert = (override: Partial<CephServiceCertificate>): CephServiceCertificate => ({
    ...baseCert,
    ...override
  });

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ServiceCertificateDetailsComponent, IconComponent],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();

    fixture = TestBed.createComponent(ServiceCertificateDetailsComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    fixture.detectChanges();
    jestExpect(component).toBeTruthy();
  });

  it('should format certificate status with expiry date', () => {
    const cert = makeCert({ status: CephCertificateStatus.valid, expiry_date: '2025-01-01' });

    jestExpect(component.formatCertificateStatus(cert)).toBe('Valid - 01 Jan 2025');
  });

  it('should return dash when certificate not required', () => {
    const cert = makeCert({ requires_certificate: false, has_certificate: false });
    jestExpect(component.formatCertificateStatus(cert)).toBe('-');
  });

  it('should emit editService with service identifiers', () => {
    component.serviceName = 'svc-name';
    component.serviceType = 'svc-type';
    fixture.detectChanges();

    const emitSpy = jest.spyOn(component.editService, 'emit');
    const button = fixture.debugElement.query(By.css('cds-icon-button'));

    button.triggerEventHandler('click', {});

    jestExpect(emitSpy).toHaveBeenCalledWith({ serviceName: 'svc-name', serviceType: 'svc-type' });
  });

  it('should show success icon and text for valid status', () => {
    component.certificate = makeCert({
      status: CephCertificateStatus.valid,
      expiry_date: '2025-01-01'
    });
    fixture.detectChanges();

    const statusIcon = fixture.debugElement.query(By.css('.status-row cd-icon'));
    const statusText = fixture.debugElement
      .query(By.css('.status-row span'))
      .nativeElement.textContent.trim();

    jestExpect(statusIcon.componentInstance.type).toBe('success');
    jestExpect(statusText).toBe('Valid - 01 Jan 2025');
  });

  it('should fall back to warning icon for invalid status', () => {
    component.certificate = makeCert({ status: 'invalid_status', expiry_date: '2025-01-01' });
    fixture.detectChanges();

    const statusIcon = fixture.debugElement.query(By.css('.status-row cd-icon'));
    jestExpect(statusIcon.componentInstance.type).toBe('warning');
  });

  it('should use warning icon for warning status', () => {
    component.certificate = makeCert({
      status: CephCertificateStatus.expiringSoon,
      expiry_date: '2025-01-01'
    });
    fixture.detectChanges();

    const statusIcon = fixture.debugElement.query(By.css('.status-row cd-icon'));
    const statusText = fixture.debugElement
      .query(By.css('.status-row span'))
      .nativeElement.textContent.trim();

    jestExpect(statusIcon.componentInstance.type).toBe('warning');
    jestExpect(statusText).toBe('Expiring soon - 01 Jan 2025');
  });

  it('should display dash when service has no certificate', () => {
    component.certificate = makeCert({ requires_certificate: false, has_certificate: false });
    fixture.detectChanges();

    const statusText = fixture.debugElement
      .query(By.css('.status-row span'))
      .nativeElement.textContent.trim();
    jestExpect(statusText).toBe('-');
  });
});
