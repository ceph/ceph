import { Pipe, PipeTransform } from '@angular/core';
import {
  CephCertificateStatus,
  CephServiceCertificate
} from '~/app/shared/models/service.interface';
import { CdDatePipe } from '~/app/shared/pipes/cd-date.pipe';

@Pipe({ name: 'serviceCertificateStatus', standalone: false })
export class ServiceCertificateStatusPipe implements PipeTransform {
  constructor(private cdDatePipe: CdDatePipe) {}

  transform(cert: CephServiceCertificate | undefined): string {
    if (!cert || !cert.requires_certificate || !cert.status) {
      return '-';
    }

    const formattedDate = cert.expiry_date
      ? this.cdDatePipe.transform(cert.expiry_date, 'DD MMM y')
      : null;

    switch (cert.status) {
      case CephCertificateStatus.valid:
        return formattedDate ? $localize`Valid - ${formattedDate}` : $localize`Valid`;
      case CephCertificateStatus.expiring:
      case CephCertificateStatus.expiringSoon:
        return formattedDate
          ? $localize`Expiring soon - ${formattedDate}`
          : $localize`Expiring soon`;
      case CephCertificateStatus.expired:
        return formattedDate ? $localize`Expired - ${formattedDate}` : $localize`Expired`;
      case CephCertificateStatus.notConfigured:
        return '-';
      default:
        return formattedDate ? `${cert.status} - ${formattedDate}` : cert.status;
    }
  }
}
