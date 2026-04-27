import {
  CephCertificateStatus,
  CephServiceCertificate,
  CERTIFICATE_STATUS_ICON_MAP
} from '~/app/shared/models/service.interface';
import { Component, EventEmitter, Input, Output } from '@angular/core';
import { CdDatePipe } from '~/app/shared/pipes/cd-date.pipe';

@Component({
  selector: 'cd-service-certificate-details',
  templateUrl: './service-certificate-details.component.html',
  styleUrls: ['./service-certificate-details.component.scss'],
  providers: [CdDatePipe],
  standalone: false
})
export class ServiceCertificateDetailsComponent {
  @Input() certificate: CephServiceCertificate;
  @Input() serviceName?: string;
  @Input() serviceType?: string;

  @Output() editService = new EventEmitter<{ serviceName?: string; serviceType?: string }>();

  readonly SERVICES_SUPPORTING_CERT_EDIT = [
    'rgw',
    'ingress',
    'iscsi',
    'oauth2-proxy',
    'mgmt-gateway',
    'nvmeof',
    'nfs'
  ];
  statusIconMap = CERTIFICATE_STATUS_ICON_MAP;

  constructor(private cdDatePipe: CdDatePipe) {}

  formatCertificateStatus(cert: CephServiceCertificate): string {
    if (!cert || !cert.requires_certificate || !cert.status) {
      return '-';
    }
    const formattedDate = this.formatDate(cert.expiry_date);
    switch (cert.status) {
      case CephCertificateStatus.valid:
        return formattedDate ? `Valid - ${formattedDate}` : 'Valid';
      case CephCertificateStatus.expiring:
      case CephCertificateStatus.expiringSoon:
        return formattedDate ? `Expiring soon - ${formattedDate}` : 'Expiring soon';
      case CephCertificateStatus.expired:
        return formattedDate ? `Expired - ${formattedDate}` : 'Expired';
      case CephCertificateStatus.notConfigured:
        return 'Not configured';
      default:
        return formattedDate ? `${cert.status} - ${formattedDate}` : cert.status;
    }
  }

  formatDate(dateValue: string | Date | null | undefined): string | null {
    if (!dateValue) {
      return null;
    }
    return this.cdDatePipe.transform(dateValue, 'DD MMM y');
  }

  onEdit(): void {
    this.editService.emit({ serviceName: this.serviceName, serviceType: this.serviceType });
  }
}
