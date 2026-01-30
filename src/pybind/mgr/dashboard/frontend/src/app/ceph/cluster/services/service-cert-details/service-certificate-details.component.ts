import {
  CephCertificateStatus,
  CephServiceCertificate
} from '~/app/shared/models/service.interface';
import { DatePipe } from '@angular/common';
import { Component, EventEmitter, Input, Output } from '@angular/core';
import { ICON_TYPE, Icons } from '~/app/shared/enum/icons.enum';

@Component({
  selector: 'cd-service-certificate-details',
  templateUrl: './service-certificate-details.component.html',
  styleUrls: ['./service-certificate-details.component.scss'],
  providers: [DatePipe],
  standalone: false
})
export class ServiceCertificateDetailsComponent {
  @Input() certificate: CephServiceCertificate;
  @Input() serviceName?: string;
  @Input() serviceType?: string;

  @Output() editService = new EventEmitter<{ serviceName?: string; serviceType?: string }>();

  icons = Icons;
  readonly statusIconMap: Record<string, keyof typeof ICON_TYPE> = {
    valid: 'success',
    expiring: 'warning',
    expiring_soon: 'warning',
    expired: 'danger',
    default: 'warning'
  };

  constructor(private datePipe: DatePipe) {}

  formatCertificateStatus(cert: CephServiceCertificate): string {
    if (!cert || !cert.requires_certificate || !cert.status) {
      return '-';
    }

    const formattedDate = this.formatDate(cert.expiry_date);
    switch (cert.status) {
      case CephCertificateStatus.valid:
        return formattedDate ? `Valid - ${formattedDate}` : 'Valid';
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
    return this.datePipe.transform(dateValue, 'dd MMM y');
  }

  onEdit(): void {
    this.editService.emit({ serviceName: this.serviceName, serviceType: this.serviceType });
  }
}
