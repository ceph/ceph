import { ServiceCertificateStatusPipe } from './service-certificate-status.pipe';
import {
  CephCertificateStatus,
  CephServiceCertificate
} from '~/app/shared/models/service.interface';

describe('ServiceCertificateStatusPipe', () => {
  let pipe: ServiceCertificateStatusPipe;
  let mockCdDatePipe: any;

  beforeEach(() => {
    // Mock the CdDatePipe to return a predictable string
    mockCdDatePipe = {
      transform: jest
        .fn()
        .mockImplementation((date: string, _format: string) => `formatted_${date}`)
    };

    pipe = new ServiceCertificateStatusPipe(mockCdDatePipe);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('should create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  describe('Invalid or Missing Data', () => {
    it('should return "-" if cert is undefined', () => {
      expect(pipe.transform(undefined)).toBe('-');
    });

    it('should return "-" if cert does not require a certificate', () => {
      const cert = {
        requires_certificate: false,
        status: CephCertificateStatus.valid
      } as CephServiceCertificate;
      expect(pipe.transform(cert)).toBe('-');
    });

    it('should return "-" if cert status is undefined or empty', () => {
      const cert = {
        requires_certificate: true,
        status: undefined
      } as any;
      expect(pipe.transform(cert)).toBe('-');
    });
  });

  describe('Valid Status', () => {
    it('should format string with date when expiry_date is provided', () => {
      const cert = {
        requires_certificate: true,
        status: CephCertificateStatus.valid,
        expiry_date: '2026-10-10'
      } as CephServiceCertificate;

      expect(pipe.transform(cert)).toBe('Valid - formatted_2026-10-10');
      expect(mockCdDatePipe.transform).toHaveBeenCalledWith('2026-10-10', 'DD MMM y');
    });

    it('should format string without date when expiry_date is missing', () => {
      const cert = {
        requires_certificate: true,
        status: CephCertificateStatus.valid
      } as CephServiceCertificate;

      expect(pipe.transform(cert)).toBe('Valid');
      expect(mockCdDatePipe.transform).not.toHaveBeenCalled();
    });
  });

  describe('Expiring Status', () => {
    it('should format expiring correctly with date', () => {
      const cert = {
        requires_certificate: true,
        status: CephCertificateStatus.expiring,
        expiry_date: '2026-08-10'
      } as CephServiceCertificate;

      expect(pipe.transform(cert)).toBe('Expiring soon - formatted_2026-08-10');
    });

    it('should format expiringSoon correctly with date', () => {
      const cert = {
        requires_certificate: true,
        status: CephCertificateStatus.expiringSoon,
        expiry_date: '2026-08-15'
      } as CephServiceCertificate;

      expect(pipe.transform(cert)).toBe('Expiring soon - formatted_2026-08-15');
    });

    it('should format correctly without date', () => {
      const cert = {
        requires_certificate: true,
        status: CephCertificateStatus.expiring
      } as CephServiceCertificate;

      expect(pipe.transform(cert)).toBe('Expiring soon');
    });
  });

  describe('Expired Status', () => {
    it('should format correctly with date', () => {
      const cert = {
        requires_certificate: true,
        status: CephCertificateStatus.expired,
        expiry_date: '2025-01-01'
      } as CephServiceCertificate;

      expect(pipe.transform(cert)).toBe('Expired - formatted_2025-01-01');
    });

    it('should format correctly without date', () => {
      const cert = {
        requires_certificate: true,
        status: CephCertificateStatus.expired
      } as CephServiceCertificate;

      expect(pipe.transform(cert)).toBe('Expired');
    });
  });

  describe('Not Configured Status', () => {
    it('should return "-" when status is notConfigured', () => {
      const cert = {
        requires_certificate: true,
        status: CephCertificateStatus.notConfigured,
        expiry_date: '2026-10-10'
      } as CephServiceCertificate;

      expect(pipe.transform(cert)).toBe('-');
    });
  });

  describe('Default (Unknown) Status', () => {
    it('should fallback to raw status text with date', () => {
      const cert = {
        requires_certificate: true,
        status: 'CustomStatus',
        expiry_date: '2026-12-31'
      } as any;

      expect(pipe.transform(cert)).toBe('CustomStatus - formatted_2026-12-31');
    });

    it('should fallback to raw status text without date', () => {
      const cert = {
        requires_certificate: true,
        status: 'CustomStatus'
      } as any;

      expect(pipe.transform(cert)).toBe('CustomStatus');
    });
  });
});
