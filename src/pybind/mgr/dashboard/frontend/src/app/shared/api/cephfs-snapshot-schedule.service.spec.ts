import { TestBed } from '@angular/core/testing';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { configureTestBed } from '~/testing/unit-test-helper';
import { CephfsSnapshotScheduleService } from './cephfs-snapshot-schedule.service';

describe('CephfsSnapshotScheduleService', () => {
  let service: CephfsSnapshotScheduleService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    providers: [CephfsSnapshotScheduleService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.inject(CephfsSnapshotScheduleService);
    httpTesting = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  describe('buildRetentionPolicyParam', () => {
    it('should omit empty and placeholder retention', () => {
      expect(service.buildRetentionPolicyParam(undefined)).toBeUndefined();
      expect(service.buildRetentionPolicyParam(null)).toBeUndefined();
      expect(service.buildRetentionPolicyParam('-')).toBeUndefined();
      expect(service.buildRetentionPolicyParam({})).toBeUndefined();
    });

    it('should convert object retention to the API query format', () => {
      expect(service.buildRetentionPolicyParam({ d: 7, w: 4 })).toBe('7-d|4-w');
    });

    it('should convert table string retention without treating "-" as a spec', () => {
      expect(service.buildRetentionPolicyParam('7d 4w')).toBe('7-d|4-w');
      expect(service.buildRetentionPolicyParam('--')).toBeUndefined();
    });
  });

  it('should omit placeholder retention_policy from delete requests', () => {
    service
      .delete({
        fs: 'test_fs',
        path: '/e2e_data',
        schedule: '1d',
        start: '2024-01-01T00:00:00',
        retentionPolicy: service.buildRetentionPolicyParam('-')
      })
      .subscribe();

    const req = httpTesting.expectOne((request) => request.url.includes('/delete_snapshot'));
    expect(req.request.method).toBe('DELETE');
    expect(req.request.url).not.toContain('retention_policy');
    req.flush({});
  });
});
