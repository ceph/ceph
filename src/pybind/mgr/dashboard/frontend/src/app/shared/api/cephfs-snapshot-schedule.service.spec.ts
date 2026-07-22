import { TestBed } from '@angular/core/testing';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { configureTestBed } from '~/testing/unit-test-helper';
import { CephfsSnapshotScheduleService } from './cephfs-snapshot-schedule.service';
import { RepeatFrequency } from '../enum/repeat-frequency.enum';

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

  it('checkScheduleExists should match subvolume schedules with /.. paths', () => {
    const path = '/volumes/Group1/subvol1/uuid';
    let exists: boolean | undefined;

    service.checkScheduleExists(path, 'fs1', 3, RepeatFrequency.Monthly, true).subscribe((result) => {
      exists = result;
    });

    const req = httpTesting.expectOne(
      (request) =>
        request.method === 'GET' &&
        request.url.includes('api/cephfs/snapshot/schedule/fs1') &&
        request.url.includes('path=/volumes/Group1/subvol1/uuid')
    );
    req.flush([
      {
        path: '/volumes/Group1/subvol1/uuid/..',
        schedule: '3M',
        active: true,
        retention: {}
      }
    ]);

    expect(exists).toBe(true);
  });

  it('checkScheduleExists should not match a different interval on the same path', () => {
    const path = '/volumes/Group1/subvol1/uuid';
    let exists: boolean | undefined;

    service.checkScheduleExists(path, 'fs1', 8, RepeatFrequency.Monthly, true).subscribe((result) => {
      exists = result;
    });

    const req = httpTesting.expectOne(
      (request) =>
        request.method === 'GET' &&
        request.url.includes('api/cephfs/snapshot/schedule/fs1') &&
        request.url.includes('path=/volumes/Group1/subvol1/uuid')
    );
    req.flush([
      {
        path: '/volumes/Group1/subvol1/uuid/..',
        schedule: '3M',
        active: true,
        retention: {}
      }
    ]);

    expect(exists).toBe(false);
  });

  it('checkRetentionPolicyExists should detect existing retention on subvolume paths with /.. suffix', () => {
    const path = '/volumes/Group1/subvol1/uuid';
    let result: { exists: boolean; errorIndex: number } | undefined;

    service
      .checkRetentionPolicyExists(path, 'fs1', ['M'], [], true)
      .subscribe((value) => (result = value));

    const req = httpTesting.expectOne(
      (request) =>
        request.method === 'GET' &&
        request.url.includes('api/cephfs/snapshot/schedule/fs1') &&
        request.url.includes('path=/volumes/Group1/subvol1/uuid')
    );
    req.flush([
      {
        path: '/volumes/Group1/subvol1/uuid/..',
        schedule: '4w',
        active: true,
        retention: { M: 2 }
      }
    ]);

    expect(result).toEqual({ exists: true, errorIndex: 0 });
  });

  it('parseScheduleCopy should format yearly schedules with lowercase y', () => {
    expect(service.parseScheduleCopy('7y')).toBe('Every 7 years');
    expect(service.parseScheduleCopy('1y')).toBe('Every year');
  });

  it('parseRetentionConflictFrequency should extract the conflicting frequency', () => {
    expect(
      service.parseRetentionConflictFrequency(
        'Failed to add retention policy for path /volumes/Group1/A1/uuid: Retention for M is already present with value 2. Please remove it first.'
      )
    ).toBe('M');
  });
});
