import { TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { configureTestBed } from '~/testing/unit-test-helper';
import { CephfsSnapshotScheduleService } from './cephfs-snapshot-schedule.service';

describe('CephfsSnapshotScheduleService', () => {
  let service: CephfsSnapshotScheduleService;

  configureTestBed({
    providers: [CephfsSnapshotScheduleService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.inject(CephfsSnapshotScheduleService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  describe('scheduleMatchesPath', () => {
    it('should match by path', () => {
      expect(
        service.scheduleMatchesPath(
          { path: '/volumes/g1/sv1', schedule: '1h', start: new Date(), created: new Date(), active: true, status: 'Active' },
          '/volumes/g1/sv1'
        )
      ).toBe(true);
    });

    it('should match by rel_path', () => {
      expect(
        service.scheduleMatchesPath(
          {
            path: '/internal/subvol/path',
            rel_path: '/volumes/g1/sv1',
            schedule: '1h',
            start: new Date(),
            created: new Date(),
            active: true,
            status: 'Active'
          },
          '/volumes/g1/sv1'
        )
      ).toBe(true);
    });

    it('should match subvolume schedules by subvol and group', () => {
      expect(
        service.scheduleMatchesPath(
          {
            path: '/internal/subvol/path',
            rel_path: '',
            subvol: 'sv1',
            group: 'g1',
            schedule: '1h',
            start: new Date(),
            created: new Date(),
            active: true,
            status: 'Active'
          },
          '/volumes/g1/sv1'
        )
      ).toBe(true);
    });
  });
});
