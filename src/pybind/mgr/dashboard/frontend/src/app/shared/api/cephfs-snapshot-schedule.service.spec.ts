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
});
