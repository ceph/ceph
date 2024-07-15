import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '~/testing/unit-test-helper';
import { CephfsSubvolumeGroupService } from './cephfs-subvolume-group.service';
import { HttpClientTestingModule } from '@angular/common/http/testing';

describe('CephfsSubvolumeGroupService', () => {
  let service: CephfsSubvolumeGroupService;

  configureTestBed({
    imports: [HttpClientTestingModule],
    providers: [CephfsSubvolumeGroupService]
  });

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(CephfsSubvolumeGroupService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
