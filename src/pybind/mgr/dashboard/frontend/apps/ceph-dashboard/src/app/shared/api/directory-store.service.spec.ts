import { TestBed } from '@angular/core/testing';

import { DirectoryStoreService } from './directory-store.service';
import { configureTestBed } from '~/testing/unit-test-helper';
import { CephfsService } from './cephfs.service';
import { HttpClientTestingModule } from '@angular/common/http/testing';

describe('DirectoryStoreService', () => {
  let service: DirectoryStoreService;

  configureTestBed({
    imports: [HttpClientTestingModule],
    providers: [CephfsService]
  });

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(DirectoryStoreService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
