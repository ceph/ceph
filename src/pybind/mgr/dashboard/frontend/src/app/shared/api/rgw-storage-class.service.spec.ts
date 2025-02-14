import { TestBed } from '@angular/core/testing';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';

import { RgwStorageClassService } from './rgw-storage-class.service';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('RgwStorageClassService', () => {
  let service: RgwStorageClassService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    providers: [RgwStorageClassService],
    imports: [HttpClientTestingModule]
  });
  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(RgwStorageClassService);
    httpTesting = TestBed.inject(HttpTestingController);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call remove', () => {
    service.removeStorageClass('default-placement', 'Cloud8ibm').subscribe();
    const req = httpTesting.expectOne(
      'api/rgw/zonegroup/storage-class/default-placement/Cloud8ibm'
    );
    expect(req.request.method).toBe('DELETE');
  });
});
