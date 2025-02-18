import { TestBed } from '@angular/core/testing';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';

import { RgwStorageClassService } from './rgw-storage-class.service';
import { configureTestBed } from '~/testing/unit-test-helper';

import { RequestModel } from '~/app/ceph/rgw/models/rgw-storage-class.model';

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

  it('should call create', () => {
    const request: RequestModel = {
      zone_group: 'default',
      placement_targets: [
        {
          tags: [],
          placement_id: 'default-placement',
          storage_class: 'test1',
          tier_type: 'cloud-s3',
          tier_config: {
            endpoint: 'http://198.162.100.100:80',
            access_key: 'test56',
            secret: 'test56',
            target_path: 'tsest-dnyanee',
            retain_head_object: false,
            region: 'ams3d',
            multipart_sync_threshold: 33554432,
            multipart_min_part_size: 33554432
          }
        }
      ]
    };
    service.createStorageClass(request).subscribe();
    const req = httpTesting.expectOne('api/rgw/zonegroup/storage-class');
    expect(req.request.method).toBe('POST');
  });
});
