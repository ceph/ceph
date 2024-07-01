import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';
import { configureTestBed } from '~/testing/unit-test-helper';
import { RgwMultisiteService } from './rgw-multisite.service';

const mockSyncPolicyData: any = [
  {
    id: 'test',
    data_flow: {},
    pipes: [],
    status: 'enabled',
    bucketName: 'test'
  },
  {
    id: 'test',
    data_flow: {},
    pipes: [],
    status: 'enabled'
  }
];

describe('RgwMultisiteService', () => {
  let service: RgwMultisiteService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    providers: [RgwMultisiteService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.inject(RgwMultisiteService);
    httpTesting = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should fetch all the sync policy related or un-related to a bucket', () => {
    service.getSyncPolicy('', '', true).subscribe();
    const req = httpTesting.expectOne('api/rgw/multisite/sync-policy?all_policy=true');
    expect(req.request.method).toBe('GET');
    req.flush(mockSyncPolicyData);
  });
});
