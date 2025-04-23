import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';
import { configureTestBed } from '~/testing/unit-test-helper';
import { RgwMultisiteService } from './rgw-multisite.service';
import { BlockUIModule } from 'ng-block-ui';
import { ToastrModule } from 'ngx-toastr';
import { SharedModule } from '../shared.module';

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
    imports: [
      HttpClientTestingModule,
      BlockUIModule.forRoot(),
      ToastrModule.forRoot(),
      SharedModule
    ]
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

  it('should create Sync Policy Group w/o bucket_name', () => {
    const postData = { group_id: 'test', status: 'enabled' };
    service.createSyncPolicyGroup(postData).subscribe();
    const req = httpTesting.expectOne('api/rgw/multisite/sync-policy-group');
    expect(req.request.method).toBe('POST');
    expect(req.request.body).toEqual(postData);
    req.flush(null);
  });

  it('should create Sync Policy Group with bucket_name', () => {
    const postData = { group_id: 'test', status: 'enabled', bucket_name: 'test' };
    service.createSyncPolicyGroup(postData).subscribe();
    const req = httpTesting.expectOne('api/rgw/multisite/sync-policy-group');
    expect(req.request.method).toBe('POST');
    expect(req.request.body).toEqual(postData);
    req.flush(null);
  });

  it('should modify Sync Policy Group', () => {
    const postData = { group_id: 'test', status: 'enabled', bucket_name: 'test' };
    service.modifySyncPolicyGroup(postData).subscribe();
    const req = httpTesting.expectOne('api/rgw/multisite/sync-policy-group');
    expect(req.request.method).toBe('PUT');
    expect(req.request.body).toEqual(postData);
    req.flush(null);
  });

  it('should remove Sync Policy Group', () => {
    const group_id = 'test';
    service.removeSyncPolicyGroup(group_id).subscribe();
    const req = httpTesting.expectOne('api/rgw/multisite/sync-policy-group/' + group_id);
    expect(req.request.method).toBe('DELETE');
    req.flush(null);
  });

  it('should fetch the sync policy group with given group_id and bucket_name', () => {
    service.getSyncPolicyGroup('test', 'test').subscribe();
    const req = httpTesting.expectOne('api/rgw/multisite/sync-policy-group/test?bucket_name=test');
    expect(req.request.method).toBe('GET');
    req.flush(mockSyncPolicyData[1]);
  });

  it('should create Symmetrical Sync flow', () => {
    const payload = {
      group_id: 'test',
      bucket_name: 'test',
      flow_type: 'symmetrical',
      flow_id: 'new-flow',
      zones: ['zone1-zg1-realm1']
    };
    service.createEditSyncFlow(payload).subscribe();
    const req = httpTesting.expectOne('api/rgw/multisite/sync-flow');
    expect(req.request.method).toBe('PUT');
    expect(req.request.body).toEqual(payload);
    req.flush(null);
  });

  it('should create Directional Sync flow', () => {
    const payload = {
      group_id: 'test',
      bucket_name: 'test',
      flow_type: 'directional',
      flow_id: 'new-flow',
      source_zone: ['zone1-zg1-realm1'],
      destination_zone: ['zone1-zg2-realm2']
    };
    service.createEditSyncFlow(payload).subscribe();
    const req = httpTesting.expectOne('api/rgw/multisite/sync-flow');
    expect(req.request.method).toBe('PUT');
    expect(req.request.body).toEqual(payload);
    req.flush(null);
  });

  it('should edit Symmetrical Sync flow', () => {
    const payload = {
      group_id: 'test',
      bucket_name: 'test',
      flow_type: 'symmetrical',
      flow_id: 'new-flow',
      zones: ['zone1-zg1-realm1', 'zone2-zg1-realm1']
    };
    service.createEditSyncFlow(payload).subscribe();
    const req = httpTesting.expectOne('api/rgw/multisite/sync-flow');
    expect(req.request.method).toBe('PUT');
    expect(req.request.body).toEqual(payload);
    req.flush(null);
  });

  it('should edit Directional Sync flow', () => {
    const payload = {
      group_id: 'test',
      bucket_name: 'test',
      flow_type: 'directional',
      flow_id: 'new-flow',
      source_zone: ['zone1-zg1-realm1'],
      destination_zone: ['zone1-zg2-realm2', 'zone2-zg2-realm2']
    };
    service.createEditSyncFlow(payload).subscribe();
    const req = httpTesting.expectOne('api/rgw/multisite/sync-flow');
    expect(req.request.method).toBe('PUT');
    expect(req.request.body).toEqual(payload);
    req.flush(null);
  });

  it('should remove Symmetrical Sync flow', () => {
    service.removeSyncFlow('test', 'symmetrical', 'test', 'new-bucket').subscribe();
    const req = httpTesting.expectOne(
      `api/rgw/multisite/sync-flow/test/symmetrical/test?bucket_name=new-bucket`
    );
    expect(req.request.method).toBe('DELETE');
    req.flush(null);
  });

  it('should remove Directional Sync flow', () => {
    service.removeSyncFlow('test', 'directional', 'test', 'new-bucket').subscribe();
    const req = httpTesting.expectOne(
      `api/rgw/multisite/sync-flow/test/directional/test?bucket_name=new-bucket`
    );
    expect(req.request.method).toBe('DELETE');
    req.flush(null);
  });

  it('should create Sync Pipe', () => {
    const payload = {
      pipe_id: 'test',
      bucket_name: 'test',
      source_zones: ['zone1-zg1-realm1'],
      destination_zones: ['zone1-zg2-realm2'],
      group_id: 'sync-grp'
    };
    service.createEditSyncPipe(payload).subscribe();
    const req = httpTesting.expectOne('api/rgw/multisite/sync-pipe');
    expect(req.request.method).toBe('PUT');
    expect(req.request.body).toEqual(payload);
    req.flush(null);
  });

  it('should edit Symmetrical Sync flow', () => {
    const payload = {
      pipe_id: 'test',
      bucket_name: 'test',
      source_zones: ['zone1-zg1-realm1'],
      destination_zones: ['zone1-zg2-realm2', 'zone2-zg1-realm1'],
      group_id: 'sync-grp'
    };
    service.createEditSyncFlow(payload).subscribe();
    const req = httpTesting.expectOne('api/rgw/multisite/sync-flow');
    expect(req.request.method).toBe('PUT');
    expect(req.request.body).toEqual(payload);
    req.flush(null);
  });

  it('should remove Sync Pipe', () => {
    service.removeSyncPipe('test', 'sync-grp', 'new-bucket').subscribe();
    const req = httpTesting.expectOne(
      `api/rgw/multisite/sync-pipe/sync-grp/test?bucket_name=new-bucket`
    );
    expect(req.request.method).toBe('DELETE');
    req.flush(null);
  });
});
