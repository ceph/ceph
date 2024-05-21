import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { fakeAsync, TestBed } from '@angular/core/testing';

import { configureTestBed } from '~/testing/unit-test-helper';
import { ClusterService } from './cluster.service';

describe('ClusterService', () => {
  let service: ClusterService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    imports: [HttpClientTestingModule],
    providers: [ClusterService]
  });

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(ClusterService);
    httpTesting = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call getStatus', () => {
    service.getStatus().subscribe();
    const req = httpTesting.expectOne('api/cluster');
    expect(req.request.method).toBe('GET');
  });

  it('should update cluster status', fakeAsync(() => {
    service.updateStatus('fakeStatus').subscribe();
    const req = httpTesting.expectOne('api/cluster');
    expect(req.request.method).toBe('PUT');
    expect(req.request.body).toEqual({ status: 'fakeStatus' });
  }));
});
