import { HttpRequest } from '@angular/common/http';
import {
  HttpClientTestingModule,
  HttpTestingController,
  TestRequest
} from '@angular/common/http/testing';
import { fakeAsync, TestBed, tick } from '@angular/core/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { RbdMirroringService } from './rbd-mirroring.service';

describe('RbdMirroringService', () => {
  let service: RbdMirroringService;
  let httpTesting: HttpTestingController;
  let getMirroringSummaryCalls: () => TestRequest[];
  let flushCalls: (call: TestRequest) => void;

  const summary: Record<string, any> = {
    status: 0,
    content_data: {
      daemons: [],
      pools: [],
      image_error: [],
      image_syncing: [],
      image_ready: []
    },
    executing_tasks: [{}]
  };

  configureTestBed({
    providers: [RbdMirroringService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.get(RbdMirroringService);
    httpTesting = TestBed.get(HttpTestingController);
    getMirroringSummaryCalls = () => {
      return httpTesting.match((request: HttpRequest<any>) => {
        return request.url.match(/api\/block\/mirroring\/summary/) && request.method === 'GET';
      });
    };
    flushCalls = (call: TestRequest) => {
      if (!call.cancelled) {
        call.flush(summary);
      }
    };
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should periodically poll summary', fakeAsync(() => {
    const subs = service.startPolling();
    tick();
    const calledWith: any[] = [];
    service.subscribeSummary((data) => {
      calledWith.push(data);
    });
    tick(service.REFRESH_INTERVAL * 2);
    const calls = getMirroringSummaryCalls();

    expect(calls.length).toEqual(3);
    calls.forEach((call: TestRequest) => flushCalls(call));
    expect(calledWith).toEqual([summary]);

    subs.unsubscribe();
  }));

  it('should get pool config', () => {
    service.getPool('poolName').subscribe();

    const req = httpTesting.expectOne('api/block/mirroring/pool/poolName');
    expect(req.request.method).toBe('GET');
  });

  it('should update pool config', () => {
    const request = {
      mirror_mode: 'pool'
    };
    service.updatePool('poolName', request).subscribe();

    const req = httpTesting.expectOne('api/block/mirroring/pool/poolName');
    expect(req.request.method).toBe('PUT');
    expect(req.request.body).toEqual(request);
  });

  it('should get site name', () => {
    service.getSiteName().subscribe();

    const req = httpTesting.expectOne('api/block/mirroring/site_name');
    expect(req.request.method).toBe('GET');
  });

  it('should set site name', () => {
    service.setSiteName('site-a').subscribe();

    const req = httpTesting.expectOne('api/block/mirroring/site_name');
    expect(req.request.method).toBe('PUT');
    expect(req.request.body).toEqual({ site_name: 'site-a' });
  });

  it('should create bootstrap token', () => {
    service.createBootstrapToken('poolName').subscribe();

    const req = httpTesting.expectOne('api/block/mirroring/pool/poolName/bootstrap/token');
    expect(req.request.method).toBe('POST');
  });

  it('should import bootstrap token', () => {
    service.importBootstrapToken('poolName', 'rx', 'token-1234').subscribe();

    const req = httpTesting.expectOne('api/block/mirroring/pool/poolName/bootstrap/peer');
    expect(req.request.method).toBe('POST');
    expect(req.request.body).toEqual({
      direction: 'rx',
      token: 'token-1234'
    });
  });

  it('should get peer config', () => {
    service.getPeer('poolName', 'peerUUID').subscribe();

    const req = httpTesting.expectOne('api/block/mirroring/pool/poolName/peer/peerUUID');
    expect(req.request.method).toBe('GET');
  });

  it('should add peer config', () => {
    const request = {
      cluster_name: 'remote',
      client_id: 'admin',
      mon_host: 'localhost',
      key: '1234'
    };
    service.addPeer('poolName', request).subscribe();

    const req = httpTesting.expectOne('api/block/mirroring/pool/poolName/peer');
    expect(req.request.method).toBe('POST');
    expect(req.request.body).toEqual(request);
  });

  it('should update peer config', () => {
    const request = {
      cluster_name: 'remote'
    };
    service.updatePeer('poolName', 'peerUUID', request).subscribe();

    const req = httpTesting.expectOne('api/block/mirroring/pool/poolName/peer/peerUUID');
    expect(req.request.method).toBe('PUT');
    expect(req.request.body).toEqual(request);
  });

  it('should delete peer config', () => {
    service.deletePeer('poolName', 'peerUUID').subscribe();

    const req = httpTesting.expectOne('api/block/mirroring/pool/poolName/peer/peerUUID');
    expect(req.request.method).toBe('DELETE');
  });
});
