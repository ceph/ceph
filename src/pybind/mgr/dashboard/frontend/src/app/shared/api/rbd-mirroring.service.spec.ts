import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { fakeAsync, TestBed, tick } from '@angular/core/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { RbdMirroringService } from './rbd-mirroring.service';

describe('RbdMirroringService', () => {
  let service: RbdMirroringService;
  let httpTesting: HttpTestingController;

  const summary = {
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

  configureTestBed(
    {
      providers: [RbdMirroringService],
      imports: [HttpClientTestingModule]
    },
    true
  );

  beforeEach(() => {
    service = TestBed.get(RbdMirroringService);
    httpTesting = TestBed.get(HttpTestingController);

    const req = httpTesting.expectOne('api/block/mirroring/summary');
    expect(req.request.method).toBe('GET');
    req.flush(summary);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should periodically poll summary', fakeAsync(() => {
    const calledWith = [];
    service.subscribeSummary((data) => {
      calledWith.push(data);
    });
    service.refreshAndSchedule();
    tick(30000);
    // In order to not trigger setTimeout again,
    // which would raise 'Error: 1 timer(s) still in the queue.'
    spyOn(service, 'refreshAndSchedule').and.callFake(() => true);
    tick(30000);

    const calls = httpTesting.match((request) => {
      return request.url.match(/api\/block\/mirroring\/summary/) && request.method === 'GET';
    });

    expect(calls.length).toEqual(2);
    calls.forEach((call) => call.flush(summary));

    expect(calledWith).toEqual([summary, summary, summary]);
  }));

  it('should get current summary', () => {
    expect(service.getCurrentSummary()).toEqual(summary);
  });

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
