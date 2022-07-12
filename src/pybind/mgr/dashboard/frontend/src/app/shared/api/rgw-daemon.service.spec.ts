import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { fakeAsync, TestBed, tick } from '@angular/core/testing';

import { of } from 'rxjs';

import { RgwDaemon } from '~/app/ceph/rgw/models/rgw-daemon';
import { configureTestBed, RgwHelper } from '~/testing/unit-test-helper';
import { RgwDaemonService } from './rgw-daemon.service';

describe('RgwDaemonService', () => {
  let service: RgwDaemonService;
  let httpTesting: HttpTestingController;
  let selectDaemonSpy: jasmine.Spy;

  const daemonList: Array<RgwDaemon> = RgwHelper.getDaemonList();
  const retrieveDaemonList = (reqDaemonList: RgwDaemon[], daemon: RgwDaemon) => {
    service
      .request((params) => of(params))
      .subscribe((params) => expect(params.get('daemon_name')).toBe(daemon.id));
    const listReq = httpTesting.expectOne('api/rgw/daemon');
    listReq.flush(reqDaemonList);
    tick();
    expect(service['selectedDaemon'].getValue()).toEqual(daemon);
  };

  configureTestBed({
    providers: [RgwDaemonService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.inject(RgwDaemonService);
    selectDaemonSpy = spyOn(service, 'selectDaemon').and.callThrough();
    httpTesting = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should get daemon list', () => {
    service.list().subscribe();
    const req = httpTesting.expectOne('api/rgw/daemon');
    req.flush(daemonList);
    expect(req.request.method).toBe('GET');
    expect(service['daemons'].getValue()).toEqual(daemonList);
  });

  it('should call "get daemon"', () => {
    service.get('foo').subscribe();
    const req = httpTesting.expectOne('api/rgw/daemon/foo');
    expect(req.request.method).toBe('GET');
  });

  it('should call request and not select any daemon from empty daemon list', fakeAsync(() => {
    expect(() => retrieveDaemonList([], null)).toThrowError('No RGW daemons found!');
    expect(selectDaemonSpy).toHaveBeenCalledTimes(0);
  }));

  it('should call request and select default daemon from daemon list', fakeAsync(() => {
    retrieveDaemonList(daemonList, daemonList[1]);
    expect(selectDaemonSpy).toHaveBeenCalledTimes(1);
    expect(selectDaemonSpy).toHaveBeenCalledWith(daemonList[1]);
  }));

  it('should call request and select first daemon from daemon list that has no default', fakeAsync(() => {
    const noDefaultDaemonList = daemonList.map((daemon) => {
      daemon.default = false;
      return daemon;
    });
    retrieveDaemonList(noDefaultDaemonList, noDefaultDaemonList[0]);
    expect(selectDaemonSpy).toHaveBeenCalledTimes(1);
    expect(selectDaemonSpy).toHaveBeenCalledWith(noDefaultDaemonList[0]);
  }));

  it('should update default daemon if not exist in daemon list', fakeAsync(() => {
    const tmpDaemonList = [...daemonList];
    service.selectDaemon(tmpDaemonList[1]); // Select 'default' daemon.
    tmpDaemonList.splice(1, 1); // Remove 'default' daemon.
    tmpDaemonList[0].default = true; // Set new 'default' daemon.
    service.list().subscribe();
    const testReq = httpTesting.expectOne('api/rgw/daemon');
    testReq.flush(tmpDaemonList);
    expect(service['selectedDaemon'].getValue()).toEqual(tmpDaemonList[0]);
  }));
});
