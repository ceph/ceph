import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { fakeAsync, TestBed, tick } from '@angular/core/testing';

import { configureTestBed } from '~/testing/unit-test-helper';
import { CdTableFetchDataContext } from '../models/cd-table-fetch-data-context';
import { HostService } from './host.service';

describe('HostService', () => {
  let service: HostService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    providers: [HostService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.inject(HostService);
    httpTesting = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call list', fakeAsync(() => {
    let result: any[] = [{}, {}];
    const hostContext = new CdTableFetchDataContext(() => undefined);
    service.list(hostContext.toParams(), 'true').subscribe((resp) => (result = resp));
    const req = httpTesting.expectOne('api/host?offset=0&limit=10&search=&sort=%2Bname&facts=true');
    expect(req.request.method).toBe('GET');
    req.flush([{ foo: 1 }, { bar: 2 }]);
    tick();
    expect(result[0].foo).toEqual(1);
    expect(result[1].bar).toEqual(2);
  }));

  it('should make a GET request on the devices endpoint when requesting devices', () => {
    const hostname = 'hostname';
    service.getDevices(hostname).subscribe();
    const req = httpTesting.expectOne(`api/host/${hostname}/devices`);
    expect(req.request.method).toBe('GET');
  });

  it('should update host', fakeAsync(() => {
    service.update('mon0', true, ['foo', 'bar'], true, false).subscribe();
    const req = httpTesting.expectOne('api/host/mon0');
    expect(req.request.method).toBe('PUT');
    expect(req.request.body).toEqual({
      force: false,
      labels: ['foo', 'bar'],
      maintenance: true,
      update_labels: true,
      drain: false
    });
  }));

  it('should test host drain call', fakeAsync(() => {
    service.update('host0', false, null, false, false, true).subscribe();
    const req = httpTesting.expectOne('api/host/host0');
    expect(req.request.method).toBe('PUT');
    expect(req.request.body).toEqual({
      force: false,
      labels: null,
      maintenance: false,
      update_labels: false,
      drain: true
    });
  }));

  it('should call getInventory', () => {
    service.getInventory('host-0').subscribe();
    let req = httpTesting.expectOne('api/host/host-0/inventory');
    expect(req.request.method).toBe('GET');

    service.getInventory('host-0', true).subscribe();
    req = httpTesting.expectOne('api/host/host-0/inventory?refresh=true');
    expect(req.request.method).toBe('GET');
  });

  it('should call inventoryList', () => {
    service.inventoryList().subscribe();
    let req = httpTesting.expectOne('ui-api/host/inventory');
    expect(req.request.method).toBe('GET');

    service.inventoryList(true).subscribe();
    req = httpTesting.expectOne('ui-api/host/inventory?refresh=true');
    expect(req.request.method).toBe('GET');
  });
});
