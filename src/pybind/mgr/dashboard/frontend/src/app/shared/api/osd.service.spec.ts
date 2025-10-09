import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '~/testing/unit-test-helper';
import { OsdService } from './osd.service';
import { CdTableFetchDataContext } from '../models/cd-table-fetch-data-context';

describe('OsdService', () => {
  let service: OsdService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    providers: [OsdService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.inject(OsdService);
    httpTesting = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call create', () => {
    const trackingId = 'all_hdd, host1_ssd';
    const post_data = {
      method: 'drive_groups',
      data: [
        {
          service_name: 'osd',
          service_id: 'all_hdd',
          host_pattern: '*',
          data_devices: {
            rotational: true
          }
        },
        {
          service_name: 'osd',
          service_id: 'host1_ssd',
          host_pattern: 'host1',
          data_devices: {
            rotational: false
          }
        }
      ],
      tracking_id: trackingId
    };
    service.create(post_data.data, trackingId).subscribe();
    const req = httpTesting.expectOne('api/osd');
    expect(req.request.method).toBe('POST');
    expect(req.request.body).toEqual(post_data);
  });

  it('should call delete', () => {
    const id = 1;
    service.delete(id, true, true).subscribe();
    const req = httpTesting.expectOne(`api/osd/${id}?preserve_id=true&force=true`);
    expect(req.request.method).toBe('DELETE');
  });

  it('should call getList', () => {
    const context = new CdTableFetchDataContext(() => {});
    service.getList(context.toParams()).observable.subscribe();
    const req = httpTesting.expectOne('api/osd?offset=0&limit=10&search=&sort=%2Bname');
    expect(req.request.method).toBe('GET');
  });

  it('should call getDetails', () => {
    service.getDetails(1).subscribe();
    const req = httpTesting.expectOne('api/osd/1');
    expect(req.request.method).toBe('GET');
  });

  it('should call scrub, with deep=true', () => {
    service.scrub('foo', true).subscribe();
    const req = httpTesting.expectOne('api/osd/foo/scrub?deep=true');
    expect(req.request.method).toBe('POST');
  });

  it('should call scrub, with deep=false', () => {
    service.scrub('foo', false).subscribe();
    const req = httpTesting.expectOne('api/osd/foo/scrub?deep=false');
    expect(req.request.method).toBe('POST');
  });

  it('should call getFlags', () => {
    service.getFlags().subscribe();
    const req = httpTesting.expectOne('api/osd/flags');
    expect(req.request.method).toBe('GET');
  });

  it('should call updateFlags', () => {
    service.updateFlags(['foo']).subscribe();
    const req = httpTesting.expectOne('api/osd/flags');
    expect(req.request.method).toBe('PUT');
    expect(req.request.body).toEqual({ flags: ['foo'] });
  });

  it('should call updateIndividualFlags to update individual flags', () => {
    const flags = { noin: true, noout: true };
    const ids = [0, 1];
    service.updateIndividualFlags(flags, ids).subscribe();
    const req = httpTesting.expectOne('api/osd/flags/individual');
    expect(req.request.method).toBe('PUT');
    expect(req.request.body).toEqual({ flags: flags, ids: ids });
  });

  it('should mark the OSD out', () => {
    service.markOut(1).subscribe();
    const req = httpTesting.expectOne('api/osd/1/mark');
    expect(req.request.method).toBe('PUT');
    expect(req.request.body).toEqual({ action: 'out' });
  });

  it('should mark the OSD in', () => {
    service.markIn(1).subscribe();
    const req = httpTesting.expectOne('api/osd/1/mark');
    expect(req.request.method).toBe('PUT');
    expect(req.request.body).toEqual({ action: 'in' });
  });

  it('should mark the OSD down', () => {
    service.markDown(1).subscribe();
    const req = httpTesting.expectOne('api/osd/1/mark');
    expect(req.request.method).toBe('PUT');
    expect(req.request.body).toEqual({ action: 'down' });
  });

  it('should reweight an OSD', () => {
    service.reweight(1, 0.5).subscribe();
    const req = httpTesting.expectOne('api/osd/1/reweight');
    expect(req.request.method).toBe('POST');
    expect(req.request.body).toEqual({ weight: 0.5 });
  });

  it('should update OSD', () => {
    service.update(1, 'hdd').subscribe();
    const req = httpTesting.expectOne('api/osd/1');
    expect(req.request.method).toBe('PUT');
    expect(req.request.body).toEqual({ device_class: 'hdd' });
  });

  it('should mark an OSD lost', () => {
    service.markLost(1).subscribe();
    const req = httpTesting.expectOne('api/osd/1/mark');
    expect(req.request.method).toBe('PUT');
    expect(req.request.body).toEqual({ action: 'lost' });
  });

  it('should purge an OSD', () => {
    service.purge(1).subscribe();
    const req = httpTesting.expectOne('api/osd/1/purge');
    expect(req.request.method).toBe('POST');
  });

  it('should destroy an OSD', () => {
    service.destroy(1).subscribe();
    const req = httpTesting.expectOne('api/osd/1/destroy');
    expect(req.request.method).toBe('POST');
  });

  it('should return if it is safe to destroy an OSD', () => {
    service.safeToDestroy('[0,1]').subscribe();
    const req = httpTesting.expectOne('api/osd/safe_to_destroy?ids=[0,1]');
    expect(req.request.method).toBe('GET');
  });

  it('should call the devices endpoint to retrieve smart data', () => {
    service.getDevices(1).subscribe();
    const req = httpTesting.expectOne('api/osd/1/devices');
    expect(req.request.method).toBe('GET');
  });

  it('should call getDeploymentOptions', () => {
    service.getDeploymentOptions().subscribe();
    const req = httpTesting.expectOne('ui-api/osd/deployment_options');
    expect(req.request.method).toBe('GET');
  });
});
