import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { fakeAsync, TestBed, tick } from '@angular/core/testing';

import { configureTestBed, i18nProviders } from '../../../testing/unit-test-helper';
import { RbdConfigurationSourceField } from '../models/configuration';
import { RbdConfigurationService } from '../services/rbd-configuration.service';
import { PoolService } from './pool.service';

describe('PoolService', () => {
  let service: PoolService;
  let httpTesting: HttpTestingController;
  const apiPath = 'api/pool';

  configureTestBed({
    providers: [PoolService, RbdConfigurationService, i18nProviders],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.inject(PoolService);
    httpTesting = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call getList', () => {
    service.getList().subscribe();
    const req = httpTesting.expectOne(`${apiPath}?stats=true`);
    expect(req.request.method).toBe('GET');
  });

  it('should call getInfo', () => {
    service.getInfo().subscribe();
    const req = httpTesting.expectOne(`ui-${apiPath}/info`);
    expect(req.request.method).toBe('GET');
  });

  it('should call create', () => {
    const pool = { pool: 'somePool' };
    service.create(pool).subscribe();
    const req = httpTesting.expectOne(apiPath);
    expect(req.request.method).toBe('POST');
    expect(req.request.body).toEqual(pool);
  });

  it('should call update', () => {
    service.update({ pool: 'somePool', application_metadata: [] }).subscribe();
    const req = httpTesting.expectOne(`${apiPath}/somePool`);
    expect(req.request.method).toBe('PUT');
    expect(req.request.body).toEqual({ application_metadata: [] });
  });

  it('should call delete', () => {
    service.delete('somePool').subscribe();
    const req = httpTesting.expectOne(`${apiPath}/somePool`);
    expect(req.request.method).toBe('DELETE');
  });

  it('should call list without parameter', fakeAsync(() => {
    let result;
    service.list().then((resp) => (result = resp));
    const req = httpTesting.expectOne(`${apiPath}?attrs=`);
    expect(req.request.method).toBe('GET');
    req.flush(['foo', 'bar']);
    tick();
    expect(result).toEqual(['foo', 'bar']);
  }));

  it('should call list with a list', fakeAsync(() => {
    let result;
    service.list(['foo']).then((resp) => (result = resp));
    const req = httpTesting.expectOne(`${apiPath}?attrs=foo`);
    expect(req.request.method).toBe('GET');
    req.flush(['foo', 'bar']);
    tick();
    expect(result).toEqual(['foo', 'bar']);
  }));

  it('should test injection of data from getConfiguration()', fakeAsync(() => {
    const pool = 'foo';
    let value;
    service.getConfiguration(pool).subscribe((next) => (value = next));
    const req = httpTesting.expectOne(`${apiPath}/${pool}/configuration`);
    expect(req.request.method).toBe('GET');
    req.flush([
      {
        name: 'rbd_qos_bps_limit',
        value: '60',
        source: RbdConfigurationSourceField.global
      },
      {
        name: 'rbd_qos_iops_limit',
        value: '0',
        source: RbdConfigurationSourceField.global
      }
    ]);
    tick();
    expect(value).toEqual([
      {
        description: 'The desired limit of IO bytes per second.',
        displayName: 'BPS Limit',
        name: 'rbd_qos_bps_limit',
        source: RbdConfigurationSourceField.global,
        type: 0,
        value: '60'
      },
      {
        description: 'The desired limit of IO operations per second.',
        displayName: 'IOPS Limit',
        name: 'rbd_qos_iops_limit',
        source: RbdConfigurationSourceField.global,
        type: 1,
        value: '0'
      }
    ]);
  }));
});
