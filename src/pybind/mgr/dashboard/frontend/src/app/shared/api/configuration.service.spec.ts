import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { ConfigFormCreateRequestModel } from '../../ceph/cluster/configuration/configuration-form/configuration-form-create-request.model';
import { ConfigurationService } from './configuration.service';

describe('ConfigurationService', () => {
  let service: ConfigurationService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    providers: [ConfigurationService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.get(ConfigurationService);
    httpTesting = TestBed.get(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call getConfigData', () => {
    service.getConfigData().subscribe();
    const req = httpTesting.expectOne('api/cluster_conf/');
    expect(req.request.method).toBe('GET');
  });

  it('should call get', () => {
    service.get('configOption').subscribe();
    const req = httpTesting.expectOne('api/cluster_conf/configOption');
    expect(req.request.method).toBe('GET');
  });

  it('should call create', () => {
    const configOption = new ConfigFormCreateRequestModel();
    configOption.name = 'Test option';
    configOption.value = [
      { section: 'section1', value: 'value1' },
      { section: 'section2', value: 'value2' }
    ];
    service.create(configOption).subscribe();
    const req = httpTesting.expectOne('api/cluster_conf/');
    expect(req.request.method).toBe('POST');
    expect(req.request.body).toEqual(configOption);
  });
});
