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
    service = TestBed.inject(ConfigurationService);
    httpTesting = TestBed.inject(HttpTestingController);
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

  it('should call bulkCreate', () => {
    const configOptions = {
      configOption1: { section: 'section', value: 'value' },
      configOption2: { section: 'section', value: 'value' }
    };
    service.bulkCreate(configOptions).subscribe();
    const req = httpTesting.expectOne('api/cluster_conf/');
    expect(req.request.method).toBe('PUT');
    expect(req.request.body).toEqual(configOptions);
  });

  it('should call filter', () => {
    const configOptions = ['configOption1', 'configOption2', 'configOption3'];
    service.filter(configOptions).subscribe();
    const req = httpTesting.expectOne(
      'api/cluster_conf/filter?names=configOption1,configOption2,configOption3'
    );
    expect(req.request.method).toBe('GET');
  });

  it('should call delete', () => {
    service.delete('testOption', 'testSection').subscribe();
    const reg = httpTesting.expectOne('api/cluster_conf/testOption?section=testSection');
    expect(reg.request.method).toBe('DELETE');
  });

  it('should get value', () => {
    const config = {
      default: 'a',
      value: [
        { section: 'global', value: 'b' },
        { section: 'mon', value: 'c' },
        { section: 'mon.1', value: 'd' },
        { section: 'mds', value: 'e' }
      ]
    };
    expect(service.getValue(config, 'mon.1')).toBe('d');
    expect(service.getValue(config, 'mon')).toBe('c');
    expect(service.getValue(config, 'mds.1')).toBe('e');
    expect(service.getValue(config, 'mds')).toBe('e');
    expect(service.getValue(config, 'osd')).toBe('b');
    config.value = [];
    expect(service.getValue(config, 'osd')).toBe('a');
  });
});
