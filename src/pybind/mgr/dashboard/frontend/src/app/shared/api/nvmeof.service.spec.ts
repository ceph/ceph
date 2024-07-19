import { TestBed } from '@angular/core/testing';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { configureTestBed } from '~/testing/unit-test-helper';
import { NvmeofService } from '../../shared/api/nvmeof.service';

describe('NvmeofService', () => {
  let service: NvmeofService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    providers: [NvmeofService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.inject(NvmeofService);
    httpTesting = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call listGateways', () => {
    service.listGateways().subscribe();
    const req = httpTesting.expectOne('api/nvmeof/gateway');
    expect(req.request.method).toBe('GET');
  });

  it('should call getSubsystem', () => {
    service.getSubsystem('nqn.2001-07.com.ceph:1721041732363').subscribe();
    const req = httpTesting.expectOne('api/nvmeof/subsystem/nqn.2001-07.com.ceph:1721041732363');
    expect(req.request.method).toBe('GET');
  });

  it('should call createSubsystem', () => {
    const request = {
      nqn: 'nqn.2001-07.com.ceph:1721041732363',
      enable_ha: true,
      initiators: '*'
    };
    service.createSubsystem(request).subscribe();
    const req = httpTesting.expectOne('api/nvmeof/subsystem');
    expect(req.request.method).toBe('POST');
  });

  it('should call getInitiators', () => {
    service.getInitiators('nqn.2001-07.com.ceph:1721041732363').subscribe();
    const req = httpTesting.expectOne(
      'api/nvmeof/subsystem/nqn.2001-07.com.ceph:1721041732363/host'
    );
    expect(req.request.method).toBe('GET');
  });

  it('should call updateInitiators', () => {
    service.updateInitiators('nqn.2001-07.com.ceph:1721041732363', '*').subscribe();
    const req = httpTesting.expectOne(
      'api/nvmeof/subsystem/nqn.2001-07.com.ceph:1721041732363/host/*'
    );
    expect(req.request.method).toBe('PUT');
  });
});
