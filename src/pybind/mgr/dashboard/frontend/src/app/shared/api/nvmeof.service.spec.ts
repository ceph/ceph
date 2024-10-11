import { TestBed } from '@angular/core/testing';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { configureTestBed } from '~/testing/unit-test-helper';
import { NvmeofService } from '../../shared/api/nvmeof.service';

describe('NvmeofService', () => {
  let service: NvmeofService;
  let httpTesting: HttpTestingController;
  const mockGroupName = 'default';
  const mockNQN = 'nqn.2001-07.com.ceph:1721041732363';

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

  // gateways
  it('should call listGatewayGroups', () => {
    service.listGatewayGroups().subscribe();
    const req = httpTesting.expectOne('api/nvmeof/gateway/group');
    expect(req.request.method).toBe('GET');
  });

  it('should call listGateways', () => {
    service.listGateways().subscribe();
    const req = httpTesting.expectOne('api/nvmeof/gateway');
    expect(req.request.method).toBe('GET');
  });

  // subsystems
  it('should call listSubsystems', () => {
    service.listSubsystems(mockGroupName).subscribe();
    const req = httpTesting.expectOne(`api/nvmeof/subsystem?gw_group=${mockGroupName}`);
    expect(req.request.method).toBe('GET');
  });

  it('should call getSubsystem', () => {
    service.getSubsystem(mockNQN, mockGroupName).subscribe();
    const req = httpTesting.expectOne(`api/nvmeof/subsystem/${mockNQN}?gw_group=${mockGroupName}`);
    expect(req.request.method).toBe('GET');
  });

  it('should call createSubsystem', () => {
    const request = {
      nqn: mockNQN,
      enable_ha: true,
      initiators: '*',
      gw_group: mockGroupName
    };
    service.createSubsystem(request).subscribe();
    const req = httpTesting.expectOne('api/nvmeof/subsystem');
    expect(req.request.method).toBe('POST');
  });

  it('should call deleteSubsystem', () => {
    service.deleteSubsystem(mockNQN, mockGroupName).subscribe();
    const req = httpTesting.expectOne(`api/nvmeof/subsystem/${mockNQN}?gw_group=${mockGroupName}`);
    expect(req.request.method).toBe('DELETE');
  });

  // initiators
  it('should call getInitiators', () => {
    service.getInitiators(mockNQN, mockGroupName).subscribe();
    const req = httpTesting.expectOne(
      `api/nvmeof/subsystem/${mockNQN}/host?gw_group=${mockGroupName}`
    );
    expect(req.request.method).toBe('GET');
  });
});
