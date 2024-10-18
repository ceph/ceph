import { TestBed } from '@angular/core/testing';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { configureTestBed } from '~/testing/unit-test-helper';
import { NvmeofService } from '../../shared/api/nvmeof.service';
import { throwError } from 'rxjs';

describe('NvmeofService', () => {
  let service: NvmeofService;
  let httpTesting: HttpTestingController;
  const mockGroupName = 'default';
  const mockNQN = 'nqn.2001-07.com.ceph:1721041732363';
  const UI_API_PATH = 'ui-api/nvmeof';
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

  describe('gateway', () => {
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
  });

  describe('subsystems', () => {
    it('should call listSubsystems', () => {
      service.listSubsystems(mockGroupName).subscribe();
      const req = httpTesting.expectOne(`api/nvmeof/subsystem?gw_group=${mockGroupName}`);
      expect(req.request.method).toBe('GET');
    });

    it('should call getSubsystem', () => {
      service.getSubsystem(mockNQN, mockGroupName).subscribe();
      const req = httpTesting.expectOne(
        `api/nvmeof/subsystem/${mockNQN}?gw_group=${mockGroupName}`
      );
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
      const req = httpTesting.expectOne(
        `api/nvmeof/subsystem/${mockNQN}?gw_group=${mockGroupName}`
      );
      expect(req.request.method).toBe('DELETE');
    });
    it('should call isSubsystemPresent', () => {
      spyOn(service, 'getSubsystem').and.returnValue(throwError('test'));
      service.isSubsystemPresent(mockNQN, mockGroupName).subscribe((res) => {
        expect(res).toBe(false);
      });
    });
  });

  describe('initiators', () => {
    let request = { host_nqn: '', gw_group: mockGroupName };
    it('should call getInitiators', () => {
      service.getInitiators(mockNQN, mockGroupName).subscribe();
      const req = httpTesting.expectOne(
        `api/nvmeof/subsystem/${mockNQN}/host?gw_group=${mockGroupName}`
      );
      expect(req.request.method).toBe('GET');
    });
    it('should call addInitiators', () => {
      service.addInitiators(mockNQN, request).subscribe();
      const req = httpTesting.expectOne(`${UI_API_PATH}/subsystem/${mockNQN}/host`);
      expect(req.request.method).toBe('POST');
    });
    it('should call removeInitiators', () => {
      service.removeInitiators(mockNQN, request).subscribe();
      const req = httpTesting.expectOne(
        `${UI_API_PATH}/subsystem/${mockNQN}/host/${request.host_nqn}/${mockGroupName}`
      );
      expect(req.request.method).toBe('DELETE');
    });
  });

  describe('listener', () => {
    let group = mockGroupName;
    it('it should listListeners', () => {
      service.listListeners(mockNQN, group).subscribe();
      const req = httpTesting.expectOne(
        `api/nvmeof/subsystem/${mockNQN}/listener?gw_group=${mockGroupName}`
      );
      expect(req.request.method).toBe('GET');
    });
    it('should call createListener', () => {
      const request = {
        gw_group: mockGroupName,
        host_name: 'ceph-node-02',
        traddr: '192.168.100.102',
        trsvcid: 4421
      };
      service.createListener(mockNQN, request).subscribe();
      const req = httpTesting.expectOne(`api/nvmeof/subsystem/${mockNQN}/listener`);
      expect(req.request.method).toBe('POST');
    });
    it('should call deleteListener', () => {
      const request = { host_name: 'ceph-node-02', traddr: '192.168.100.102', trsvcid: '4421' };
      service
        .deleteListener(mockNQN, request.host_name, request.traddr, request.trsvcid)
        .subscribe();
      const req = httpTesting.expectOne(
        `api/nvmeof/subsystem/${mockNQN}/listener/${request.host_name}/${request.traddr}?trsvcid=${request.trsvcid}`
      );
      expect(req.request.method).toBe('DELETE');
    });
  });

  describe('namespace', () => {
    let nsid = '1';
    it('should call listNamespaces', () => {
      service.listNamespaces(mockNQN, mockGroupName).subscribe();
      const req = httpTesting.expectOne(
        `api/nvmeof/subsystem/${mockNQN}/namespace?gw_group=${mockGroupName}`
      );
      expect(req.request.method).toBe('GET');
    });
    it('should call getNamespace', () => {
      service.getNamespace(mockNQN, nsid, mockGroupName).subscribe();
      const req = httpTesting.expectOne(
        `api/nvmeof/subsystem/${mockNQN}/namespace/${nsid}?gw_group=${mockGroupName}`
      );
      expect(req.request.method).toBe('GET');
    });
    it('should call createNamespace', () => {
      const mockNamespaceObj = {
        rbd_image_name: 'string',
        rbd_pool: 'rbd',
        size: 1024,
        gw_group: mockGroupName
      };
      service.createNamespace(mockNQN, mockNamespaceObj).subscribe();
      const req = httpTesting.expectOne(`api/nvmeof/subsystem/${mockNQN}/namespace`);
      expect(req.request.method).toBe('POST');
    });
    it('should call updateNamespace', () => {
      const request = { rbd_image_size: 1024, gw_group: mockGroupName };
      service.updateNamespace(mockNQN, nsid, request).subscribe();
      const req = httpTesting.expectOne(`api/nvmeof/subsystem/${mockNQN}/namespace/${nsid}`);
      expect(req.request.method).toBe('PATCH');
    });
    it('should call deleteNamespace', () => {
      service.deleteNamespace(mockNQN, nsid, mockGroupName).subscribe();
      const req = httpTesting.expectOne(
        `api/nvmeof/subsystem/${mockNQN}/namespace/${nsid}?gw_group=${mockGroupName}`
      );
      expect(req.request.method).toBe('DELETE');
    });
  });
});
