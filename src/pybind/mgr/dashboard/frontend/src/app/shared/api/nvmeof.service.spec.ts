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
  const API_PATH = 'api/nvmeof';
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

  describe('test gateway APIs', () => {
    it('should call listGatewayGroups', () => {
      service.listGatewayGroups().subscribe();
      const req = httpTesting.expectOne(`${API_PATH}/gateway/group`);
      expect(req.request.method).toBe('GET');
    });

    it('should call listGateways', () => {
      service.listGateways().subscribe();
      const req = httpTesting.expectOne(`${API_PATH}/gateway`);
      expect(req.request.method).toBe('GET');
    });
  });

  describe('test subsystems APIs', () => {
    it('should call listSubsystems', () => {
      service.listSubsystems(mockGroupName).subscribe();
      const req = httpTesting.expectOne(`${API_PATH}/subsystem?gw_group=${mockGroupName}`);
      expect(req.request.method).toBe('GET');
    });

    it('should call getSubsystem', () => {
      service.getSubsystem(mockNQN, mockGroupName).subscribe();
      const req = httpTesting.expectOne(
        `${API_PATH}/subsystem/${mockNQN}?gw_group=${mockGroupName}`
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
      const req = httpTesting.expectOne(`${API_PATH}/subsystem`);
      expect(req.request.method).toBe('POST');
    });

    it('should call deleteSubsystem', () => {
      service.deleteSubsystem(mockNQN, mockGroupName).subscribe();
      const req = httpTesting.expectOne(
        `${API_PATH}/subsystem/${mockNQN}?gw_group=${mockGroupName}`
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

  describe('test initiators APIs', () => {
    let request = { host_nqn: '', gw_group: mockGroupName };
    it('should call getInitiators', () => {
      service.getInitiators(mockNQN, mockGroupName).subscribe();
      const req = httpTesting.expectOne(
        `${API_PATH}/subsystem/${mockNQN}/host?gw_group=${mockGroupName}`
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

  describe('test listener APIs', () => {
    it('it should listListeners', () => {
      service.listListeners(mockNQN, mockGroupName).subscribe();
      const req = httpTesting.expectOne(
        `${API_PATH}/subsystem/${mockNQN}/listener?gw_group=${mockGroupName}`
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
      const req = httpTesting.expectOne(`${API_PATH}/subsystem/${mockNQN}/listener`);
      expect(req.request.method).toBe('POST');
    });
    it('should call deleteListener', () => {
      const request = { host_name: 'ceph-node-02', traddr: '192.168.100.102', trsvcid: '4421' };
      service
        .deleteListener(mockNQN, request.host_name, request.traddr, request.trsvcid)
        .subscribe();
      const req = httpTesting.expectOne(
        `${API_PATH}/subsystem/${mockNQN}/listener/${request.host_name}/${request.traddr}?trsvcid=${request.trsvcid}`
      );
      expect(req.request.method).toBe('DELETE');
    });
  });

  describe('test namespace APIs', () => {
    const mockNsid = '1';
    it('should call listNamespaces', () => {
      service.listNamespaces(mockNQN, mockGroupName).subscribe();
      const req = httpTesting.expectOne(
        `${API_PATH}/subsystem/${mockNQN}/namespace?gw_group=${mockGroupName}`
      );
      expect(req.request.method).toBe('GET');
    });
    it('should call getNamespace', () => {
      service.getNamespace(mockNQN, mockNsid, mockGroupName).subscribe();
      const req = httpTesting.expectOne(
        `${API_PATH}/subsystem/${mockNQN}/namespace/${mockNsid}?gw_group=${mockGroupName}`
      );
      expect(req.request.method).toBe('GET');
    });
    it('should call createNamespace', () => {
      const mockNamespaceObj = {
        rbd_image_name: 'nvme_ns_image:12345678',
        rbd_pool: 'rbd',
        size: 1024,
        gw_group: mockGroupName
      };
      service.createNamespace(mockNQN, mockNamespaceObj).subscribe();
      const req = httpTesting.expectOne(`${API_PATH}/subsystem/${mockNQN}/namespace`);
      expect(req.request.method).toBe('POST');
    });
    it('should call updateNamespace', () => {
      const request = { rbd_image_size: 1024, gw_group: mockGroupName };
      service.updateNamespace(mockNQN, mockNsid, request).subscribe();
      const req = httpTesting.expectOne(`${API_PATH}/subsystem/${mockNQN}/namespace/${mockNsid}`);
      expect(req.request.method).toBe('PATCH');
    });
    it('should call deleteNamespace', () => {
      service.deleteNamespace(mockNQN, mockNsid, mockGroupName).subscribe();
      const req = httpTesting.expectOne(
        `${API_PATH}/subsystem/${mockNQN}/namespace/${mockNsid}?gw_group=${mockGroupName}`
      );
      expect(req.request.method).toBe('DELETE');
    });
  });
});
