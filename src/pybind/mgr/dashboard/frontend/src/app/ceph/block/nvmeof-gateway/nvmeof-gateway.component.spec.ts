import { ComponentFixture, TestBed } from '@angular/core/testing';
import { of } from 'rxjs';
import { NvmeofGatewayComponent } from './nvmeof-gateway.component';
import { NvmeofService } from '../../../shared/api/nvmeof.service';
import { HttpClientModule } from '@angular/common/http';
import { SharedModule } from '~/app/shared/shared.module';
import { ComboBoxModule, GridModule } from 'carbon-components-angular';
import { NvmeofTabsComponent } from '../nvmeof-tabs/nvmeof-tabs.component';
import { CephServiceService } from '~/app/shared/api/ceph-service.service';
import { CephServiceSpec } from '~/app/shared/models/service.interface';

const mockServiceDaemons = [
  {
    daemon_type: 'nvmeof',
    daemon_id: 'nvmeof.default.ceph-node-01.kdcguk',
    daemon_name: 'nvmeof.nvmeof.default.ceph-node-01.kdcguk',
    hostname: 'ceph-node-01',
    container_id: '6fe5a9ae9c96',
    container_image_id: '32a3d75b7c146d6c37b04ee3c9ba883ab88a8f7ae8f286de268d0f41ebd86a51',
    container_image_name: 'quay.io/ceph/nvmeof:1.2.17',
    container_image_digests: [
      'quay.io/ceph/nvmeof@sha256:4308d05d3bb2167fc695d755316fec8d12ec3f00eb7639eeeabad38a5c4df0f9'
    ],
    memory_usage: 89443532,
    cpu_percentage: '98.87%',
    version: '1.2.17',
    status: 1,
    status_desc: 'running'
  },
  {
    daemon_type: 'nvmeof',
    daemon_id: 'nvmeof.default.ceph-node-02.hybprc',
    daemon_name: 'nvmeof.nvmeof.default.ceph-node-02.hybprc',
    hostname: 'ceph-node-02',
    container_id: '2b061130726b',
    container_image_id: '32a3d75b7c146d6c37b04ee3c9ba883ab88a8f7ae8f286de268d0f41ebd86a51',
    container_image_name: 'quay.io/ceph/nvmeof:1.2.17',
    container_image_digests: [
      'quay.io/ceph/nvmeof@sha256:4308d05d3bb2167fc695d755316fec8d12ec3f00eb7639eeeabad38a5c4df0f9'
    ],
    memory_usage: 89328189,
    cpu_percentage: '98.89%',
    version: '1.2.17',
    status: 1,
    status_desc: 'running'
  }
];

const mockGateways = [
  {
    id: 'client.nvmeof.nvmeof.default.ceph-node-01.kdcguk',
    hostname: 'ceph-node-01',
    status_desc: 'running',
    status: 1
  },
  {
    id: 'client.nvmeof.nvmeof.default.ceph-node-02.hybprc',
    hostname: 'ceph-node-02',
    status_desc: 'running',
    status: 1
  }
];

const mockformattedGwGroups = [
  {
    content: 'default',
    serviceName: 'nvmeof.rbd.default'
  },
  {
    content: 'foo',
    serviceName: 'nvmeof.rbd.foo'
  }
];

const mockServices = [
  [
    {
      service_name: 'nvmeof.rbd.default',
      service_type: 'nvmeof',
      unmanaged: false,
      spec: {
        group: 'default'
      }
    },
    {
      service_name: 'nvmeof.rbd.foo',
      service_type: 'nvmeof',
      unmanaged: false,
      spec: {
        group: 'foo'
      }
    }
  ],
  2
];
class MockNvmeOfService {
  listGatewayGroups() {
    return of(mockServices);
  }

  formatGwGroupsList(_data: CephServiceSpec[][]) {
    return mockformattedGwGroups;
  }
}

class MockCephServiceService {
  getDaemons(_service: string) {
    return of(mockServiceDaemons);
  }
}

describe('NvmeofGatewayComponent', () => {
  let component: NvmeofGatewayComponent;
  let fixture: ComponentFixture<NvmeofGatewayComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [NvmeofGatewayComponent, NvmeofTabsComponent],
      imports: [HttpClientModule, SharedModule, ComboBoxModule, GridModule],
      providers: [
        { provide: NvmeofService, useClass: MockNvmeOfService },
        { provide: CephServiceService, useClass: MockCephServiceService }
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(NvmeofGatewayComponent);
    component = fixture.componentInstance;
    component.ngOnInit();
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should load gateway groups correctly', () => {
    expect(component.gwGroups.length).toBe(2);
    expect(component.gwGroups).toStrictEqual(mockformattedGwGroups);
  });

  it('should set service name of gateway groups correctly', () => {
    expect(component.groupService).toBe(mockServices[0][0].service_name);
  });

  it('should set gateways correctly', () => {
    expect(component.gateways.length).toBe(2);
    expect(component.gateways).toStrictEqual(mockGateways);
  });
});
