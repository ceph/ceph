import { ComponentFixture, TestBed, fakeAsync, tick } from '@angular/core/testing';
import { of } from 'rxjs';
import { NvmeofGatewayComponent } from './nvmeof-gateway.component';
import { NvmeofService } from '../../../shared/api/nvmeof.service';
import { HttpClientModule } from '@angular/common/http';
import { SharedModule } from '~/app/shared/shared.module';

const mockGateways = [
  {
    cli_version: '',
    version: '1.2.5',
    name: 'client.nvmeof.rbd.ceph-node-01.jnmnwa',
    group: '',
    addr: '192.168.100.101',
    port: '5500',
    load_balancing_group: 1,
    spdk_version: '24.01'
  }
];

class MockNvmeOfService {
  listGateways() {
    return of(mockGateways);
  }
}

describe('NvmeofGatewayComponent', () => {
  let component: NvmeofGatewayComponent;
  let fixture: ComponentFixture<NvmeofGatewayComponent>;

  beforeEach(fakeAsync(() => {
    TestBed.configureTestingModule({
      declarations: [NvmeofGatewayComponent],
      imports: [HttpClientModule, SharedModule],
      providers: [{ provide: NvmeofService, useClass: MockNvmeOfService }]
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(NvmeofGatewayComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should retrieve gateways', fakeAsync(() => {
    component.getGateways();
    tick();
    expect(component.gateways).toEqual(mockGateways);
  }));
});
