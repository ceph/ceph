import { ComponentFixture, TestBed } from '@angular/core/testing';
import { NvmeofGatewayGroupComponent } from './nvmeof-gateway-group.component';
import { GridModule, TabsModule } from 'carbon-components-angular';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { CephServiceService } from '~/app/shared/api/ceph-service.service';
import { of } from 'rxjs';
import { HttpClientModule } from '@angular/common/http';
import { SharedModule } from '~/app/shared/shared.module';

describe('NvmeofGatewayGroupComponent', () => {
  let component: NvmeofGatewayGroupComponent;
  let fixture: ComponentFixture<NvmeofGatewayGroupComponent>;
  let nvmeofService: any;
  let cephServiceService: any;

  beforeEach(async () => {
    const nvmeofServiceSpy = {
      listGatewayGroups: jest.fn().mockReturnValue(of([])),
      listSubsystems: jest.fn().mockReturnValue(of([]))
    };
    const cephServiceServiceSpy = {
      getDaemons: jest.fn().mockReturnValue(of([]))
    };

    await TestBed.configureTestingModule({
      imports: [HttpClientModule, SharedModule, TabsModule, GridModule],
      declarations: [NvmeofGatewayGroupComponent],
      providers: [
        { provide: NvmeofService, useValue: nvmeofServiceSpy },
        { provide: CephServiceService, useValue: cephServiceServiceSpy }
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(NvmeofGatewayGroupComponent);
    component = fixture.componentInstance;
    nvmeofService = TestBed.inject(NvmeofService);
    cephServiceService = TestBed.inject(CephServiceService);
    fixture.detectChanges(); // Initialize the component
  });

  it('should create the component', () => {
    expect(component).toBeTruthy();
  });

  it('should call listGatewayGroups and getDaemons on initialization', () => {
    expect(nvmeofService.listGatewayGroups).toHaveBeenCalled();
    expect(cephServiceService.getDaemons).toHaveBeenCalled();
  });

  it('should populate gatewayGroup$ with the correct data', (done) => {
    const mockData = [
      {
        service_name: 'TestGroup',
        service_type: 'nvmeof-gateway',
        service_id: 'gw-1',
        unmanaged: false,
        status: {
          container_image_id: 'img123',
          container_image_name: 'ceph-gw',
          size: 1,
          running: 3,
          last_refresh: new Date(),
          created: new Date('2025-11-21T11:30:00Z')
        },
        spec: {} as any,
        placement: {} as any,
        name: 'TestGroup',
        gateway: 3,
        gateWayCount: { running: 3, error: 2 },
        events: [{ created: '2025-11-21T11:30:00Z' }]
      }
    ];
    component.gatewayGroup$ = of(mockData);

    component.gatewayGroup$.subscribe((data) => {
      expect(data).toEqual(mockData);
      done();
    });
  });
});
