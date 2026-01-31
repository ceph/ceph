import { ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ActivatedRoute } from '@angular/router';
import { of } from 'rxjs';

import { NvmeofGatewaySubsystemComponent } from './nvmeof-gateway-subsystem.component';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { NvmeofSubsystem } from '~/app/shared/models/nvmeof';

import { SharedModule } from '~/app/shared/shared.module';

describe('NvmeofGatewaySubsystemComponent', () => {
  let component: NvmeofGatewaySubsystemComponent;
  let fixture: ComponentFixture<NvmeofGatewaySubsystemComponent>;
  let nvmeofService: NvmeofService;

  const mockSubsystems: NvmeofSubsystem[] = [
    {
      nqn: 'nqn.2014-08.org.nvmexpress:uuid:1111',
      enable_ha: true,
      allow_any_host: true,
      gw_group: 'group1',
      serial_number: 'SN001',
      model_number: 'MN001',
      min_cntlid: 1,
      max_cntlid: 65519,
      max_namespaces: 256,
      namespace_count: 0,
      subtype: 'NVMe',
      namespaces: []
    } as NvmeofSubsystem,
    {
      nqn: 'nqn.2014-08.org.nvmexpress:uuid:2222',
      enable_ha: false,
      allow_any_host: false,
      gw_group: 'group1',
      serial_number: 'SN002',
      model_number: 'MN002',
      min_cntlid: 1,
      max_cntlid: 65519,
      max_namespaces: 256,
      namespace_count: 0,
      subtype: 'NVMe',
      namespaces: []
    } as NvmeofSubsystem
  ];

  const mockInitiators1 = [{ nqn: 'host1' }, { nqn: 'host2' }];
  const mockInitiators2 = [{ nqn: 'host3' }];

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [NvmeofGatewaySubsystemComponent],
      imports: [HttpClientTestingModule, SharedModule],
      providers: [
        {
          provide: NvmeofService,
          useValue: {
            listSubsystems: jest.fn(() => of(mockSubsystems)),
            getInitiators: jest.fn((nqn) => {
              if (nqn === 'nqn.2014-08.org.nvmexpress:uuid:1111') {
                return of(mockInitiators1);
              }
              return of(mockInitiators2);
            })
          }
        },
        {
          provide: ActivatedRoute,
          useValue: {
            parent: {
              params: of({ group: 'group1' })
            }
          }
        }
      ]
    }).compileComponents();

    nvmeofService = TestBed.inject(NvmeofService);
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(NvmeofGatewaySubsystemComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should verify getData fetches and processes data correctly', () => {
    component.groupName = 'direct-test-group';
    component.getSubsystemsData();

    expect(nvmeofService.listSubsystems).toHaveBeenCalledWith('direct-test-group');
    expect(component.subsystems.length).toBe(2);
    expect(component.subsystems[0].nqn).toBe(mockSubsystems[0].nqn);
  });
});
