import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { of, throwError } from 'rxjs';

import { CephModule } from '~/app/ceph/ceph.module';
import { CephSharedModule } from '~/app/ceph/shared/ceph-shared.module';
import { CoreModule } from '~/app/core/core.module';
import { HostService } from '~/app/shared/api/host.service';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { HostStatus } from '~/app/shared/enum/host-status.enum';
import { Permissions } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { TagModule } from 'carbon-components-angular';
import { NvmeofGatewayNodeComponent } from './nvmeof-gateway-node.component';

describe('NvmeofGatewayNodeComponent', () => {
  let component: NvmeofGatewayNodeComponent;
  let fixture: ComponentFixture<NvmeofGatewayNodeComponent>;
  let hostService: HostService;
  let orchService: OrchestratorService;
  let nvmeofService: NvmeofService;

  const fakeAuthStorageService = {
    getPermissions: () => {
      return new Permissions({ nvmeof: ['read', 'update', 'create', 'delete'] });
    }
  };

  const mockGatewayNodes = [
    {
      hostname: 'gateway-node-1',
      addr: '192.168.1.10',
      status: HostStatus.AVAILABLE,
      labels: ['nvmeof', 'gateway'],
      services: [
        {
          type: 'nvmeof-gw',
          id: 'gateway-1'
        }
      ],
      ceph_version: 'ceph version 18.0.0',
      sources: {
        ceph: true,
        orchestrator: true
      },
      service_instances: [] as any[]
    },
    {
      hostname: 'gateway-node-2',
      addr: '192.168.1.11',
      status: HostStatus.MAINTENANCE,
      labels: ['nvmeof'],
      services: [
        {
          type: 'nvmeof-gw',
          id: 'gateway-2'
        }
      ],
      ceph_version: 'ceph version 18.0.0',
      sources: {
        ceph: true,
        orchestrator: true
      },
      service_instances: [] as any[]
    },
    {
      hostname: 'gateway-node-3',
      addr: '192.168.1.12',
      status: '',
      labels: [],
      services: [],
      ceph_version: 'ceph version 18.0.0',
      sources: {
        ceph: true,
        orchestrator: false
      },
      service_instances: [] as any[]
    }
  ];

  configureTestBed({
    imports: [
      BrowserAnimationsModule,
      CephSharedModule,
      SharedModule,
      HttpClientTestingModule,
      RouterTestingModule,
      CephModule,
      CoreModule,
      TagModule
    ],
    providers: [{ provide: AuthStorageService, useValue: fakeAuthStorageService }]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(NvmeofGatewayNodeComponent);
    component = fixture.componentInstance;
    hostService = TestBed.inject(HostService);
    orchService = TestBed.inject(OrchestratorService);
    nvmeofService = TestBed.inject(NvmeofService);
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should initialize columns on component init', () => {
    component.ngOnInit();

    expect(component.columns).toBeDefined();
    expect(component.columns.length).toBeGreaterThan(0);
    expect(component.columns[0].name).toBe('Hostname');
    expect(component.columns[0].prop).toBe('hostname');
  });

  it('should have all required columns defined', () => {
    component.ngOnInit();

    const columnProps = component.columns.map((col) => col.prop);
    expect(columnProps).toContain('hostname');
    expect(columnProps).toContain('addr');
    expect(columnProps).toContain('status');
    expect(columnProps).toContain('labels');
  });

  it('should initialize with default values', () => {
    expect(component.hosts).toEqual([]);
    expect(component.isLoadingHosts).toBe(false);
    expect(component.count).toBe(5);
    expect(component.permission).toBeDefined();
  });

  it('should update selection', () => {
    const selection = new CdTableSelection();
    selection.selected = [mockGatewayNodes[0]];

    component.updateSelection(selection);

    expect(component.selection).toBe(selection);
    expect(component.selection.selected.length).toBe(1);
  });

  it('should get selected hosts', () => {
    component.selection = new CdTableSelection();
    component.selection.selected = [mockGatewayNodes[0], mockGatewayNodes[1]];

    // ensure hosts list contains the selected hosts for lookup
    component.hosts = [mockGatewayNodes[0], mockGatewayNodes[1]];

    const selectedHosts = component
      .getSelectedHostnames()
      .map((hostname) => component.hosts.find((host) => host.hostname === hostname));

    expect(selectedHosts.length).toBe(2);
    expect(selectedHosts[0]).toEqual(mockGatewayNodes[0]);
    expect(selectedHosts[1]).toEqual(mockGatewayNodes[1]);
  });

  it('should get selected hostnames', () => {
    component.selection = new CdTableSelection();
    component.selection.selected = [mockGatewayNodes[0], mockGatewayNodes[1]];

    const selectedHostnames = component.getSelectedHostnames();

    expect(selectedHostnames).toEqual(['gateway-node-1', 'gateway-node-2']);
  });

  it('should load hosts with orchestrator available and facts feature enabled', (done) => {
    const hostListSpy = spyOn(hostService, 'list').and.returnValue(of(mockGatewayNodes));
    const mockOrcStatus: any = {
      available: true,
      features: new Map([['get_facts', { available: true }]])
    };

    spyOn(orchService, 'status').and.returnValue(of(mockOrcStatus));
    spyOn(nvmeofService, 'listGatewayGroups').and.returnValue(of([[]]));
    spyOn(hostService, 'checkHostsFactsAvailable').and.returnValue(true);
    fixture.detectChanges();

    component.getHosts(new CdTableFetchDataContext(() => undefined));

    setTimeout(() => {
      expect(hostListSpy).toHaveBeenCalled();
      // Only hosts with status 'available', '' or 'running' are included (excluding 'maintenance')
      expect(component.hosts.length).toBe(2);
      expect(component.isLoadingHosts).toBe(false);
      expect(component.hosts[0]['hostname']).toBe('gateway-node-1');
      expect(component.hosts[0]['status']).toBe(HostStatus.AVAILABLE);
      done();
    }, 100);
  });

  it('should normalize empty status to "available"', (done) => {
    spyOn(hostService, 'list').and.returnValue(of(mockGatewayNodes));
    const mockOrcStatus: any = {
      available: true,
      features: new Map()
    };

    spyOn(orchService, 'status').and.returnValue(of(mockOrcStatus));
    spyOn(nvmeofService, 'listGatewayGroups').and.returnValue(of([[]]));
    spyOn(hostService, 'checkHostsFactsAvailable').and.returnValue(true);
    fixture.detectChanges();

    component.getHosts(new CdTableFetchDataContext(() => undefined));

    setTimeout(() => {
      // Host at index 1 in filtered list (gateway-node-3 has empty status which becomes 'available')
      const nodeWithEmptyStatus = component.hosts.find((h) => h.hostname === 'gateway-node-3');
      expect(nodeWithEmptyStatus?.['status']).toBe(HostStatus.AVAILABLE);
      done();
    }, 100);
  });

  it('should set count to hosts length', (done) => {
    spyOn(hostService, 'list').and.returnValue(of(mockGatewayNodes));
    const mockOrcStatus: any = {
      available: true,
      features: new Map()
    };

    spyOn(orchService, 'status').and.returnValue(of(mockOrcStatus));
    spyOn(nvmeofService, 'listGatewayGroups').and.returnValue(of([[]]));
    spyOn(hostService, 'checkHostsFactsAvailable').and.returnValue(true);
    fixture.detectChanges();

    component.getHosts(new CdTableFetchDataContext(() => undefined));

    setTimeout(() => {
      // Count should equal the filtered hosts length
      expect(component.count).toBe(component.hosts.length);
      done();
    }, 100);
  });

  it('should set count to 0 when no hosts are returned', (done) => {
    spyOn(hostService, 'list').and.returnValue(of([]));
    const mockOrcStatus: any = {
      available: true,
      features: new Map()
    };

    spyOn(orchService, 'status').and.returnValue(of(mockOrcStatus));
    spyOn(nvmeofService, 'listGatewayGroups').and.returnValue(of([[]]));
    spyOn(hostService, 'checkHostsFactsAvailable').and.returnValue(true);
    fixture.detectChanges();

    component.getHosts(new CdTableFetchDataContext(() => undefined));

    setTimeout(() => {
      expect(component.count).toBe(0);
      expect(component.hosts.length).toBe(0);
      done();
    }, 100);
  });

  it('should handle error when fetching hosts', (done) => {
    const errorMsg = 'Failed to fetch hosts';
    spyOn(hostService, 'list').and.returnValue(throwError(() => new Error(errorMsg)));
    const mockOrcStatus: any = {
      available: true,
      features: new Map()
    };

    spyOn(orchService, 'status').and.returnValue(of(mockOrcStatus));
    spyOn(nvmeofService, 'listGatewayGroups').and.returnValue(of([[]]));
    spyOn(hostService, 'checkHostsFactsAvailable').and.returnValue(true);
    fixture.detectChanges();

    const context = new CdTableFetchDataContext(() => undefined);
    spyOn(context, 'error');

    component.getHosts(context);

    setTimeout(() => {
      expect(component.isLoadingHosts).toBe(false);
      expect(context.error).toHaveBeenCalled();
      done();
    }, 100);
  });

  it('should check hosts facts available when orchestrator features present', () => {
    component.orchStatus = {
      available: true,
      features: new Map([['get_facts', { available: true }]])
    } as any;

    spyOn(hostService, 'checkHostsFactsAvailable').and.returnValue(true);

    const result = component.checkHostsFactsAvailable();

    expect(result).toBe(true);
  });

  it('should return false when get_facts feature is not available', () => {
    component.orchStatus = {
      available: true,
      features: new Map([['other_feature', { available: true }]])
    } as any;

    const result = component.checkHostsFactsAvailable();

    expect(result).toBe(false);
  });

  it('should return false when orchestrator status features are empty', () => {
    component.orchStatus = {
      available: true,
      features: new Map()
    } as any;

    const result = component.checkHostsFactsAvailable();

    expect(result).toBe(false);
  });

  it('should return false when orchestrator status is undefined', () => {
    component.orchStatus = undefined;

    const result = component.checkHostsFactsAvailable();

    expect(result).toBe(false);
  });

  it('should not re-fetch if already loading', (done) => {
    component.isLoadingHosts = true;
    const hostListSpy = spyOn(hostService, 'list');

    component.getHosts(new CdTableFetchDataContext(() => undefined));

    setTimeout(() => {
      expect(hostListSpy).not.toHaveBeenCalled();
      done();
    }, 100);
  });

  it('should unsubscribe on component destroy', () => {
    const destroy$ = component['destroy$'];
    spyOn(destroy$, 'next');
    spyOn(destroy$, 'complete');

    component.ngOnDestroy();

    expect(destroy$.next).toHaveBeenCalled();
    expect(destroy$.complete).toHaveBeenCalled();
  });

  it('should handle host list with various label types', (done) => {
    const hostsWithLabels = [
      {
        ...mockGatewayNodes[0],
        labels: ['nvmeof', 'gateway', 'high-priority']
      },
      {
        ...mockGatewayNodes[2],
        labels: []
      }
    ];

    spyOn(hostService, 'list').and.returnValue(of(hostsWithLabels));
    const mockOrcStatus: any = {
      available: true,
      features: new Map()
    };

    spyOn(orchService, 'status').and.returnValue(of(mockOrcStatus));
    spyOn(nvmeofService, 'listGatewayGroups').and.returnValue(of([[]]));
    spyOn(hostService, 'checkHostsFactsAvailable').and.returnValue(true);
    fixture.detectChanges();

    component.getHosts(new CdTableFetchDataContext(() => undefined));

    setTimeout(() => {
      expect(component.hosts[0]['labels'].length).toBe(3);
      expect(component.hosts[1]['labels'].length).toBe(0);
      done();
    }, 100);
  });

  it('should handle hosts with multiple services', (done) => {
    const hostsWithServices = [
      {
        ...mockGatewayNodes[0],
        services: [
          { type: 'nvmeof-gw', id: 'gateway-1' },
          { type: 'mon', id: '0' }
        ]
      }
    ];

    spyOn(hostService, 'list').and.returnValue(of(hostsWithServices));
    const mockOrcStatus: any = {
      available: true,
      features: new Map()
    };

    spyOn(orchService, 'status').and.returnValue(of(mockOrcStatus));
    spyOn(nvmeofService, 'listGatewayGroups').and.returnValue(of([[]]));
    spyOn(hostService, 'checkHostsFactsAvailable').and.returnValue(true);
    fixture.detectChanges();

    component.getHosts(new CdTableFetchDataContext(() => undefined));

    setTimeout(() => {
      expect(component.hosts[0]['services'].length).toBe(2);
      done();
    }, 100);
  });

  it('should initialize table context on first getHosts call', (done) => {
    spyOn(hostService, 'list').and.returnValue(of(mockGatewayNodes));
    const mockOrcStatus: any = {
      available: true,
      features: new Map()
    };

    spyOn(orchService, 'status').and.returnValue(of(mockOrcStatus));
    spyOn(nvmeofService, 'listGatewayGroups').and.returnValue(of([[]]));
    spyOn(hostService, 'checkHostsFactsAvailable').and.returnValue(true);
    fixture.detectChanges();

    expect((component as any).tableContext).toBeNull();

    component.getHosts(new CdTableFetchDataContext(() => undefined));

    setTimeout(() => {
      expect((component as any).tableContext).not.toBeNull();
      done();
    }, 100);
  });

  it('should reuse table context if already set', (done) => {
    const context = new CdTableFetchDataContext(() => undefined);
    spyOn(hostService, 'list').and.returnValue(of(mockGatewayNodes));
    const mockOrcStatus: any = {
      available: true,
      features: new Map()
    };

    spyOn(orchService, 'status').and.returnValue(of(mockOrcStatus));
    spyOn(nvmeofService, 'listGatewayGroups').and.returnValue(of([[]]));
    spyOn(hostService, 'checkHostsFactsAvailable').and.returnValue(true);
    fixture.detectChanges();

    component.getHosts(context);

    setTimeout(() => {
      const storedContext = (component as any).tableContext;
      expect(storedContext).toBe(context);
      done();
    }, 100);
  });
});
