import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed, fakeAsync, tick } from '@angular/core/testing';
import { ActivatedRoute } from '@angular/router';
import { BehaviorSubject, of, throwError } from 'rxjs';

import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { CephModule } from '~/app/ceph/ceph.module';
import { CephSharedModule } from '~/app/ceph/shared/ceph-shared.module';
import { CoreModule } from '~/app/core/core.module';
import { HostService } from '~/app/shared/api/host.service';
import { CephServiceService } from '~/app/shared/api/ceph-service.service';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { DeletionImpact } from '~/app/shared/enum/delete-confirmation-modal-impact.enum';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { HostStatus } from '~/app/shared/enum/host-status.enum';
import { Permissions } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
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
  let cephServiceService: CephServiceService;
  let modalService: ModalCdsService;
  let taskWrapperService: TaskWrapperService;

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
    providers: [
      { provide: AuthStorageService, useValue: fakeAuthStorageService },
      {
        provide: ActivatedRoute,
        useValue: {
          parent: {
            params: new BehaviorSubject({ group: 'group1' }),
            snapshot: {
              params: { group: 'group1' }
            }
          },
          snapshot: {
            data: { mode: 'selector' }
          }
        }
      },
      ModalCdsService,
      CephServiceService,
      TaskWrapperService,
      NotificationService
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(NvmeofGatewayNodeComponent);
    component = fixture.componentInstance;
    hostService = TestBed.inject(HostService);
    orchService = TestBed.inject(OrchestratorService);
    nvmeofService = TestBed.inject(NvmeofService);
    cephServiceService = TestBed.inject(CephServiceService);
    modalService = TestBed.inject(ModalCdsService);
    taskWrapperService = TestBed.inject(TaskWrapperService);
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
    expect(component.count).toBe(0);
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

    const selectedHosts = component.getSelectedHostnames();

    expect(selectedHosts.length).toBe(2);
    expect(selectedHosts[0]).toEqual(mockGatewayNodes[0].hostname);
    expect(selectedHosts[1]).toEqual(mockGatewayNodes[1].hostname);
  });

  it('should get selected hostnames', () => {
    component.selection = new CdTableSelection();
    component.selection.selected = [mockGatewayNodes[0], mockGatewayNodes[1]];

    const selectedHostnames = component.getSelectedHostnames();

    expect(selectedHostnames).toEqual(['gateway-node-1', 'gateway-node-2']);
  });

  it('should load hosts with orchestrator available and facts feature enabled', fakeAsync(() => {
    spyOn(nvmeofService, 'getAvailableHosts').and.returnValue(of([mockGatewayNodes[2]]));
    component.getHosts(new CdTableFetchDataContext(() => undefined));

    tick(100);
    expect(nvmeofService.getAvailableHosts).toHaveBeenCalled();
    expect(component.hosts.length).toBe(1);
    expect(component.hosts[0]['hostname']).toBe('gateway-node-3');
  }));

  it('should set count to hosts length', fakeAsync(() => {
    spyOn(nvmeofService, 'getAvailableHosts').and.returnValue(of(mockGatewayNodes));
    component.getHosts(new CdTableFetchDataContext(() => undefined));

    tick(100);
    expect(component.count).toBe(component.hosts.length);
  }));

  it('should set count to 0 when no hosts are returned', fakeAsync(() => {
    spyOn(nvmeofService, 'getAvailableHosts').and.returnValue(of([]));
    component.getHosts(new CdTableFetchDataContext(() => undefined));

    tick(100);
    expect(component.count).toBe(0);
    expect(component.hosts.length).toBe(0);
  }));

  it('should handle error when fetching hosts', fakeAsync(() => {
    spyOn(nvmeofService, 'getAvailableHosts').and.returnValue(throwError(() => new Error('Error')));
    const context = new CdTableFetchDataContext(() => undefined);
    spyOn(context, 'error');

    component.getHosts(context);

    tick(100);
    expect(component.isLoadingHosts).toBe(false);
    expect(context.error).toHaveBeenCalled();
  }));

  it('should not re-fetch if already loading', fakeAsync(() => {
    component.isLoadingHosts = true;
    spyOn(nvmeofService, 'getAvailableHosts');

    component.getHosts(new CdTableFetchDataContext(() => undefined));

    tick(100);
    expect(nvmeofService.getAvailableHosts).not.toHaveBeenCalled();
  }));

  it('should unsubscribe on component destroy', fakeAsync(() => {
    spyOn(hostService, 'list').and.returnValue(of([]));
    spyOn(orchService, 'status').and.returnValue(of({} as any));
    spyOn(nvmeofService, 'listGatewayGroups').and.returnValue(of([[]]));
    component.getHosts(new CdTableFetchDataContext(() => undefined));
    tick(100);

    const sub = component['sub'];
    spyOn(sub, 'unsubscribe');

    component.ngOnDestroy();

    expect(sub.unsubscribe).toHaveBeenCalled();
  }));

  it('should handle host list with various label types', fakeAsync(() => {
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
    spyOn(nvmeofService, 'listGatewayGroups').and.returnValue(
      of([
        [
          {
            service_id: 'nvmeof.group1',
            placement: { hosts: ['gateway-node-2'] }
          }
        ]
      ] as any)
    );
    spyOn(hostService, 'checkHostsFactsAvailable').and.returnValue(true);
    component.groupName = 'group1';
    fixture.detectChanges();

    component.getHosts(new CdTableFetchDataContext(() => undefined));

    tick(100);
    expect(component.hosts[0]['labels'].length).toBe(3);
    expect(component.hosts[1]['labels'].length).toBe(0);
  }));

  it('should handle hosts with multiple services', fakeAsync(() => {
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
    spyOn(nvmeofService, 'listGatewayGroups').and.returnValue(
      of([
        [
          {
            service_id: 'nvmeof.group1',
            placement: { hosts: ['gateway-node-2'] }
          }
        ]
      ] as any)
    );
    spyOn(hostService, 'checkHostsFactsAvailable').and.returnValue(true);
    component.groupName = 'group1';
    fixture.detectChanges();

    component.getHosts(new CdTableFetchDataContext(() => undefined));

    tick(100);
    expect(component.hosts[0]['services'].length).toBe(2);
  }));

  it('should initialize table context on first getHosts call', fakeAsync(() => {
    spyOn(hostService, 'list').and.returnValue(of(mockGatewayNodes));
    const mockOrcStatus: any = {
      available: true,
      features: new Map()
    };

    spyOn(orchService, 'status').and.returnValue(of(mockOrcStatus));
    spyOn(nvmeofService, 'listGatewayGroups').and.returnValue(
      of([
        [
          {
            service_id: 'nvmeof.group1',
            placement: { hosts: ['gateway-node-1', 'gateway-node-2'] }
          }
        ]
      ] as any)
    );
    spyOn(hostService, 'checkHostsFactsAvailable').and.returnValue(true);
    component.groupName = 'group1';
    fixture.detectChanges();

    expect((component as any).tableContext).toBeUndefined();

    component.getHosts(new CdTableFetchDataContext(() => undefined));

    tick(100);
    expect((component as any).tableContext).toBeDefined();
  }));

  it('should reuse table context if already set', fakeAsync(() => {
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

    tick(100);
    const storedContext = (component as any).tableContext;
    expect(storedContext).toBe(context);
  }));

  it('should fetch data using fetchHostsAndGroups in details mode', fakeAsync(() => {
    (component as any).route.snapshot.data = { mode: 'details' };
    component.ngOnInit();
    component.groupName = 'group1';

    spyOn(nvmeofService, 'fetchHostsAndGroups').and.returnValue(
      of({
        groups: [
          [
            {
              service_id: 'nvmeof.group1',
              spec: { group: 'group1' },
              placement: { hosts: ['gateway-node-1'] }
            }
          ]
        ],
        hosts: mockGatewayNodes
      } as any)
    );

    fixture.detectChanges();
    component.getHosts(new CdTableFetchDataContext(() => undefined));
    tick(100);

    expect(nvmeofService.fetchHostsAndGroups).toHaveBeenCalled();
    expect(component.hosts.length).toBe(1);
    expect(component.hosts[0].hostname).toBe('gateway-node-1');
  }));

  it('should set selectionType to multiClick in selector mode', () => {
    (component as any).route.snapshot.data = { mode: 'selector' };
    component.ngOnInit();
    expect(component.selectionType).toBe('multiClick');
  });

  it('should set selectionType to single in details mode', () => {
    (component as any).route.snapshot.data = { mode: 'details' };
    component.ngOnInit();
    expect(component.selectionType).toBe('single');
  });

  it('should remove gateway', () => {
    spyOn(modalService, 'show').and.callFake((_component, options) => {
      options.submitActionObservable();
      return undefined;
    });
    const updateSpy = spyOn(cephServiceService, 'update').and.returnValue(of(null));
    component.serviceSpec = {
      service_name: 'nvmeof.group1',
      placement: {
        hosts: ['host1', 'host2']
      }
    } as any;
    component.selection.selected = [{ hostname: 'host1' }] as any;

    component.removeGateway();

    expect(modalService.show).toHaveBeenCalledWith(DeleteConfirmationModalComponent, {
      itemDescription: 'gateway node',
      itemNames: ['host1'],
      actionDescription: 'remove',
      hideDefaultWarning: true,
      impact: DeletionImpact.high,

      bodyContext: {
        deletionMessage:
          'Removing <strong>host1</strong> will detach it from the gateway group and stop handling new I/O requests. Active connections may be disrupted.<br><br>You can re-add this node later if required.'
      },
      submitActionObservable: jasmine.any(Function)
    });

    const call = (modalService.show as jasmine.Spy).calls.mostRecent().args[1]
      .submitActionObservable;
    spyOn(taskWrapperService, 'wrapTaskAroundCall').and.callFake((args: any) => args.call);
    call();

    expect(taskWrapperService.wrapTaskAroundCall).toHaveBeenCalledWith({
      task: jasmine.objectContaining({
        name: 'nvmeof/gateway-node/delete',
        metadata: {
          hostname: 'host1'
        }
      }),
      call: jasmine.any(Object)
    });
    expect(updateSpy).toHaveBeenCalledWith({
      service_name: 'nvmeof.group1',
      placement: {
        hosts: ['host2']
      }
    });
  });
});
