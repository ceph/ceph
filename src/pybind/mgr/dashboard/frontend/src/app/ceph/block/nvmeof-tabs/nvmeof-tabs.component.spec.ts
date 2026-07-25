import {
  ComponentFixture,
  TestBed,
  discardPeriodicTasks,
  fakeAsync,
  tick
} from '@angular/core/testing';
import { ActivatedRoute, Event as RouterEvent, NavigationEnd, Router } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';
import { BehaviorSubject, Subject, of } from 'rxjs';

import { TabsModule } from 'carbon-components-angular';

import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { PerformanceCardService } from '~/app/shared/api/performance-card.service';
import { CephServiceSpec } from '~/app/shared/models/service.interface';
import { PrometheusAlertService } from '~/app/shared/services/prometheus-alert.service';
import { SharedModule } from '~/app/shared/shared.module';
import { NvmeofStateService } from '../nvmeof-state.service';
import { NvmeofTabsComponent } from './nvmeof-tabs.component';
import { NvmeofSetupCardsComponent } from '../nvmeof-setup-cards/nvmeof-setup-cards.component';

const makeGroup = (name: string, running: number, size: number): CephServiceSpec => ({
  service_name: `nvmeof.${name}`,
  service_type: 'nvmeof',
  service_id: name,
  unmanaged: false,
  spec: { group: name } as CephServiceSpec['spec'],
  status: {
    container_image_id: '',
    container_image_name: '',
    size,
    running,
    last_refresh: new Date('2026-05-25T00:00:00'),
    created: new Date('2026-05-25T00:00:00')
  },
  placement: { hosts: [`host-${name}`] }
});

describe('NvmeofTabsComponent', () => {
  let component: NvmeofTabsComponent;
  let fixture: ComponentFixture<NvmeofTabsComponent>;
  let router: Router;
  let nvmeofServiceSpy: any;
  let nvmeofService: NvmeofService;
  let performanceCardService: PerformanceCardService;
  let prometheusAlertService: PrometheusAlertService;
  let queryParams$: BehaviorSubject<any>;
  let refresh$: Subject<void>;
  let routerEvents$: Subject<RouterEvent>;

  const mockGroups = [
    [{ spec: { group: 'grp1' }, placement: { hosts: ['h1'] }, status: { running: 1, size: 1 } }]
  ];
  const mockSubsystems = [{ nqn: 'sub1', namespace_count: 2, initiator_count: 0 }];
  const mockNamespaces = [{ nsid: 1, rbd_image_name: 'img1', rbd_pool_name: 'rbd' }];

  const setQueryParams = (params: any) => queryParams$.next(params);
  const emitRefresh = () => refresh$.next();

  beforeEach(async () => {
    queryParams$ = new BehaviorSubject<any>({ group: 'grp1' });
    refresh$ = new Subject<void>();
    nvmeofServiceSpy = {
      listGatewayGroups: jest.fn().mockReturnValue(of(mockGroups)),
      listSubsystems: jest.fn().mockReturnValue(of(mockSubsystems)),
      listNamespaces: jest.fn().mockReturnValue(of(mockNamespaces))
    };

    await TestBed.configureTestingModule({
      declarations: [NvmeofTabsComponent],
      imports: [RouterTestingModule, SharedModule, TabsModule, NvmeofSetupCardsComponent],
      providers: [
        { provide: NvmeofService, useValue: nvmeofServiceSpy },
        {
          provide: PerformanceCardService,
          useValue: {
            getNvmeofResourceStats: jest.fn().mockReturnValue(
              of({
                gatewayGroups: 1,
                subsystems: 1,
                namespaces: 2,
                hosts: 1,
                activeConnections: 0
              })
            ),
            getNvmeofThroughput: jest.fn().mockReturnValue(of({ reads: 0, writes: 0, combined: 0 }))
          }
        },
        {
          provide: PrometheusAlertService,
          useValue: {
            fetchGroupedAlerts: jest.fn().mockReturnValue(of([]))
          }
        },
        { provide: ActivatedRoute, useValue: { queryParams: queryParams$.asObservable() } },
        { provide: NvmeofStateService, useValue: { refresh$: refresh$.asObservable() } }
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(NvmeofTabsComponent);
    component = fixture.componentInstance;
    router = TestBed.inject(Router);
    nvmeofService = TestBed.inject(NvmeofService);
    performanceCardService = TestBed.inject(PerformanceCardService);
    prometheusAlertService = TestBed.inject(PrometheusAlertService);
    routerEvents$ = new Subject<RouterEvent>();
    jest.spyOn(router, 'events', 'get').mockReturnValue(routerEvents$.asObservable());
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should default activeTab to gateways', () => {
    jest.spyOn(router, 'url', 'get').mockReturnValue('/block/nvmeof/gateways');
    component.ngOnInit();
    expect(component.activeTab).toBe(component.Tabs.gateways);
  });

  it('should set activeTab to subsystems when URL contains subsystems', () => {
    jest.spyOn(router, 'url', 'get').mockReturnValue('/block/nvmeof/subsystems');
    component.ngOnInit();
    expect(component.activeTab).toBe(component.Tabs.subsystems);
  });

  it('should set activeTab to namespaces when URL contains namespaces', () => {
    jest.spyOn(router, 'url', 'get').mockReturnValue('/block/nvmeof/namespaces');
    component.ngOnInit();
    expect(component.activeTab).toBe(component.Tabs.namespaces);
  });

  it('should fallback to gateways when URL does not match any tab', () => {
    jest.spyOn(router, 'url', 'get').mockReturnValue('/block/nvmeof/unknown');
    component.ngOnInit();
    expect(component.activeTab).toBe(component.Tabs.gateways);
  });

  it('should hide the shell on namespace create routes', () => {
    jest.spyOn(router, 'url', 'get').mockReturnValue('/block/nvmeof/namespaces/create');
    component.ngOnInit();
    expect(component.showTabsShell).toBe(false);
  });

  it('should keep the shell visible on namespace list routes', () => {
    jest.spyOn(router, 'url', 'get').mockReturnValue('/block/nvmeof/namespaces');
    component.ngOnInit();
    expect(component.showTabsShell).toBe(true);
  });

  it('should keep the shell visible on list routes with a secondary outlet', () => {
    jest
      .spyOn(router, 'url', 'get')
      .mockReturnValue('/block/nvmeof/subsystems(modal:create)?group=default');
    component.ngOnInit();
    expect(component.showTabsShell).toBe(true);
  });

  it('should hide the shell when primary route is a create page with secondary outlet', () => {
    jest
      .spyOn(router, 'url', 'get')
      .mockReturnValue('/block/nvmeof/subsystems/create(modal:create)?group=default');
    component.ngOnInit();
    expect(component.showTabsShell).toBe(false);
  });

  it('should navigate to correct path on tab selection', () => {
    spyOn(router, 'navigate');
    component.onSelected(component.Tabs.subsystems);
    expect(component.activeTab).toBe(component.Tabs.subsystems);
    expect(router.navigate).toHaveBeenCalledWith(['block/nvmeof/subsystems'], {
      queryParamsHandling: 'preserve'
    });
  });

  it('should navigate to gateways on selecting gateways tab', () => {
    spyOn(router, 'navigate');
    component.onSelected(component.Tabs.gateways);
    expect(component.activeTab).toBe(component.Tabs.gateways);
    expect(router.navigate).toHaveBeenCalledWith(['block/nvmeof/gateways'], {
      queryParamsHandling: 'preserve'
    });
  });

  it('should navigate to namespaces on selecting namespaces tab', () => {
    spyOn(router, 'navigate');
    component.onSelected(component.Tabs.namespaces);
    expect(component.activeTab).toBe(component.Tabs.namespaces);
    expect(router.navigate).toHaveBeenCalledWith(['block/nvmeof/namespaces'], {
      queryParamsHandling: 'preserve'
    });
  });

  it('should expose TABS enum via Tabs getter', () => {
    const tabs = component.Tabs;
    expect(tabs.gateways).toBe('gateways');
    expect(tabs.subsystems).toBe('subsystems');
    expect(tabs.namespaces).toBe('namespaces');
  });

  describe('setup cards scenarios', () => {
    it('should show setup cards', () => {
      component.ngOnInit();
      expect(component.showSetupCards).toBe(true);
    });

    it('should show setup cards when no group is selected in dropdown', () => {
      component.ngOnInit();
      nvmeofServiceSpy.listSubsystems.mockReturnValue(of(mockSubsystems));
      nvmeofServiceSpy.listNamespaces.mockReturnValue(of(mockNamespaces));
      setQueryParams({});
      expect(component.hasGatewayGroups).toBe(true);
      expect(component.hasSubsystems).toBe(false);
      expect(component.hasNamespaces).toBe(false);
      expect(component.isAllConfigured).toBe(false);
      expect(component.showSetupCards).toBe(true);
    });

    it('scenario: no gateway groups — all steps pending', () => {
      nvmeofServiceSpy.listGatewayGroups.mockReturnValue(of([[]]));
      component.ngOnInit();
      expect(component.hasGatewayGroups).toBe(false);
      expect(component.hasSubsystems).toBe(false);
      expect(component.hasNamespaces).toBe(false);
      expect(component.isAllConfigured).toBe(false);
      expect(component.showSetupCards).toBe(true);
    });

    it('scenario: selected gateway group exists, no subsystems — step 1 complete', () => {
      component.ngOnInit();
      nvmeofServiceSpy.listSubsystems.mockReturnValue(of([]));
      nvmeofServiceSpy.listNamespaces.mockReturnValue(of([]));
      setQueryParams({ group: 'grp1' });
      expect(component.hasGatewayGroups).toBe(true);
      expect(component.hasSubsystems).toBe(false);
      expect(component.hasNamespaces).toBe(false);
      expect(component.isAllConfigured).toBe(false);
    });

    it('scenario: selected gateway group does not exist — step 1 complete only', () => {
      component.ngOnInit();
      setQueryParams({ group: 'grp-other' });
      expect(component.hasGatewayGroups).toBe(true);
      expect(component.hasSubsystems).toBe(false);
      expect(component.hasNamespaces).toBe(false);
      expect(component.isAllConfigured).toBe(false);
    });

    it('scenario: selected gateway groups exists, no subsystems in object response — step 1 complete', () => {
      component.ngOnInit();
      nvmeofServiceSpy.listSubsystems.mockReturnValue(of({ subsystems: [] }));
      nvmeofServiceSpy.listNamespaces.mockReturnValue(of([]));
      setQueryParams({ group: 'grp1' });
      expect(component.hasGatewayGroups).toBe(true);
      expect(component.hasSubsystems).toBe(false);
      expect(component.hasNamespaces).toBe(false);
      expect(component.isAllConfigured).toBe(false);
    });

    it('scenario: selected gateway group + subsystems, no namespaces — steps 1 & 2 complete', () => {
      component.ngOnInit();
      nvmeofServiceSpy.listSubsystems.mockReturnValue(
        of([{ nqn: 'sub1', namespace_count: 0, initiator_count: 0 }])
      );
      nvmeofServiceSpy.listNamespaces.mockReturnValue(of([]));
      setQueryParams({ group: 'grp1' });
      expect(component.hasGatewayGroups).toBe(true);
      expect(component.hasSubsystems).toBe(true);
      expect(component.hasNamespaces).toBe(false);
      expect(component.isAllConfigured).toBe(false);
    });

    it('scenario: selected gateway group all configured — isAllConfigured is true', () => {
      component.ngOnInit();
      setQueryParams({ group: 'grp1' });
      expect(component.hasGatewayGroups).toBe(true);
      expect(component.hasSubsystems).toBe(true);
      expect(component.hasNamespaces).toBe(true);
      expect(component.isAllConfigured).toBe(true);
    });

    it('scenario: selected gateway group all configured in object response — isAllConfigured is true', () => {
      component.ngOnInit();
      nvmeofServiceSpy.listSubsystems.mockReturnValue(
        of({ subsystems: [{ nqn: 'sub1', namespace_count: 2, initiator_count: 0 }] })
      );
      nvmeofServiceSpy.listNamespaces.mockReturnValue(
        of({ namespaces: [{ nsid: 1, rbd_image_name: 'img1', rbd_pool_name: 'rbd' }] })
      );
      setQueryParams({ group: 'grp1' });
      expect(component.hasGatewayGroups).toBe(true);
      expect(component.hasSubsystems).toBe(true);
      expect(component.hasNamespaces).toBe(true);
      expect(component.isAllConfigured).toBe(true);
    });

    it('should hide setup cards on dismissOnboarding', () => {
      component.ngOnInit();
      component.loadSetupState();
      component.showSetupCards = true;
      component.dismissOnboarding();
      expect(component.showSetupCards).toBe(false);
    });

    it('should keep setup cards hidden after refresh when onboarding was dismissed', () => {
      component.ngOnInit();
      component.dismissOnboarding();

      emitRefresh();
      expect(component.showSetupCards).toBe(false);
    });

    it('should refresh setup cards when a relevant delete task finishes', () => {
      const loadSetupStateSpy = jest.spyOn(component, 'loadSetupState');

      component.ngOnInit();
      loadSetupStateSpy.mockClear();

      emitRefresh();

      expect(loadSetupStateSpy).toHaveBeenCalledTimes(1);
    });

    it('should refresh overview cards when a relevant task finishes', () => {
      const loadResourceStatsSpy = jest.spyOn(component, 'loadResourceStats');
      const loadThroughputSpy = jest.spyOn(component, 'loadThroughput');
      const loadAlertsSpy = jest.spyOn(component, 'loadAlerts');

      component.ngOnInit();
      loadResourceStatsSpy.mockClear();
      loadThroughputSpy.mockClear();
      loadAlertsSpy.mockClear();

      emitRefresh();

      expect(loadResourceStatsSpy).toHaveBeenCalledTimes(1);
      expect(loadThroughputSpy).toHaveBeenCalledTimes(1);
      expect(loadAlertsSpy).toHaveBeenCalledTimes(1);
    });

    it('should refresh setup cards when an NVMeoF gateway service delete task finishes', () => {
      const loadSetupStateSpy = jest.spyOn(component, 'loadSetupState');

      component.ngOnInit();
      loadSetupStateSpy.mockClear();

      emitRefresh();

      expect(loadSetupStateSpy).toHaveBeenCalledTimes(1);
    });

    it('should refresh setup cards when an NVMeoF gateway service create task finishes', () => {
      const loadSetupStateSpy = jest.spyOn(component, 'loadSetupState');

      component.ngOnInit();
      loadSetupStateSpy.mockClear();

      emitRefresh();

      expect(loadSetupStateSpy).toHaveBeenCalledTimes(1);
    });

    it('should refresh setup cards when a namespace create task finishes', () => {
      const loadSetupStateSpy = jest.spyOn(component, 'loadSetupState');

      component.ngOnInit();
      loadSetupStateSpy.mockClear();

      emitRefresh();

      expect(loadSetupStateSpy).toHaveBeenCalledTimes(1);
    });

    it('should call loadSetupState on NavigationEnd (e.g. navigating back from a form)', () => {
      const loadSetupStateSpy = jest.spyOn(component, 'loadSetupState');

      component.ngOnInit();
      loadSetupStateSpy.mockClear();

      routerEvents$.next(
        new NavigationEnd(1, '/block/nvmeof/namespaces', '/block/nvmeof/namespaces')
      );

      expect(loadSetupStateSpy).toHaveBeenCalledTimes(1);
    });

    it('should show the initial gateway-only state after the last subsystem and namespace are removed', () => {
      component.ngOnInit();

      nvmeofServiceSpy.listSubsystems.mockReturnValue(of([]));
      nvmeofServiceSpy.listNamespaces.mockReturnValue(of([]));

      emitRefresh();

      expect(component.hasGatewayGroups).toBe(true);
      expect(component.hasSubsystems).toBe(false);
      expect(component.hasNamespaces).toBe(false);
      expect(component.isAllConfigured).toBe(false);
    });

    it('should show the initial empty state after the last gateway is removed', () => {
      component.ngOnInit();

      nvmeofServiceSpy.listGatewayGroups.mockReturnValue(of([[]]));

      emitRefresh();

      expect(component.hasGatewayGroups).toBe(false);
      expect(component.hasSubsystems).toBe(false);
      expect(component.hasNamespaces).toBe(false);
      expect(component.isAllConfigured).toBe(false);
    });

    it('should show setup cards after everything is deleted even when onboarding was dismissed', () => {
      component.ngOnInit();
      component.dismissOnboarding();

      nvmeofServiceSpy.listGatewayGroups.mockReturnValue(of([[]]));
      emitRefresh();

      expect(component.hasGatewayGroups).toBe(false);
      expect(component.showSetupCards).toBe(true);
    });

    it('should refresh setup cards when the gateway list refreshes to empty', () => {
      component.ngOnInit();

      nvmeofServiceSpy.listGatewayGroups.mockReturnValue(of([[]]));
      emitRefresh();

      expect(component.hasGatewayGroups).toBe(false);
      expect(component.hasSubsystems).toBe(false);
      expect(component.hasNamespaces).toBe(false);
      expect(component.isAllConfigured).toBe(false);
    });

    it('should show gateway step complete after a gateway is created', () => {
      nvmeofServiceSpy.listGatewayGroups.mockReturnValueOnce(of([[]]));
      component.ngOnInit();

      nvmeofServiceSpy.listGatewayGroups.mockReturnValue(of(mockGroups));
      nvmeofServiceSpy.listSubsystems.mockReturnValue(of([]));
      nvmeofServiceSpy.listNamespaces.mockReturnValue(of([]));

      emitRefresh();

      expect(component.hasGatewayGroups).toBe(true);
      expect(component.hasSubsystems).toBe(false);
      expect(component.hasNamespaces).toBe(false);
      expect(component.isAllConfigured).toBe(false);
    });
  });

  describe('loadResourceStats', () => {
    it('should load stats when gateway groups response is indexable object with numeric keys', fakeAsync(() => {
      const indexedResponse = {
        0: [makeGroup('default', 1, 1)],
        1: 1
      };

      jest
        .spyOn(nvmeofService, 'listGatewayGroups')
        .mockReturnValue(of((indexedResponse as unknown) as CephServiceSpec[][]));

      component.loadResourceStats();
      let stats: any;
      component.nvmeof$.subscribe((s) => (stats = s));
      tick();

      expect(stats.gatewayGroups).toBe(1);
      expect(stats.hasData).toBe(true);
    }));

    it('should load stats when gateway groups response is a flat array', fakeAsync(() => {
      jest
        .spyOn(nvmeofService, 'listGatewayGroups')
        .mockReturnValue(of(([makeGroup('default', 1, 1)] as unknown) as CephServiceSpec[][]));

      component.loadResourceStats();
      let stats: any;
      component.nvmeof$.subscribe((s) => (stats = s));
      tick();

      expect(stats.gatewayGroups).toBe(1);
      expect(stats.hasData).toBe(true);
    }));

    it('should return null when no gateway groups exist', fakeAsync(() => {
      jest.spyOn(performanceCardService, 'getNvmeofResourceStats').mockReturnValue(
        of({
          gatewayGroups: 0,
          subsystems: 0,
          namespaces: 0,
          hosts: 0,
          activeConnections: 0
        })
      );
      jest.spyOn(nvmeofService, 'listGatewayGroups').mockReturnValue(of([[]]));

      component.loadResourceStats();
      let stats: any;
      component.nvmeof$.subscribe((s) => (stats = s));
      tick();

      expect(stats).toBeNull();
    }));
  });

  describe('loadThroughput', () => {
    it('should load throughput from PerformanceCardService', fakeAsync(() => {
      jest
        .spyOn(performanceCardService, 'getNvmeofThroughput')
        .mockReturnValue(of({ reads: 12.5, writes: 7.25, combined: 19.75 }));

      component.loadThroughput();
      let throughput: any;
      component.nvmeofThroughput$.subscribe((t) => (throughput = t));
      tick();

      expect(performanceCardService.getNvmeofThroughput).toHaveBeenCalled();
      expect(throughput).toEqual({ reads: 12.5, writes: 7.25, combined: 19.75 });
    }));
  });

  describe('loadAlerts', () => {
    it('should return zero counts when there are no alert groups', fakeAsync(() => {
      jest.spyOn(prometheusAlertService, 'fetchGroupedAlerts').mockReturnValue(of([]));

      component.loadAlerts();
      let alerts: any;
      component.nvmeofAlerts$.subscribe((a) => (alerts = a));
      tick(0);
      discardPeriodicTasks();

      expect(alerts.total).toBe(0);
      expect(alerts.critical).toBe(0);
      expect(alerts.warning).toBe(0);
    }));

    it('should count active nvmeof critical and warning alerts', fakeAsync(() => {
      const mockGroups = [
        {
          alerts: [
            {
              labels: {
                alertname: 'NVMeoFHighGatewayCPU',
                category: 'gateway',
                severity: 'critical'
              },
              status: { state: 'active' }
            },
            {
              labels: {
                alertname: 'NVMeoFInterfaceDuplex',
                category: 'listener',
                severity: 'warning'
              },
              status: { state: 'active' }
            },
            {
              labels: {
                alertname: 'NVMeoFMissingListener',
                category: 'listener',
                severity: 'warning'
              },
              status: { state: 'suppressed' }
            },
            {
              labels: { alertname: 'CephDaemonCrash', severity: 'critical' },
              status: { state: 'active' }
            }
          ]
        }
      ];

      jest.spyOn(prometheusAlertService, 'fetchGroupedAlerts').mockReturnValue(of(mockGroups as any));

      component.loadAlerts();
      let alerts: any;
      component.nvmeofAlerts$.subscribe((a) => (alerts = a));
      tick(0);
      discardPeriodicTasks();

      expect(alerts.critical).toBe(1);
      expect(alerts.warning).toBe(1);
      expect(alerts.total).toBe(2);
      expect(alerts.byCategory).toEqual({ gateway: 1, listener: 1 });
    }));

    it('should match nvmeof alerts by prometheus job label', fakeAsync(() => {
      const mockGroups = [
        {
          alerts: [
            {
              labels: { job: 'nvmeof', severity: 'warning', alertname: 'SomeAlert' },
              status: { state: 'active' }
            }
          ]
        }
      ];

      jest.spyOn(prometheusAlertService, 'fetchGroupedAlerts').mockReturnValue(of(mockGroups as any));

      component.loadAlerts();
      let alerts: any;
      component.nvmeofAlerts$.subscribe((a) => (alerts = a));
      tick(0);
      discardPeriodicTasks();

      expect(alerts.warning).toBe(1);
      expect(alerts.total).toBe(1);
    }));

    it('should not match alerts by alertname when nvme labels are absent', fakeAsync(() => {
      const mockGroups = [
        {
          alerts: [
            {
              labels: { alertname: 'NVMeofInterfaceDuplex', severity: 'warning' },
              status: { state: 'active' }
            }
          ]
        }
      ];

      jest.spyOn(prometheusAlertService, 'fetchGroupedAlerts').mockReturnValue(of(mockGroups as any));

      component.loadAlerts();
      let alerts: any;
      component.nvmeofAlerts$.subscribe((a) => (alerts = a));
      tick(0);
      discardPeriodicTasks();

      expect(alerts.warning).toBe(0);
      expect(alerts.total).toBe(0);
    }));

    it('should not count non-nvme alerts that only match nvme categories', fakeAsync(() => {
      const mockGroups = [
        {
          alerts: [
            {
              labels: {
                alertname: 'SomeGatewayAlert',
                category: 'gateway',
                severity: 'critical'
              },
              status: { state: 'active' }
            },
            {
              labels: {
                alertname: 'NVMeoFInterfaceDuplex',
                category: 'listener',
                severity: 'warning'
              },
              status: { state: 'active' }
            }
          ]
        }
      ];

      jest.spyOn(prometheusAlertService, 'fetchGroupedAlerts').mockReturnValue(of(mockGroups as any));

      component.loadAlerts();
      let alerts: any;
      component.nvmeofAlerts$.subscribe((a) => (alerts = a));
      tick(0);
      discardPeriodicTasks();

      expect(alerts.critical).toBe(0);
      expect(alerts.warning).toBe(1);
      expect(alerts.total).toBe(1);
      expect(alerts.byCategory).toEqual({ listener: 1 });
    }));

    it('should not count inactive nvmeof alerts', fakeAsync(() => {
      const mockGroups = [
        {
          alerts: [
            {
              labels: {
                alertname: 'NVMeoFHighGatewayCPU',
                category: 'gateway',
                severity: 'critical'
              },
              status: { state: 'inactive' }
            },
            {
              labels: {
                alertname: 'NVMeoFInterfaceDuplex',
                category: 'listener',
                severity: 'warning'
              },
              status: { state: 'pending' }
            }
          ]
        }
      ];

      jest.spyOn(prometheusAlertService, 'fetchGroupedAlerts').mockReturnValue(of(mockGroups as any));

      component.loadAlerts();
      let alerts: any;
      component.nvmeofAlerts$.subscribe((a) => (alerts = a));
      tick(0);
      discardPeriodicTasks();

      expect(alerts.critical).toBe(0);
      expect(alerts.warning).toBe(0);
      expect(alerts.total).toBe(0);
    }));
  });
});
