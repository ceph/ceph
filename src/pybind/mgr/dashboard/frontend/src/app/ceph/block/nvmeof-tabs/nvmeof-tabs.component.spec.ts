import {
  ComponentFixture,
  TestBed,
  discardPeriodicTasks,
  fakeAsync,
  tick
} from '@angular/core/testing';
import { Router } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';
import { of } from 'rxjs';

import { TabsModule } from 'carbon-components-angular';

import { NvmeofTabsComponent } from './nvmeof-tabs.component';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { PerformanceCardService } from '~/app/shared/api/performance-card.service';
import { PrometheusService } from '~/app/shared/api/prometheus.service';
import { CephServiceSpec } from '~/app/shared/models/service.interface';
import { SharedModule } from '~/app/shared/shared.module';

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

const mockSubsystems = [{ namespace_count: 2, initiator_count: 1 }];
const mockNamespaces: any[] = [];

describe('NvmeofTabsComponent', () => {
  let component: NvmeofTabsComponent;
  let fixture: ComponentFixture<NvmeofTabsComponent>;
  let router: Router;
  let nvmeofService: NvmeofService;
  let performanceCardService: PerformanceCardService;
  let prometheusService: PrometheusService;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [NvmeofTabsComponent],
      imports: [RouterTestingModule, SharedModule, TabsModule],
      providers: [
        {
          provide: NvmeofService,
          useValue: {
            listGatewayGroups: jest.fn().mockReturnValue(of([[]])),
            listSubsystems: jest.fn().mockReturnValue(of(mockSubsystems)),
            listNamespaces: jest.fn().mockReturnValue(of(mockNamespaces))
          }
        },
        {
          provide: PerformanceCardService,
          useValue: {
            getNvmeofThroughput: jest.fn().mockReturnValue(of({ reads: 0, writes: 0 }))
          }
        },
        {
          provide: PrometheusService,
          useValue: {
            isAlertmanagerUsable: jest.fn().mockReturnValue(of(false)),
            getAlerts: jest.fn().mockReturnValue(of([]))
          }
        }
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(NvmeofTabsComponent);
    component = fixture.componentInstance;
    router = TestBed.inject(Router);
    nvmeofService = TestBed.inject(NvmeofService);
    performanceCardService = TestBed.inject(PerformanceCardService);
    prometheusService = TestBed.inject(PrometheusService);
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

  it('should navigate to correct path on tab selection', () => {
    spyOn(router, 'navigate');
    component.onSelected(component.Tabs.subsystems);
    expect(component.selectedTab).toBe(component.Tabs.subsystems);
    expect(router.navigate).toHaveBeenCalledWith(['block/nvmeof/subsystems']);
  });

  it('should navigate to gateways on selecting gateways tab', () => {
    spyOn(router, 'navigate');
    component.onSelected(component.Tabs.gateways);
    expect(component.selectedTab).toBe(component.Tabs.gateways);
    expect(router.navigate).toHaveBeenCalledWith(['block/nvmeof/gateways']);
  });

  it('should navigate to namespaces on selecting namespaces tab', () => {
    spyOn(router, 'navigate');
    component.onSelected(component.Tabs.namespaces);
    expect(component.selectedTab).toBe(component.Tabs.namespaces);
    expect(router.navigate).toHaveBeenCalledWith(['block/nvmeof/namespaces']);
  });

  it('should expose TABS enum via Tabs getter', () => {
    const tabs = component.Tabs;
    expect(tabs.gateways).toBe('gateways');
    expect(tabs.subsystems).toBe('subsystems');
    expect(tabs.namespaces).toBe('namespaces');
  });

  describe('loadResourceStats – gatewayGroupsDown', () => {
    it('should load stats when gateway groups response is indexable object with numeric keys', fakeAsync(() => {
      const indexedResponse = {
        0: [makeGroup('default', 1, 1)],
        1: 1
      };

      jest
        .spyOn(nvmeofService, 'listGatewayGroups')
        .mockReturnValue(of(indexedResponse as unknown as CephServiceSpec[][]));

      component.loadResourceStats();
      let stats: any;
      component.nvmeof$.subscribe((s) => (stats = s));
      tick();

      expect(stats.gatewayGroups).toBe(1);
      expect(stats.gatewayGroupsDown).toBe(0);
      expect(stats.hasData).toBe(true);
    }));

    it('should load stats when gateway groups response is a flat array', fakeAsync(() => {
      jest
        .spyOn(nvmeofService, 'listGatewayGroups')
        .mockReturnValue(of([makeGroup('default', 1, 1)] as unknown as CephServiceSpec[][]));

      component.loadResourceStats();
      let stats: any;
      component.nvmeof$.subscribe((s) => (stats = s));
      tick();

      expect(stats.gatewayGroups).toBe(1);
      expect(stats.gatewayGroupsDown).toBe(0);
      expect(stats.hasData).toBe(true);
    }));

    it('should set gatewayGroupsDown to 0 when all gateways are running', fakeAsync(() => {
      jest
        .spyOn(nvmeofService, 'listGatewayGroups')
        .mockReturnValue(of([[makeGroup('default', 1, 1), makeGroup('default1', 2, 2)]]));

      component.loadResourceStats();
      let stats: any;
      component.nvmeof$.subscribe((s) => (stats = s));
      tick();

      expect(stats.gatewayGroups).toBe(2);
      expect(stats.gatewayGroupsDown).toBe(0);
    }));

    it('should count groups with at least one down gateway in gatewayGroupsDown', fakeAsync(() => {
      jest
        .spyOn(nvmeofService, 'listGatewayGroups')
        .mockReturnValue(of([[makeGroup('default', 0, 1), makeGroup('default1', 1, 2)]]));

      component.loadResourceStats();
      let stats: any;
      component.nvmeof$.subscribe((s) => (stats = s));
      tick();

      expect(stats.gatewayGroups).toBe(2);
      expect(stats.gatewayGroupsDown).toBe(2);
    }));

    it('should count only the groups that have errors', fakeAsync(() => {
      jest
        .spyOn(nvmeofService, 'listGatewayGroups')
        .mockReturnValue(of([[makeGroup('default', 1, 1), makeGroup('default1', 0, 1)]]));

      component.loadResourceStats();
      let stats: any;
      component.nvmeof$.subscribe((s) => (stats = s));
      tick();

      expect(stats.gatewayGroups).toBe(2);
      expect(stats.gatewayGroupsDown).toBe(1);
    }));

    it('should return null when no gateway groups exist', fakeAsync(() => {
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
        .mockReturnValue(of({ reads: 12.5, writes: 7.25 }));

      component.loadThroughput();
      let throughput: any;
      component.nvmeofThroughput$.subscribe((t) => (throughput = t));
      tick();

      expect(performanceCardService.getNvmeofThroughput).toHaveBeenCalled();
      expect(throughput).toEqual({ reads: 12.5, writes: 7.25 });
    }));
  });

  describe('loadAlerts', () => {
    it('should return zero counts when alertmanager is not usable', fakeAsync(() => {
      jest.spyOn(prometheusService, 'isAlertmanagerUsable').mockReturnValue(of(false));

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
      const mockAlerts = [
        {
          labels: { alertname: 'NVMeoFHighGatewayCPU', category: 'gateway', severity: 'critical' },
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
          labels: { alertname: 'NVMeoFMissingListener', category: 'listener', severity: 'warning' },
          status: { state: 'suppressed' }
        },
        {
          labels: { alertname: 'CephDaemonCrash', severity: 'critical' },
          status: { state: 'active' }
        }
      ];

      jest.spyOn(prometheusService, 'isAlertmanagerUsable').mockReturnValue(of(true));
      jest.spyOn(prometheusService, 'getAlerts').mockReturnValue(of(mockAlerts as any));

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
      const mockAlerts = [
        {
          labels: { job: 'nvmeof', severity: 'warning', alertname: 'SomeAlert' },
          status: { state: 'active' }
        }
      ];

      jest.spyOn(prometheusService, 'isAlertmanagerUsable').mockReturnValue(of(true));
      jest.spyOn(prometheusService, 'getAlerts').mockReturnValue(of(mockAlerts as any));

      component.loadAlerts();
      let alerts: any;
      component.nvmeofAlerts$.subscribe((a) => (alerts = a));
      tick(0);
      discardPeriodicTasks();

      expect(alerts.warning).toBe(1);
      expect(alerts.total).toBe(1);
    }));

    it('should match nvmeof alerts by alertname when category label is absent', fakeAsync(() => {
      const mockAlerts = [
        {
          labels: { alertname: 'NVMeofInterfaceDuplex', severity: 'warning' },
          status: { state: 'active' }
        }
      ];

      jest.spyOn(prometheusService, 'isAlertmanagerUsable').mockReturnValue(of(true));
      jest.spyOn(prometheusService, 'getAlerts').mockReturnValue(of(mockAlerts as any));

      component.loadAlerts();
      let alerts: any;
      component.nvmeofAlerts$.subscribe((a) => (alerts = a));
      tick(0);
      discardPeriodicTasks();

      expect(alerts.warning).toBe(1);
      expect(alerts.total).toBe(1);
    }));

    it('should not count inactive nvmeof alerts', fakeAsync(() => {
      const mockAlerts = [
        {
          labels: { alertname: 'NVMeoFHighGatewayCPU', category: 'gateway', severity: 'critical' },
          status: { state: 'inactive' }
        },
        {
          labels: { alertname: 'NVMeoFInterfaceDuplex', category: 'listener', severity: 'warning' },
          status: { state: 'pending' }
        }
      ];

      jest.spyOn(prometheusService, 'isAlertmanagerUsable').mockReturnValue(of(true));
      jest.spyOn(prometheusService, 'getAlerts').mockReturnValue(of(mockAlerts as any));

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
