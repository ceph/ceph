import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute, Event as RouterEvent, NavigationEnd, Router } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';
import { BehaviorSubject, Subject, of } from 'rxjs';

import { TabsModule } from 'carbon-components-angular';

import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { NvmeofStateService } from '../nvmeof-state.service';
import { NvmeofTabsComponent } from './nvmeof-tabs.component';
import { SharedModule } from '~/app/shared/shared.module';
import { NvmeofSetupCardsComponent } from '../nvmeof-setup-cards/nvmeof-setup-cards.component';

describe('NvmeofTabsComponent', () => {
  let component: NvmeofTabsComponent;
  let fixture: ComponentFixture<NvmeofTabsComponent>;
  let router: Router;
  let nvmeofServiceSpy: any;
  let queryParams$: BehaviorSubject<any>;
  let refresh$: Subject<void>;
  let routerEvents$: Subject<RouterEvent>;

  const mockGroups = [
    [{ spec: { group: 'grp1' }, placement: { hosts: ['h1'] }, status: { running: 1, size: 1 } }]
  ];
  const mockMultipleGroups = [
    [
      { spec: { group: 'grp1' }, placement: { hosts: ['h1'] }, status: { running: 1, size: 1 } },
      { spec: { group: 'grp2' }, placement: { hosts: ['h2'] }, status: { running: 1, size: 1 } }
    ]
  ];
  const mockSubsystems = [{ nqn: 'sub1', namespace_count: 2, initiator_count: 0 }];
  const mockNamespaces = [{ nsid: 1, rbd_image_name: 'img1', rbd_pool_name: 'rbd' }];

  const onboardingDismissedKey = () => 'nvmeof_onboarding_dismissed';

  const setQueryParams = (params: any) => queryParams$.next(params);
  const emitRefresh = () => refresh$.next();

  beforeEach(async () => {
    localStorage.removeItem(onboardingDismissedKey());
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
        { provide: ActivatedRoute, useValue: { queryParams: queryParams$.asObservable() } },
        { provide: NvmeofStateService, useValue: { refresh$: refresh$.asObservable() } }
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(NvmeofTabsComponent);
    component = fixture.componentInstance;
    router = TestBed.inject(Router);
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

    it('should evaluate setup cards across gateway groups when no group is selected', () => {
      component.ngOnInit();
      nvmeofServiceSpy.listSubsystems.mockReturnValue(of(mockSubsystems));
      nvmeofServiceSpy.listNamespaces.mockReturnValue(of(mockNamespaces));
      setQueryParams({});
      expect(component.hasGatewayGroups).toBe(true);
      expect(component.hasSubsystems).toBe(true);
      expect(component.hasNamespaces).toBe(true);
      expect(component.isAllConfigured).toBe(true);
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

    it('scenario: gateway groups exist, no subsystems across all groups — step 1 complete', () => {
      component.ngOnInit();
      nvmeofServiceSpy.listSubsystems.mockReturnValue(of([]));
      nvmeofServiceSpy.listNamespaces.mockReturnValue(of([]));
      setQueryParams({ group: 'grp1' });
      expect(component.hasGatewayGroups).toBe(true);
      expect(component.hasSubsystems).toBe(false);
      expect(component.hasNamespaces).toBe(false);
      expect(component.isAllConfigured).toBe(false);
    });

    it('scenario: selected gateway group does not exist — setup still reflects all groups', () => {
      component.ngOnInit();
      setQueryParams({ group: 'grp-other' });
      expect(component.hasGatewayGroups).toBe(true);
      expect(component.hasSubsystems).toBe(true);
      expect(component.hasNamespaces).toBe(true);
      expect(component.isAllConfigured).toBe(true);
    });

    it('scenario: no subsystems in object response across all groups — step 1 complete', () => {
      component.ngOnInit();
      nvmeofServiceSpy.listSubsystems.mockReturnValue(of({ subsystems: [] }));
      nvmeofServiceSpy.listNamespaces.mockReturnValue(of([]));
      setQueryParams({ group: 'grp1' });
      expect(component.hasGatewayGroups).toBe(true);
      expect(component.hasSubsystems).toBe(false);
      expect(component.hasNamespaces).toBe(false);
      expect(component.isAllConfigured).toBe(false);
    });

    it('scenario: subsystems in any group, no namespaces — steps 1 & 2 complete', () => {
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

    it('scenario: all configured across any group — isAllConfigured is true', () => {
      component.ngOnInit();
      setQueryParams({ group: 'grp1' });
      expect(component.hasGatewayGroups).toBe(true);
      expect(component.hasSubsystems).toBe(true);
      expect(component.hasNamespaces).toBe(true);
      expect(component.isAllConfigured).toBe(true);
    });

    it('scenario: all configured in object response across any group — isAllConfigured is true', () => {
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

    it('scenario: subsystems exist in grp2 only — step 2 still marked complete', () => {
      nvmeofServiceSpy.listGatewayGroups.mockReturnValue(of(mockMultipleGroups));
      nvmeofServiceSpy.listSubsystems.mockImplementation((group: string) => {
        if (group === 'grp2') return of([{ nqn: 'sub1', namespace_count: 0, initiator_count: 0 }]);
        return of([]);
      });
      nvmeofServiceSpy.listNamespaces.mockReturnValue(of([]));
      component.ngOnInit();
      setQueryParams({ group: 'grp1' });
      expect(component.hasGatewayGroups).toBe(true);
      expect(component.hasSubsystems).toBe(true);
      expect(component.hasNamespaces).toBe(false);
      expect(component.isAllConfigured).toBe(false);
    });

    it('scenario: full config in grp2 only — onboarding complete regardless of selected group', () => {
      nvmeofServiceSpy.listGatewayGroups.mockReturnValue(of(mockMultipleGroups));
      nvmeofServiceSpy.listSubsystems.mockImplementation((group: string) => {
        if (group === 'grp2') return of([{ nqn: 'sub1', namespace_count: 1, initiator_count: 0 }]);
        return of([]);
      });
      nvmeofServiceSpy.listNamespaces.mockImplementation((group: string) => {
        if (group === 'grp2') {
          return of([{ nsid: 1, rbd_image_name: 'img1', rbd_pool_name: 'rbd' }]);
        }
        return of([]);
      });
      component.ngOnInit();
      setQueryParams({ group: 'grp1' });
      expect(component.hasGatewayGroups).toBe(true);
      expect(component.hasSubsystems).toBe(true);
      expect(component.hasNamespaces).toBe(true);
      expect(component.isAllConfigured).toBe(true);
    });

    it('should hide setup cards on dismissOnboarding', () => {
      component.ngOnInit();
      emitRefresh();
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

    it('should persist onboarding dismiss in cluster-scoped localStorage', () => {
      component.ngOnInit();
      component.dismissOnboarding();

      expect(localStorage.getItem(onboardingDismissedKey())).toBe('true');
    });

    it('should clear cluster-scoped localStorage when setup becomes incomplete after dismiss', () => {
      component.ngOnInit();
      component.dismissOnboarding();
      expect(localStorage.getItem(onboardingDismissedKey())).toBe('true');

      nvmeofServiceSpy.listSubsystems.mockReturnValue(of([]));
      nvmeofServiceSpy.listNamespaces.mockReturnValue(of([]));
      emitRefresh();

      expect(localStorage.getItem(onboardingDismissedKey())).toBeNull();
      expect(component.showSetupCards).toBe(true);
    });

    it('should show setup cards after reload when setup became incomplete after dismiss', () => {
      component.ngOnInit();
      component.dismissOnboarding();

      nvmeofServiceSpy.listSubsystems.mockReturnValue(of([]));
      nvmeofServiceSpy.listNamespaces.mockReturnValue(of([]));
      emitRefresh();

      fixture = TestBed.createComponent(NvmeofTabsComponent);
      component = fixture.componentInstance;
      component.ngOnInit();

      expect(localStorage.getItem(onboardingDismissedKey())).toBeNull();
      expect(component.showSetupCards).toBe(true);
    });

    it('should trigger state reload when refresh$ emits', () => {
      component.ngOnInit();

      nvmeofServiceSpy.listGatewayGroups.mockReturnValue(of([]));
      emitRefresh();

      expect(component.hasGatewayGroups).toBe(false);
      expect(component.hasSubsystems).toBe(false);
      expect(component.hasNamespaces).toBe(false);
    });

    it('should reload state on NavigationEnd (e.g. navigating back from a form)', () => {
      component.ngOnInit();

      nvmeofServiceSpy.listGatewayGroups.mockReturnValue(of([]));

      routerEvents$.next(
        new NavigationEnd(1, '/block/nvmeof/namespaces', '/block/nvmeof/namespaces')
      );

      expect(component.hasGatewayGroups).toBe(false);
      expect(component.hasSubsystems).toBe(false);
      expect(component.hasNamespaces).toBe(false);
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

    it('should not query subsystems or namespaces when gateway list is empty on refresh', () => {
      nvmeofServiceSpy.listGatewayGroups.mockReturnValue(of([[]]));
      component.ngOnInit();
      nvmeofServiceSpy.listSubsystems.mockClear();
      nvmeofServiceSpy.listNamespaces.mockClear();

      emitRefresh();

      expect(nvmeofServiceSpy.listSubsystems).not.toHaveBeenCalled();
      expect(nvmeofServiceSpy.listNamespaces).not.toHaveBeenCalled();
    });

    it('should re-show setup cards after last gateway removed when onboarding was dismissed', () => {
      component.ngOnInit();
      component.dismissOnboarding();
      expect(component.showSetupCards).toBe(false);

      nvmeofServiceSpy.listGatewayGroups.mockReturnValue(of([[]]));
      emitRefresh();

      expect(component.showSetupCards).toBe(true);
      expect(localStorage.getItem(onboardingDismissedKey())).toBeNull();
    });

    it('should render correct setup card messages after all gateway groups are removed', () => {
      jest.spyOn(router, 'url', 'get').mockReturnValue('/block/nvmeof/gateways');
      component.ngOnInit();
      emitRefresh();

      nvmeofServiceSpy.listGatewayGroups.mockReturnValue(of([[]]));
      emitRefresh();
      fixture.detectChanges();

      const cardElements = fixture.debugElement.queryAll((el) => el.name === 'cd-setup-step-card');

      expect(cardElements.length).toBe(3);
      expect(cardElements[0].componentInstance.statusMessage).toBe(
        'No gateway groups configured for this cluster yet.'
      );
      expect(cardElements[1].componentInstance.statusMessage).toBe('No gateway configured yet.');
      expect(cardElements[2].componentInstance.statusMessage).toBe('No gateway configured yet.');
    });

    it('should render success setup card messages before gateway groups are removed', () => {
      jest.spyOn(router, 'url', 'get').mockReturnValue('/block/nvmeof/gateways');
      component.ngOnInit();
      emitRefresh();
      fixture.detectChanges();

      const cardElements = fixture.debugElement.queryAll((el) => el.name === 'cd-setup-step-card');

      expect(cardElements[0].componentInstance.statusMessage).toBe(
        'Gateway group configured successfully.'
      );
      expect(cardElements[1].componentInstance.statusMessage).toBe(
        'Subsystem configured successfully.'
      );
      expect(cardElements[2].componentInstance.statusMessage).toBe(
        'Namespaces mapped successfully.'
      );
    });
  });
});
