import { ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ActivatedRoute, Event as RouterEvent, NavigationEnd, Router } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';
import { BehaviorSubject, Subject, of } from 'rxjs';

import { TabsModule } from 'carbon-components-angular';

import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { NvmeofStateService } from '../nvmeof-state.service';
import { NvmeofTabsComponent } from './nvmeof-tabs.component';
import { SharedModule } from '~/app/shared/shared.module';
import { NvmeofSetupCardsComponent } from '../nvmeof-setup-cards/nvmeof-setup-cards.component';

type SetupState = {
  hasGatewayGroups: boolean;
  hasSubsystems: boolean;
  hasNamespaces: boolean;
};

describe('NvmeofTabsComponent', () => {
  let component: NvmeofTabsComponent;
  let fixture: ComponentFixture<NvmeofTabsComponent>;
  let router: Router;
  let nvmeofServiceSpy: any;
  let queryParams$: BehaviorSubject<any>;
  let refresh$: Subject<void>;
  let routerEvents$: Subject<RouterEvent>;
  let currentSetupState: SetupState;
  let routerUrl = '/block/nvmeof/gateways';

  const setQueryParams = (params: any) => queryParams$.next(params);
  const emitRefresh = () => refresh$.next();
  const setRouterUrl = (url: string) => {
    routerUrl = url;
  };
  const setSetupState = (state: SetupState) => {
    currentSetupState = state;
    nvmeofServiceSpy.fetchSetupState.mockReturnValue(of(currentSetupState));
  };

  beforeEach(async () => {
    queryParams$ = new BehaviorSubject<any>({ group: 'grp1' });
    refresh$ = new Subject<void>();
    routerUrl = '/block/nvmeof/gateways';
    currentSetupState = { hasGatewayGroups: true, hasSubsystems: true, hasNamespaces: true };
    nvmeofServiceSpy = {
      fetchSetupState: jest.fn().mockImplementation(() => of(currentSetupState))
    };

    TestBed.configureTestingModule({
      declarations: [NvmeofTabsComponent],
      imports: [
        HttpClientTestingModule,
        RouterTestingModule,
        SharedModule,
        TabsModule,
        NvmeofSetupCardsComponent
      ],
      providers: [
        { provide: NvmeofService, useValue: nvmeofServiceSpy },
        { provide: ActivatedRoute, useValue: { queryParams: queryParams$.asObservable() } }
      ]
    })
      .overrideComponent(NvmeofTabsComponent, {
        set: {
          providers: [
            {
              provide: NvmeofStateService,
              useValue: {
                refresh$: refresh$.asObservable(),
                requestRefresh: jest.fn()
              }
            }
          ]
        }
      })
      .compileComponents();

    fixture = TestBed.createComponent(NvmeofTabsComponent);
    component = fixture.componentInstance;
    router = TestBed.inject(Router);
    routerEvents$ = new Subject<RouterEvent>();
    Object.defineProperty(router, 'events', {
      configurable: true,
      get: () => routerEvents$.asObservable()
    });
    Object.defineProperty(router, 'url', {
      configurable: true,
      get: () => routerUrl
    });
    Object.defineProperty(router, 'navigate', {
      configurable: true,
      value: jest.fn().mockResolvedValue(true)
    });
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should default activeTab to gateways', () => {
    setRouterUrl('/block/nvmeof/gateways');
    component.ngOnInit();
    expect(component.activeTab).toBe(component.Tabs.gateways);
  });

  it('should set activeTab to subsystems when URL contains subsystems', () => {
    setRouterUrl('/block/nvmeof/subsystems');
    component.ngOnInit();
    expect(component.activeTab).toBe(component.Tabs.subsystems);
  });

  it('should set activeTab to namespaces when URL contains namespaces', () => {
    setRouterUrl('/block/nvmeof/namespaces');
    component.ngOnInit();
    expect(component.activeTab).toBe(component.Tabs.namespaces);
  });

  it('should fallback to gateways when URL does not match any tab', () => {
    setRouterUrl('/block/nvmeof/unknown');
    component.ngOnInit();
    expect(component.activeTab).toBe(component.Tabs.gateways);
  });

  it('should hide the shell on namespace create routes', () => {
    setRouterUrl('/block/nvmeof/namespaces/create');
    component.ngOnInit();
    expect(component.showTabsShell).toBe(false);
  });

  it('should keep the shell visible on namespace list routes', () => {
    setRouterUrl('/block/nvmeof/namespaces');
    component.ngOnInit();
    expect(component.showTabsShell).toBe(true);
  });

  it('should keep the shell visible on list routes with a secondary outlet', () => {
    setRouterUrl('/block/nvmeof/subsystems(modal:create)?group=default');
    component.ngOnInit();
    expect(component.showTabsShell).toBe(true);
  });

  it('should hide the shell when primary route is a create page with secondary outlet', () => {
    setRouterUrl('/block/nvmeof/subsystems/create(modal:create)?group=default');
    component.ngOnInit();
    expect(component.showTabsShell).toBe(false);
  });

  it('should navigate to correct path on tab selection', () => {
    component.onSelected(component.Tabs.subsystems);
    expect(component.activeTab).toBe(component.Tabs.subsystems);
    expect(router.navigate).toHaveBeenCalledWith(['block/nvmeof', 'subsystems'], {
      queryParamsHandling: 'preserve'
    });
  });

  it('should navigate to gateways on selecting gateways tab', () => {
    component.onSelected(component.Tabs.gateways);
    expect(component.activeTab).toBe(component.Tabs.gateways);
    expect(router.navigate).toHaveBeenCalledWith(['block/nvmeof', 'gateways'], {
      queryParamsHandling: 'preserve'
    });
  });

  it('should navigate to namespaces on selecting namespaces tab', () => {
    component.onSelected(component.Tabs.namespaces);
    expect(component.activeTab).toBe(component.Tabs.namespaces);
    expect(router.navigate).toHaveBeenCalledWith(['block/nvmeof', 'namespaces'], {
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
      setSetupState({ hasGatewayGroups: false, hasSubsystems: false, hasNamespaces: false });
      component.ngOnInit();
      expect(component.showSetupCards).toBe(true);
    });

    it('should detect subsystems and namespaces regardless of dropdown selection', () => {
      setSetupState({ hasGatewayGroups: true, hasSubsystems: true, hasNamespaces: true });
      component.ngOnInit();
      setQueryParams({});
      expect(component.hasGatewayGroups).toBe(true);
      expect(component.hasSubsystems).toBe(true);
      expect(component.hasNamespaces).toBe(true);
      expect(component.isAllConfigured).toBe(true);
      expect(component.showSetupCards).toBe(true);
    });

    it('scenario: no gateway groups — all steps pending', () => {
      setSetupState({ hasGatewayGroups: false, hasSubsystems: false, hasNamespaces: false });
      component.ngOnInit();
      expect(component.hasGatewayGroups).toBe(false);
      expect(component.hasSubsystems).toBe(false);
      expect(component.hasNamespaces).toBe(false);
      expect(component.isAllConfigured).toBe(false);
      expect(component.showSetupCards).toBe(true);
    });

    it('scenario: gateway groups exist, no subsystems across all groups — step 1 complete', () => {
      setSetupState({ hasGatewayGroups: true, hasSubsystems: false, hasNamespaces: false });
      component.ngOnInit();
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
      setSetupState({ hasGatewayGroups: true, hasSubsystems: false, hasNamespaces: false });
      component.ngOnInit();
      setQueryParams({ group: 'grp1' });
      expect(component.hasGatewayGroups).toBe(true);
      expect(component.hasSubsystems).toBe(false);
      expect(component.hasNamespaces).toBe(false);
      expect(component.isAllConfigured).toBe(false);
    });

    it('scenario: subsystems in any group, no namespaces — steps 1 & 2 complete', () => {
      setSetupState({ hasGatewayGroups: true, hasSubsystems: true, hasNamespaces: false });
      component.ngOnInit();
      setQueryParams({ group: 'grp1' });
      expect(component.hasGatewayGroups).toBe(true);
      expect(component.hasSubsystems).toBe(true);
      expect(component.hasNamespaces).toBe(false);
      expect(component.isAllConfigured).toBe(false);
    });

    it('scenario: all configured across any group — isAllConfigured is true', () => {
      setSetupState({ hasGatewayGroups: true, hasSubsystems: true, hasNamespaces: true });
      component.ngOnInit();
      setQueryParams({ group: 'grp1' });
      expect(component.hasGatewayGroups).toBe(true);
      expect(component.hasSubsystems).toBe(true);
      expect(component.hasNamespaces).toBe(true);
      expect(component.isAllConfigured).toBe(true);
    });

    it('scenario: all configured in object response across any group — isAllConfigured is true', () => {
      setSetupState({ hasGatewayGroups: true, hasSubsystems: true, hasNamespaces: true });
      component.ngOnInit();
      setQueryParams({ group: 'grp1' });
      expect(component.hasGatewayGroups).toBe(true);
      expect(component.hasSubsystems).toBe(true);
      expect(component.hasNamespaces).toBe(true);
      expect(component.isAllConfigured).toBe(true);
    });

    it('scenario: subsystems exist in grp2 only — step 2 still marked complete', () => {
      setSetupState({ hasGatewayGroups: true, hasSubsystems: false, hasNamespaces: false });
      component.ngOnInit();
      setQueryParams({ group: 'grp1' });
      expect(component.hasGatewayGroups).toBe(true);
      expect(component.hasSubsystems).toBe(false);
      expect(component.hasNamespaces).toBe(false);
      expect(component.isAllConfigured).toBe(false);
    });

    it('scenario: full config in grp2 only — onboarding complete regardless of selected group', () => {
      setSetupState({ hasGatewayGroups: true, hasSubsystems: false, hasNamespaces: false });
      component.ngOnInit();
      setQueryParams({ group: 'grp1' });
      expect(component.hasGatewayGroups).toBe(true);
      expect(component.hasSubsystems).toBe(false);
      expect(component.hasNamespaces).toBe(false);
      expect(component.isAllConfigured).toBe(false);
    });

    it('should trigger state reload when refresh$ emits', () => {
      component.ngOnInit();

      setSetupState({ hasGatewayGroups: false, hasSubsystems: false, hasNamespaces: false });
      emitRefresh();

      expect(component.hasGatewayGroups).toBe(false);
      expect(component.hasSubsystems).toBe(false);
      expect(component.hasNamespaces).toBe(false);
    });

    it('should reload state on NavigationEnd (e.g. navigating back from a form)', () => {
      component.ngOnInit();

      setSetupState({ hasGatewayGroups: false, hasSubsystems: false, hasNamespaces: false });

      routerEvents$.next(
        new NavigationEnd(1, '/block/nvmeof/namespaces', '/block/nvmeof/namespaces')
      );

      expect(component.hasGatewayGroups).toBe(false);
      expect(component.hasSubsystems).toBe(false);
      expect(component.hasNamespaces).toBe(false);
    });

    it('should show the initial gateway-only state after the last subsystem and namespace are removed', () => {
      component.ngOnInit();

      setSetupState({ hasGatewayGroups: true, hasSubsystems: false, hasNamespaces: false });

      emitRefresh();

      expect(component.hasGatewayGroups).toBe(true);
      expect(component.hasSubsystems).toBe(false);
      expect(component.hasNamespaces).toBe(false);
      expect(component.isAllConfigured).toBe(false);
    });

    it('should show the initial empty state after the last gateway is removed', () => {
      component.ngOnInit();

      setSetupState({ hasGatewayGroups: false, hasSubsystems: false, hasNamespaces: false });

      emitRefresh();

      expect(component.hasGatewayGroups).toBe(false);
      expect(component.hasSubsystems).toBe(false);
      expect(component.hasNamespaces).toBe(false);
      expect(component.isAllConfigured).toBe(false);
    });

    it('should refresh setup cards when the gateway list refreshes to empty', () => {
      component.ngOnInit();

      setSetupState({ hasGatewayGroups: false, hasSubsystems: false, hasNamespaces: false });
      emitRefresh();

      expect(component.hasGatewayGroups).toBe(false);
      expect(component.hasSubsystems).toBe(false);
      expect(component.hasNamespaces).toBe(false);
      expect(component.isAllConfigured).toBe(false);
    });

    it('should show gateway step complete after a gateway is created', () => {
      setSetupState({ hasGatewayGroups: false, hasSubsystems: false, hasNamespaces: false });
      component.ngOnInit();

      setSetupState({ hasGatewayGroups: true, hasSubsystems: false, hasNamespaces: false });

      emitRefresh();

      expect(component.hasGatewayGroups).toBe(true);
      expect(component.hasSubsystems).toBe(false);
      expect(component.hasNamespaces).toBe(false);
      expect(component.isAllConfigured).toBe(false);
    });

    it('should use fetchSetupState on refresh', () => {
      setSetupState({ hasGatewayGroups: false, hasSubsystems: false, hasNamespaces: false });
      component.ngOnInit();
      nvmeofServiceSpy.fetchSetupState.mockClear();

      emitRefresh();

      expect(nvmeofServiceSpy.fetchSetupState).toHaveBeenCalledTimes(1);
    });

    it('should render correct setup card messages after all gateway groups are removed', () => {
      setRouterUrl('/block/nvmeof/gateways');
      component.ngOnInit();

      setSetupState({ hasGatewayGroups: false, hasSubsystems: false, hasNamespaces: false });
      emitRefresh();
      fixture.detectChanges();

      const cardElements = fixture.debugElement.queryAll((el) => el.name === 'cd-setup-step-card');

      expect(cardElements.length).toBe(3);
      expect(cardElements[0].componentInstance.statusMessage).toBe(
        'No gateway groups configured for this cluster yet.'
      );
      expect(cardElements[1].componentInstance.statusMessage).toBe(
        'No subsystem configured for this cluster yet.'
      );
      expect(cardElements[2].componentInstance.statusMessage).toBe(
        'No namespace allocated or mapped yet.'
      );
    });

    it('should render success setup card messages before gateway groups are removed', () => {
      setRouterUrl('/block/nvmeof/gateways');
      component.ngOnInit();
      emitRefresh();
      fixture.detectChanges();
      component.showSetupCards = true;
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
