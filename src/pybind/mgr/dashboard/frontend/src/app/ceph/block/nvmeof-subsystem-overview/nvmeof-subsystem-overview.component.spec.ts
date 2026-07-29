import { ComponentFixture, TestBed, fakeAsync, tick } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { ActivatedRoute, NavigationEnd, Router } from '@angular/router';
import { of, Subject, BehaviorSubject } from 'rxjs';

import { NgbTooltipModule } from '@ng-bootstrap/ng-bootstrap';
import { GridModule, TilesModule } from 'carbon-components-angular';

import { NvmeofSubsystemOverviewComponent } from './nvmeof-subsystem-overview.component';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { URLVerbs } from '~/app/shared/constants/app.constants';
import { SharedModule } from '~/app/shared/shared.module';
import { NvmeofSubsystem } from '~/app/shared/models/nvmeof';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('NvmeofSubsystemOverviewComponent', () => {
  let component: NvmeofSubsystemOverviewComponent;
  let fixture: ComponentFixture<NvmeofSubsystemOverviewComponent>;
  let nvmeofService: NvmeofService;
  let router: Router;
  let activatedRoute: ActivatedRoute;
  let routerEvents$: Subject<any>;

  let parentParams$: BehaviorSubject<any>;
  let queryParams$: BehaviorSubject<any>;

  const mockSubsystem: NvmeofSubsystem = {
    nqn: 'nqn.2016-06.io.spdk:cnode1',
    serial_number: 'Ceph30487186726692',
    model_number: 'Ceph bdev Controller',
    min_cntlid: 1,
    max_cntlid: 2040,
    subtype: 'NVMe',
    namespace_count: 3,
    max_namespaces: 256,
    enable_ha: true,
    allow_any_host: true,
    gw_group: 'gateway-prod',
    has_dhchap_key: true,
    network_mask: []
  };

  let nvmeofServiceMock = {
    getSubsystem: jest.fn().mockReturnValue(of(mockSubsystem)),
    getInitiators: jest.fn().mockReturnValue(of([]))
  };

  configureTestBed({
    declarations: [NvmeofSubsystemOverviewComponent],
    imports: [
      HttpClientTestingModule,
      RouterTestingModule,
      SharedModule,
      NgbTooltipModule,
      TilesModule,
      GridModule
    ],
    providers: [
      {
        provide: ActivatedRoute,
        useFactory: () => ({
          parent: { params: parentParams$.asObservable() },
          queryParams: queryParams$.asObservable()
        })
      },
      { provide: NvmeofService, useFactory: () => nvmeofServiceMock }
    ]
  });

  beforeEach(() => {
    jest.clearAllMocks();

    // Reset route streams for each test
    parentParams$ = new BehaviorSubject({ subsystem_nqn: 'nqn.2016-06.io.spdk:cnode1' });
    queryParams$ = new BehaviorSubject({ group: 'group1' });

    nvmeofServiceMock.getSubsystem.mockReturnValue(of(mockSubsystem));
    nvmeofServiceMock.getInitiators.mockReturnValue(of([]));

    routerEvents$ = new Subject<any>();

    fixture = TestBed.createComponent(NvmeofSubsystemOverviewComponent);
    component = fixture.componentInstance;
    nvmeofService = TestBed.inject(NvmeofService);
    router = TestBed.inject(Router);
    activatedRoute = TestBed.inject(ActivatedRoute);

    Object.defineProperty(router, 'events', { get: () => routerEvents$.asObservable() });
    jest.spyOn(router, 'navigate').mockResolvedValue(true);

    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should fetch subsystem on init', fakeAsync(() => {
    component.ngOnInit();
    tick();
    expect(nvmeofService.getSubsystem).toHaveBeenCalledWith('nqn.2016-06.io.spdk:cnode1', 'group1');
  }));

  it('should store subsystem data', fakeAsync(() => {
    component.ngOnInit();
    tick();
    expect(component.subsystem).toEqual(mockSubsystem);
    expect(component.subsystem.serial_number).toBe('Ceph30487186726692');
    expect(component.subsystem.model_number).toBe('Ceph bdev Controller');
    expect(component.subsystem.max_cntlid).toBe(2040);
    expect(component.subsystem.min_cntlid).toBe(1);
    expect(component.subsystem.namespace_count).toBe(3);
    expect(component.subsystem.max_namespaces).toBe(256);
    expect(component.subsystem.gw_group).toBe('gateway-prod');
    expect(component.subsystem.subtype).toBe('NVMe');
  }));

  it('should not fetch when subsystemNQN is missing', fakeAsync(() => {
    // Clear the call history from the initial fixture.detectChanges()
    nvmeofServiceMock.getSubsystem.mockClear();

    // Push empty params to simulate missing NQN
    parentParams$.next({});

    // Force a fresh component lifecycle
    fixture = TestBed.createComponent(NvmeofSubsystemOverviewComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
    tick();

    expect(nvmeofServiceMock.getSubsystem).not.toHaveBeenCalled();
    expect(component.subsystem).toBeUndefined();
  }));

  it('should render detail labels in the template', fakeAsync(() => {
    component.ngOnInit();
    tick();
    fixture.detectChanges();

    const compiled = fixture.nativeElement;
    const labels = compiled.querySelectorAll('.cds--type-label-01');
    const labelTexts = Array.from(labels).map((el: HTMLElement) => el.textContent.trim());
    expect(labelTexts).toContain('Serial number');
    expect(labelTexts).toContain('Model Number');
    expect(labelTexts).toContain('Gateway group');
    expect(labelTexts).toContain('Subsystem Type');
    expect(labelTexts).toContain('Host access');
    expect(labelTexts).toContain('Authentication');
    expect(labelTexts).toContain('Listeners');
    expect(labelTexts).toContain('Maximum Controller Identifier');
    expect(labelTexts).toContain('Minimum Controller Identifier');
    expect(labelTexts).toContain('Namespaces');
    expect(labelTexts).toContain('Maximum allowed namespaces');
  }));

  it('should display host access and auth state from subsystem data', fakeAsync(() => {
    component.ngOnInit();
    tick();
    fixture.detectChanges();

    const hostAccessText = fixture.nativeElement.textContent;
    expect(hostAccessText).toContain('Allow all hosts');
    expect(hostAccessText).toContain('No authentication');
    expect(hostAccessText).toContain('Edit');
  }));

  it('should display Bidirectional when subsystem and host both have keys', fakeAsync(() => {
    nvmeofServiceMock.getInitiators.mockReturnValue(of([{ nqn: 'nqn.host-1', use_dhchap: true }]));

    fixture = TestBed.createComponent(NvmeofSubsystemOverviewComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
    tick();
    fixture.detectChanges();

    expect(fixture.nativeElement.textContent).toContain('Bi-directional');
  }));

  it('should display Unidirectional when only host has a key', fakeAsync(() => {
    nvmeofServiceMock.getInitiators.mockReturnValue(of([{ nqn: 'nqn.host-1', use_dhchap: true }]));
    nvmeofServiceMock.getSubsystem.mockReturnValue(
      of({
        ...mockSubsystem,
        has_dhchap_key: false
      })
    );

    fixture = TestBed.createComponent(NvmeofSubsystemOverviewComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
    tick();
    fixture.detectChanges();

    expect(fixture.nativeElement.textContent).toContain('Unidirectional');
  }));

  it('should open host access edit modal route when edit is clicked', () => {
    const navigateSpy = jest.spyOn(router, 'navigate').mockResolvedValue(true);
    component.groupName = 'group1';
    component.openEditHostAccessModal();

    expect(navigateSpy).toHaveBeenCalledWith(
      [{ outlets: { modal: [URLVerbs.ADD, 'initiator'] } }],
      {
        queryParams: { group: 'group1' },
        relativeTo: activatedRoute.parent
      }
    );
  });

  it('should refresh subsystem on non-modal navigation end', () => {
    nvmeofServiceMock.getSubsystem.mockClear();

    routerEvents$.next(new NavigationEnd(1, '/nvmeof/(modal:add)', '/nvmeof/(modal:add)'));
    expect(nvmeofService.getSubsystem).not.toHaveBeenCalled();

    routerEvents$.next(
      new NavigationEnd(2, '/nvmeof/subsystems/overview', '/nvmeof/subsystems/overview')
    );
    expect(nvmeofService.getSubsystem).toHaveBeenCalledWith('nqn.2016-06.io.spdk:cnode1', 'group1');
  });
});
