import { ComponentFixture, TestBed, fakeAsync, tick } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { ActivatedRoute, NavigationEnd, Router } from '@angular/router';
import { of, Subject } from 'rxjs';

import { NgbTooltipModule } from '@ng-bootstrap/ng-bootstrap';
import { GridModule, TilesModule } from 'carbon-components-angular';

import { NvmeofSubsystemOverviewComponent } from './nvmeof-subsystem-overview.component';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { URLVerbs } from '~/app/shared/constants/app.constants';
import { SharedModule } from '~/app/shared/shared.module';
import { NvmeofSubsystem, NvmeofSubsystemInitiator } from '~/app/shared/models/nvmeof';

describe('NvmeofSubsystemOverviewComponent', () => {
  let component: NvmeofSubsystemOverviewComponent;
  let fixture: ComponentFixture<NvmeofSubsystemOverviewComponent>;
  let nvmeofService: NvmeofService;
  let router: Router;
  let activatedRoute: ActivatedRoute;
  let routerEvents$: Subject<any>;

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

  const defaultActivatedRoute = {
    parent: { params: of({ subsystem_nqn: 'nqn.2016-06.io.spdk:cnode1' }) },
    queryParams: of({ group: 'group1' })
  };

  /**
   * Creates a TestBed configuration with custom service overrides.
   * Avoids repeating the full module declaration in tests that need different mock data.
   */
  function createTestBed(
    initiators: NvmeofSubsystemInitiator[],
    subsystem: NvmeofSubsystem = mockSubsystem,
    activatedRoute: object = defaultActivatedRoute
  ): Promise<ComponentFixture<NvmeofSubsystemOverviewComponent>> {
    return TestBed.configureTestingModule({
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
        { provide: ActivatedRoute, useValue: activatedRoute },
        {
          provide: NvmeofService,
          useValue: {
            getSubsystem: jest.fn().mockReturnValue(of(subsystem)),
            getInitiators: jest.fn().mockReturnValue(of(initiators))
          }
        }
      ]
    })
      .compileComponents()
      .then(() => {
        const f = TestBed.createComponent(NvmeofSubsystemOverviewComponent);
        f.detectChanges();
        tick();
        f.detectChanges();
        return f;
      });
  }

  beforeEach(async () => {
    routerEvents$ = new Subject<any>();

    await TestBed.configureTestingModule({
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
        { provide: ActivatedRoute, useValue: defaultActivatedRoute },
        {
          provide: NvmeofService,
          useValue: {
            getSubsystem: jest.fn().mockReturnValue(of(mockSubsystem)),
            getInitiators: jest.fn().mockReturnValue(of([]))
          }
        },
        {
          provide: Router,
          useValue: {
            events: routerEvents$.asObservable(),
            navigate: jest.fn().mockResolvedValue(true)
          }
        }
      ]
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(NvmeofSubsystemOverviewComponent);
    component = fixture.componentInstance;
    nvmeofService = TestBed.inject(NvmeofService);
    router = TestBed.inject(Router);
    activatedRoute = TestBed.inject(ActivatedRoute);
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

  it('should not fetch when subsystemNQN is missing', fakeAsync(async () => {
    TestBed.resetTestingModule();
    const noNqnRoute = { parent: { params: of({}) }, queryParams: of({ group: 'group1' }) };
    const f = await createTestBed([], mockSubsystem, noNqnRoute);
    const newService = TestBed.inject(NvmeofService);
    expect(newService.getSubsystem).not.toHaveBeenCalled();
    expect(f.componentInstance.subsystem).toBeUndefined();
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
    // has_dhchap_key=true but no initiators with use_dhchap → No authentication
    expect(hostAccessText).toContain('No authentication');
    expect(hostAccessText).toContain('Edit');
  }));

  it('should display Bidirectional when subsystem and host both have keys', fakeAsync(async () => {
    TestBed.resetTestingModule();
    const f = await createTestBed([{ nqn: 'nqn.host-1', use_dhchap: true }]);
    expect(f.nativeElement.textContent).toContain('Bi-directional');
  }));

  it('should display Unidirectional when only host has a key', fakeAsync(async () => {
    TestBed.resetTestingModule();
    const f = await createTestBed([{ nqn: 'nqn.host-1', use_dhchap: true }], {
      ...mockSubsystem,
      has_dhchap_key: false
    });
    expect(f.nativeElement.textContent).toContain('Unidirectional');
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
    (nvmeofService.getSubsystem as jest.Mock).mockClear();

    routerEvents$.next(new NavigationEnd(1, '/nvmeof/(modal:add)', '/nvmeof/(modal:add)'));
    expect(nvmeofService.getSubsystem).not.toHaveBeenCalled();

    routerEvents$.next(
      new NavigationEnd(2, '/nvmeof/subsystems/overview', '/nvmeof/subsystems/overview')
    );
    expect(nvmeofService.getSubsystem).toHaveBeenCalledWith('nqn.2016-06.io.spdk:cnode1', 'group1');
  });
});
