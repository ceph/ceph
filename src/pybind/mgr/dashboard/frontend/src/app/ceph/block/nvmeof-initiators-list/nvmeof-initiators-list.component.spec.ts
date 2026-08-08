import { ComponentFixture, TestBed, fakeAsync, tick } from '@angular/core/testing';
import { HttpClientModule } from '@angular/common/http';
import { ActivatedRoute, NavigationEnd, Router } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { of, Subject } from 'rxjs';

import { SharedModule } from '~/app/shared/shared.module';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { ALLOW_ALL_HOST } from '~/app/shared/models/nvmeof';

import { NvmeofInitiatorsListComponent } from './nvmeof-initiators-list.component';

const mockInitiators = [
  {
    nqn: 'nqn.2016-06.io.spdk:host1',
    use_dhchap: false
  }
];

const mockSubsystem = {
  nqn: 'nqn.2016-06.io.spdk:cnode1',
  serial_number: '12345',
  has_dhchap_key: false,
  allow_any_host: false
};

class MockNvmeOfService {
  getInitiators() {
    return of(mockInitiators);
  }
  getSubsystem() {
    return of(mockSubsystem);
  }
}

class MockAuthStorageService {
  getPermissions() {
    return { nvmeof: { read: true, create: true, delete: true } };
  }
}

class MockModalCdsService {}

class MockTaskWrapperService {}

describe('NvmeofInitiatorsListComponent', () => {
  let component: NvmeofInitiatorsListComponent;
  let fixture: ComponentFixture<NvmeofInitiatorsListComponent>;
  let nvmeofService: NvmeofService;
  let routeParams$: Subject<any>;
  let queryParams$: Subject<any>;
  let routerEvents$: Subject<any>;

  beforeEach(async () => {
    routeParams$ = new Subject<any>();
    queryParams$ = new Subject<any>();
    routerEvents$ = new Subject<any>();

    await TestBed.configureTestingModule({
      declarations: [NvmeofInitiatorsListComponent],
      imports: [HttpClientModule, RouterTestingModule, SharedModule],
      providers: [
        { provide: NvmeofService, useClass: MockNvmeOfService },
        { provide: AuthStorageService, useClass: MockAuthStorageService },
        { provide: ModalCdsService, useClass: MockModalCdsService },
        { provide: TaskWrapperService, useClass: MockTaskWrapperService },
        {
          provide: ActivatedRoute,
          useValue: {
            parent: { params: routeParams$.asObservable() },
            queryParams: queryParams$.asObservable()
          }
        },
        {
          provide: Router,
          useValue: {
            events: routerEvents$.asObservable(),
            navigate: jasmine.createSpy('navigate')
          }
        }
      ]
    }).compileComponents();
  });

  beforeEach(() => {
    nvmeofService = TestBed.inject(NvmeofService);
    fixture = TestBed.createComponent(NvmeofInitiatorsListComponent);
    component = fixture.componentInstance;
    component.subsystemNQN = 'nqn.2016-06.io.spdk:cnode1';
    component.group = 'group1';
    component.ngOnInit();
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should retrieve initiators and subsystem', fakeAsync(() => {
    component.listInitiators();
    component.getSubsystem();
    tick();
    expect(component.initiators).toEqual(mockInitiators);
    expect(component.subsystem).toEqual(mockSubsystem);
    expect(component.authStatus).toBe('No authentication');
    expect(component.initiatorColumns.length).toBe(2);
    expect(component.getDisplayedHostNqn(ALLOW_ALL_HOST)).toBe('Any');
  }));

  it('should default allowAllHosts to false', () => {
    expect(component.allowAllHosts).toBe(false);
  });

  it('should set allowAllHosts to true when subsystem allows any host and no initiators', fakeAsync(() => {
    const allowAllSubsystem = { ...mockSubsystem, allow_any_host: true };
    spyOn(nvmeofService, 'getInitiators').and.returnValue(of([]));
    spyOn(nvmeofService, 'getSubsystem').and.returnValue(of(allowAllSubsystem));
    component.listInitiators();
    component.getSubsystem();
    tick();
    expect(component.allowAllHosts).toBe(true);
  }));

  it('should update authStatus when initiator has dhchap_key', fakeAsync(() => {
    const initiatorsWithKey = [{ nqn: 'nqn1', use_dhchap: true }];
    spyOn(nvmeofService, 'getInitiators').and.returnValue(of(initiatorsWithKey));
    component.listInitiators();
    tick();
    expect(component.authStatus).toBe('Unidirectional');
  }));

  it('should update authStatus when subsystem has dhchap_key', fakeAsync(() => {
    const initiatorsWithKey = [{ nqn: 'nqn1', use_dhchap: true }];
    component.initiators = initiatorsWithKey;
    const subsystemWithKey = { ...mockSubsystem, has_dhchap_key: true };
    spyOn(nvmeofService, 'getSubsystem').and.returnValue(of(subsystemWithKey));
    component.getSubsystem();
    tick();
    expect(component.authStatus).toBe('Bi-directional');
  }));

  it('should fetch only when both route and query params are available', () => {
    const newFixture = TestBed.createComponent(NvmeofInitiatorsListComponent);
    const newComponent = newFixture.componentInstance;
    newComponent.subsystemNQN = undefined;
    newComponent.group = undefined;
    const listInitiatorsSpy = spyOn(newComponent, 'listInitiators');
    const getSubsystemSpy = spyOn(newComponent, 'getSubsystem');

    newComponent.ngOnInit();

    routeParams$.next({ subsystem_nqn: 'nqn.from.route' });
    expect(listInitiatorsSpy).not.toHaveBeenCalled();
    expect(getSubsystemSpy).not.toHaveBeenCalled();

    queryParams$.next({ group: 'group-from-query' });
    expect(listInitiatorsSpy).toHaveBeenCalledTimes(1);
    expect(getSubsystemSpy).toHaveBeenCalledTimes(1);
  });

  it('should refresh on non-modal navigation changes', () => {
    const newFixture = TestBed.createComponent(NvmeofInitiatorsListComponent);
    const newComponent = newFixture.componentInstance;
    newComponent.subsystemNQN = 'nqn.2016-06.io.spdk:cnode1';
    newComponent.group = 'group1';
    const listInitiatorsSpy = spyOn(newComponent, 'listInitiators');
    const getSubsystemSpy = spyOn(newComponent, 'getSubsystem');

    newComponent.ngOnInit();
    routerEvents$.next(new NavigationEnd(1, '/nvmeof/(modal:add)', '/nvmeof/(modal:add)'));
    expect(listInitiatorsSpy).toHaveBeenCalledTimes(1);
    expect(getSubsystemSpy).toHaveBeenCalledTimes(1);

    routerEvents$.next(new NavigationEnd(2, '/nvmeof/subsystem', '/nvmeof/subsystem'));
    expect(listInitiatorsSpy).toHaveBeenCalledTimes(2);
    expect(getSubsystemSpy).toHaveBeenCalledTimes(2);
  });

  it('should include ALLOW_ALL_HOST from array response', fakeAsync(() => {
    spyOn(nvmeofService, 'getInitiators').and.returnValue(
      of([
        { nqn: ALLOW_ALL_HOST, use_dhchap: false },
        { nqn: 'nqn.2016-06.io.spdk:host2', use_dhchap: false }
      ])
    );

    component.listInitiators();
    tick();

    expect(component.initiators).toEqual([
      { nqn: ALLOW_ALL_HOST, use_dhchap: false },
      { nqn: 'nqn.2016-06.io.spdk:host2', use_dhchap: false }
    ]);
  }));

  it('should support hosts-wrapper response from getInitiators', fakeAsync(() => {
    spyOn(nvmeofService, 'getInitiators').and.returnValue(
      of({
        hosts: [
          { nqn: ALLOW_ALL_HOST, use_dhchap: false },
          { nqn: 'nqn.2016-06.io.spdk:host3', use_dhchap: true }
        ]
      })
    );

    component.listInitiators();
    tick();

    expect(component.initiators).toEqual([
      { nqn: ALLOW_ALL_HOST, use_dhchap: false },
      { nqn: 'nqn.2016-06.io.spdk:host3', use_dhchap: true }
    ]);
  }));

  it('should set allowAllHosts to true when wildcard initiator exists', fakeAsync(() => {
    const allowAllSubsystem = { ...mockSubsystem, allow_any_host: true };
    spyOn(nvmeofService, 'getInitiators').and.returnValue(
      of([{ nqn: ALLOW_ALL_HOST, use_dhchap: false }])
    );
    spyOn(nvmeofService, 'getSubsystem').and.returnValue(of(allowAllSubsystem));

    component.listInitiators();
    component.getSubsystem();
    tick();

    expect(component.allowAllHosts).toBe(true);
  }));
});
