import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { HttpClientModule } from '@angular/common/http';
import { CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { BehaviorSubject, Observable, Subject, of, throwError } from 'rxjs';
import { skip, take } from 'rxjs/operators';
import { RouterTestingModule } from '@angular/router/testing';
import { SharedModule } from '~/app/shared/shared.module';

import { NvmeofService } from '../../../shared/api/nvmeof.service';
import { NvmeofStateService } from '../nvmeof-state.service';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { NvmeofSubsystemsDetailsComponent } from '../nvmeof-subsystems-details/nvmeof-subsystems-details.component';
import { NvmeofNamespacesListComponent } from './nvmeof-namespaces-list.component';
import { NvmeofGatewayGroupFilterComponent } from '../nvmeof-gateway-group-filter/nvmeof-gateway-group-filter.component';
import { TableComponent } from '~/app/shared/datatable/table/table.component';

const mockNamespaces = [
  {
    nsid: 1,
    uuid: 'f4396245-186f-401a-b71c-945ccf0f0cc9',
    bdev_name: 'bdev_f4396245-186f-401a-b71c-945ccf0f0cc9',
    rbd_image_name: 'string',
    rbd_pool_name: 'rbd',
    load_balancing_group: 1,
    rbd_image_size: 1024,
    block_size: 512,
    rw_ios_per_second: 0,
    rw_mbytes_per_second: 0,
    r_mbytes_per_second: 0,
    w_mbytes_per_second: 0
  }
];

const mockGroups = [
  [
    {
      service_name: 'nvmeof.rbd.g1',
      service_type: 'nvmeof',
      unmanaged: false,
      spec: {
        group: 'g1'
      }
    }
  ],
  1
];

const mockFormattedGwGroups = [
  {
    content: 'g1'
  }
];

class MockNvmeOfService {
  gatewayGroupsResponse: any = [[{ id: 'g1' }]];
  namespacesResponse: any = { namespaces: mockNamespaces };

  listGatewayGroups() {
    return of(mockGroups);
  }

  formatGwGroupsList(_response: any) {
    return mockFormattedGwGroups;
  }

  listNamespaces(_group?: string) {
    return of(this.namespacesResponse);
  }

  deleteNamespace() {
    return of({});
  }
}

class MockAuthStorageService {
  getPermissions() {
    return { nvmeof: {} };
  }
}

class MockModalCdsService {
  show = jest.fn();
}

class MockTaskWrapperService {
  wrapTaskAroundCall = jest.fn();
}

describe('NvmeofNamespacesListComponent', () => {
  let component: NvmeofNamespacesListComponent;
  let fixture: ComponentFixture<NvmeofNamespacesListComponent>;
  let queryParams$: BehaviorSubject<Record<string, string>>;
  let refresh$: Subject<void>;
  let modalService: MockModalCdsService;
  let nvmeofService: MockNvmeOfService;
  let nvmeofStateService: { refresh$: any; requestRefresh: jest.Mock };
  let taskWrapper: MockTaskWrapperService;
  const activatedRouteMock = {
    queryParams: null as any,
    snapshot: { queryParams: {} as Record<string, string> }
  };

  beforeEach(async () => {
    refresh$ = new Subject<void>();
    nvmeofStateService = {
      refresh$: refresh$.asObservable(),
      requestRefresh: jest.fn()
    };
    queryParams$ = new BehaviorSubject<Record<string, string>>({});
    activatedRouteMock.queryParams = queryParams$.asObservable();
    activatedRouteMock.snapshot.queryParams = queryParams$.value;

    await TestBed.configureTestingModule({
      declarations: [NvmeofNamespacesListComponent, NvmeofSubsystemsDetailsComponent],
      imports: [
        HttpClientModule,
        RouterTestingModule,
        SharedModule,
        NvmeofGatewayGroupFilterComponent
      ],
      providers: [
        { provide: NvmeofService, useClass: MockNvmeOfService },
        { provide: AuthStorageService, useClass: MockAuthStorageService },
        { provide: ModalCdsService, useClass: MockModalCdsService },
        { provide: TaskWrapperService, useClass: MockTaskWrapperService },
        { provide: ActivatedRoute, useValue: activatedRouteMock },
        {
          provide: NvmeofStateService,
          useValue: nvmeofStateService
        }
      ],
      schemas: [CUSTOM_ELEMENTS_SCHEMA]
    }).compileComponents();

    const router = TestBed.inject(Router);
    jest.spyOn(router, 'navigate').mockImplementation((_commands, extras?) => {
      const group = extras?.queryParams?.['group'];
      const params = group ? { group: String(group) } : {};
      activatedRouteMock.snapshot.queryParams = params;
      queryParams$.next(params);
      return Promise.resolve(true);
    });

    fixture = TestBed.createComponent(NvmeofNamespacesListComponent);
    component = fixture.componentInstance;
    component.subsystemNQN = 'nqn.2001-07.com.ceph:1721040751436';
    component.ngOnInit();
    fixture.detectChanges();
    modalService = TestBed.inject(ModalCdsService) as any;
    nvmeofService = TestBed.inject(NvmeofService) as any;
    taskWrapper = TestBed.inject(TaskWrapperService) as any;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should disable periodic table auto-reload', () => {
    const tableDebug = fixture.debugElement.query(By.directive(TableComponent));
    expect(tableDebug.componentInstance.autoReload).toBe(false);
  });

  it('should retrieve namespaces', (done) => {
    component.groupHandler.group = 'g1';
    component.namespaces$.pipe(take(1)).subscribe((namespaces) => {
      expect(namespaces).toEqual(
        mockNamespaces.map((ns) => ({
          ...ns,
          unique_id: `${ns.nsid}_${ns['ns_subsystem_nqn']}`
        }))
      );
      done();
    });
    component.listNamespaces();
  });

  it('should open delete modal with correct data', () => {
    // Mock selection
    const namespace = {
      nsid: 1,
      ns_subsystem_nqn: 'nqn.2001-07.com.ceph:1721040751436'
    };
    component.selection = {
      first: () => namespace
    } as any;
    component.deleteNamespaceModal();
    expect(modalService.show).toHaveBeenCalled();
    const args = modalService.show.mock.calls[0][1];
    expect(args.itemNames).toEqual([1]);
    expect(args.itemDescription).toBeDefined();
    expect(typeof args.submitActionObservable).toBe('function');
  });

  it('should deduplicate namespaces by nsid and subsystem nqn', (done) => {
    component.groupHandler.group = 'g1';
    nvmeofService.namespacesResponse = {
      namespaces: [
        { nsid: 1, ns_subsystem_nqn: 'sub1' },
        { nsid: 1, ns_subsystem_nqn: 'sub1' },
        { nsid: 1, ns_subsystem_nqn: 'sub2' }
      ]
    };

    component.namespaces$.pipe(skip(1), take(1)).subscribe((namespaces) => {
      expect(namespaces).toEqual([
        { nsid: 1, ns_subsystem_nqn: 'sub1', unique_id: '1_sub1' },
        { nsid: 1, ns_subsystem_nqn: 'sub2', unique_id: '1_sub2' }
      ]);
      done();
    });

    component.listNamespaces();
  });

  it('should update group and trigger namespace fetch on group change', () => {
    const listNamespacesSpy = jest.spyOn(component, 'listNamespaces');
    listNamespacesSpy.mockClear();

    component.onGroupChange('g1');

    expect(component.group).toBe('g1');
    expect(listNamespacesSpy).toHaveBeenCalled();
  });

  it('should clear group on onGroupChange with null', () => {
    component.group = 'g1';
    component.onGroupChange(null);
    expect(component.group).toBeNull();
  });

  it('should refresh the list when refresh$ emits', () => {
    const listSpy = jest.spyOn(component, 'listNamespaces');
    listSpy.mockClear();

    refresh$.next();

    expect(listSpy).toHaveBeenCalled();
  });

  it('should request list refresh after successful delete', () => {
    taskWrapper.wrapTaskAroundCall.mockReturnValue(
      new Observable((observer) => {
        observer.complete();
      })
    );
    component.selection = {
      first: () => ({
        nsid: 1,
        ns_subsystem_nqn: 'nqn.2001-07.com.ceph:1721040751436'
      })
    } as any;
    component.groupHandler.group = 'g1';

    component.deleteNamespaceModal();
    const submitActionObservable = modalService.show.mock.calls[0][1].submitActionObservable;
    submitActionObservable().subscribe();

    expect(nvmeofStateService.requestRefresh).toHaveBeenCalledTimes(1);
  });

  it('should not request list refresh after failed delete', () => {
    taskWrapper.wrapTaskAroundCall.mockReturnValue(throwError(() => ({ message: 'failed' })));
    component.selection = {
      first: () => ({
        nsid: 1,
        ns_subsystem_nqn: 'nqn.2001-07.com.ceph:1721040751436'
      })
    } as any;
    component.groupHandler.group = 'g1';

    component.deleteNamespaceModal();
    const submitActionObservable = modalService.show.mock.calls[0][1].submitActionObservable;
    submitActionObservable().subscribe({
      error: () => undefined
    });

    expect(nvmeofStateService.requestRefresh).not.toHaveBeenCalled();
  });
});
