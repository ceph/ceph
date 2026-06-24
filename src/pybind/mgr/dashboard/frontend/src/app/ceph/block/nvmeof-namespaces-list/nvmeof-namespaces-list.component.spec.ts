import { ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientModule } from '@angular/common/http';
import { CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { BehaviorSubject, Subject, of } from 'rxjs';
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
}

class MockAuthStorageService {
  getPermissions() {
    return { nvmeof: {} };
  }
}

class MockModalCdsService {
  show = jasmine.createSpy('show');
}

class MockTaskWrapperService {}

describe('NvmeofNamespacesListComponent', () => {
  let component: NvmeofNamespacesListComponent;
  let fixture: ComponentFixture<NvmeofNamespacesListComponent>;
  let queryParams$: BehaviorSubject<Record<string, string>>;
  let modalService: MockModalCdsService;
  let nvmeofService: MockNvmeOfService;
  const activatedRouteMock = {
    queryParams: null as any,
    snapshot: { queryParams: {} as Record<string, string> }
  };

  beforeEach(async () => {
    const refresh$ = new Subject<void>();
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
          useValue: { refresh$: refresh$.asObservable(), requestRefresh: jest.fn() }
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
  });

  it('should create', () => {
    expect(component).toBeTruthy();
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
    component.fetchData();
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
    const args = modalService.show.calls.mostRecent().args[1];
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

    component.fetchData();
  });

  it('should update group and trigger namespace fetch on group change', () => {
    const fetchDataSpy = jest.spyOn(component, 'fetchData');

    component.onGroupChange('g1');

    expect(component.group).toBe('g1');
    expect(fetchDataSpy).not.toHaveBeenCalled(); // onGroupChange calls namespaceSubject.next() directly
  });

  it('should clear group on onGroupChange with null', () => {
    component.group = 'g1';
    component.onGroupChange(null);
    expect(component.group).toBeNull();
  });
});
