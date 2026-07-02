import { ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientModule } from '@angular/common/http';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { of, Subject } from 'rxjs';
import { take } from 'rxjs/operators';
import { RouterTestingModule } from '@angular/router/testing';
import { SharedModule } from '~/app/shared/shared.module';

import { NvmeofService } from '../../../shared/api/nvmeof.service';
import { NvmeofStateService } from '../nvmeof-state.service';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { NvmeofSubsystemsDetailsComponent } from '../nvmeof-subsystems-details/nvmeof-subsystems-details.component';
import { NvmeofNamespacesListComponent } from './nvmeof-namespaces-list.component';

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

class MockNvmeOfService {
  gatewayGroupsResponse: any = [[{ id: 'g1' }]];
  namespacesResponse: any = { namespaces: mockNamespaces };

  listGatewayGroups() {
    return of(this.gatewayGroupsResponse);
  }

  formatGwGroupsList(_response: any) {
    return [{ content: 'g1', selected: false }];
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

  let modalService: MockModalCdsService;
  let nvmeofService: MockNvmeOfService;

  beforeEach(async () => {
    const nvmeofStateServiceMock = {
      refresh$: new Subject<void>(),
      requestRefresh: jest.fn()
    };

    await TestBed.configureTestingModule({
      declarations: [NvmeofNamespacesListComponent, NvmeofSubsystemsDetailsComponent],
      imports: [HttpClientModule, RouterTestingModule, SharedModule],
      providers: [
        { provide: NvmeofService, useClass: MockNvmeOfService },
        { provide: AuthStorageService, useClass: MockAuthStorageService },
        { provide: ModalCdsService, useClass: MockModalCdsService },
        { provide: TaskWrapperService, useClass: MockTaskWrapperService },
        { provide: NvmeofStateService, useValue: nvmeofStateServiceMock }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();

    fixture = TestBed.createComponent(NvmeofNamespacesListComponent);
    component = fixture.componentInstance;
    component.ngOnInit();
    component.subsystemNQN = 'nqn.2001-07.com.ceph:1721040751436';
    fixture.detectChanges();
    modalService = TestBed.inject(ModalCdsService) as any;
    nvmeofService = TestBed.inject(NvmeofService) as any;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should retrieve namespaces', (done) => {
    component.group = 'g1';
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
    component.group = 'g1';
    nvmeofService.namespacesResponse = {
      namespaces: [
        { nsid: 1, ns_subsystem_nqn: 'sub1' },
        { nsid: 1, ns_subsystem_nqn: 'sub1' },
        { nsid: 1, ns_subsystem_nqn: 'sub2' }
      ]
    };

    component.namespaces$.pipe(take(1)).subscribe((namespaces) => {
      expect(namespaces).toEqual([
        { nsid: 1, ns_subsystem_nqn: 'sub1', unique_id: '1_sub1' },
        { nsid: 1, ns_subsystem_nqn: 'sub2', unique_id: '1_sub2' }
      ]);
      done();
    });

    component.fetchData();
  });
});
