import { ComponentFixture, TestBed, fakeAsync, tick } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { ActivatedRoute } from '@angular/router';
import { Observable, Subject, of, throwError } from 'rxjs';

import { NvmeofSubsystemNamespacesListComponent } from './nvmeof-subsystem-namespaces-list.component';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { NvmeofStateService } from '../nvmeof-state.service';
import { SharedModule } from '~/app/shared/shared.module';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { TableComponent } from '~/app/shared/datatable/table/table.component';

describe('NvmeofSubsystemNamespacesListComponent', () => {
  let component: NvmeofSubsystemNamespacesListComponent;
  let fixture: ComponentFixture<NvmeofSubsystemNamespacesListComponent>;
  let nvmeofService: NvmeofService;
  let refresh$: Subject<void>;
  let nvmeofStateService: { refresh$: any; requestRefresh: jest.Mock };
  let modalService: { show: jest.Mock };
  let taskWrapper: { wrapTaskAroundCall: jest.Mock };

  const mockNamespaces = [
    {
      nsid: 1,
      subsystem_nqn: 'nqn.2016-06.io.spdk:cnode1',
      rbd_image_name: 'image1',
      rbd_pool_name: 'pool1',
      rbd_image_size: 1024,
      block_size: 512,
      rw_ios_per_second: 100
    },
    {
      nsid: 2,
      subsystem_nqn: 'nqn.2016-06.io.spdk:cnode2', // Different subsystem
      rbd_image_name: 'image2',
      rbd_pool_name: 'pool1',
      rbd_image_size: 1024,
      block_size: 512,
      rw_ios_per_second: 100
    }
  ];

  class MockAuthStorageService {
    getPermissions() {
      return { nvmeof: {} };
    }
  }

  beforeEach(async () => {
    refresh$ = new Subject<void>();
    nvmeofStateService = {
      refresh$: refresh$.asObservable(),
      requestRefresh: jest.fn()
    };
    modalService = { show: jest.fn() };
    taskWrapper = { wrapTaskAroundCall: jest.fn() };

    await TestBed.configureTestingModule({
      declarations: [NvmeofSubsystemNamespacesListComponent],
      imports: [HttpClientTestingModule, RouterTestingModule, SharedModule],
      providers: [
        {
          provide: ActivatedRoute,
          useValue: {
            parent: {
              params: of({ subsystem_nqn: 'nqn.2016-06.io.spdk:cnode1', group: 'group1' })
            },
            queryParams: of({ group: 'group1' })
          }
        },
        {
          provide: NvmeofService,
          useValue: {
            listNamespaces: jest.fn().mockReturnValue(of(mockNamespaces)),
            deleteNamespace: jest.fn().mockReturnValue(of({}))
          }
        },
        { provide: AuthStorageService, useClass: MockAuthStorageService },
        { provide: ModalCdsService, useValue: modalService },
        { provide: TaskWrapperService, useValue: taskWrapper },
        {
          provide: NvmeofStateService,
          useValue: nvmeofStateService
        }
      ]
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(NvmeofSubsystemNamespacesListComponent);
    component = fixture.componentInstance;
    nvmeofService = TestBed.inject(NvmeofService);
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).not.toBeNull();
    expect(component).not.toBeUndefined();
  });

  it('should disable periodic table auto-reload', () => {
    const tableDebug = fixture.debugElement.query(By.directive(TableComponent));
    expect(tableDebug.componentInstance.autoReload).toBe(false);
  });

  it('should list namespaces filtered by subsystem', fakeAsync(() => {
    component.ngOnInit(); // Trigger ngOnInit
    tick(); // wait for ngOnInit subscription
    expect(nvmeofService.listNamespaces).toHaveBeenCalledWith(
      'group1',
      'nqn.2016-06.io.spdk:cnode1'
    );
    expect(component.namespaces.length).toEqual(2);
    expect(component.namespaces[0].nsid).toEqual(1);
  }));
  it('should have table actions defined', () => {
    component.ngOnInit();
    expect(component.tableActions).toBeDefined();
    expect(component.tableActions.length).toBeGreaterThan(0);
  });

  it('should refresh the list when refresh$ emits', () => {
    const listSpy = jest.spyOn(component, 'listNamespaces');
    listSpy.mockClear();

    refresh$.next();

    expect(listSpy).toHaveBeenCalledTimes(1);
  });

  it('should request list refresh after successful delete', () => {
    taskWrapper.wrapTaskAroundCall.mockReturnValue(
      new Observable((observer) => {
        observer.complete();
      })
    );
    component.subsystemNQN = 'nqn.2016-06.io.spdk:cnode1';
    component.group = 'group1';
    component.selection = {
      first: () => ({ nsid: 1 })
    } as any;

    component.deleteNamespaceModal();
    const submitActionObservable = modalService.show.mock.calls[0][1].submitActionObservable;
    submitActionObservable().subscribe();

    expect(nvmeofStateService.requestRefresh).toHaveBeenCalledTimes(1);
  });

  it('should not request list refresh after failed delete', () => {
    taskWrapper.wrapTaskAroundCall.mockReturnValue(throwError(() => ({ message: 'failed' })));
    component.subsystemNQN = 'nqn.2016-06.io.spdk:cnode1';
    component.group = 'group1';
    component.selection = {
      first: () => ({ nsid: 1 })
    } as any;

    component.deleteNamespaceModal();
    const submitActionObservable = modalService.show.mock.calls[0][1].submitActionObservable;
    submitActionObservable().subscribe({
      error: () => undefined
    });

    expect(nvmeofStateService.requestRefresh).not.toHaveBeenCalled();
  });
});
