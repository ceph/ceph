import { ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientModule } from '@angular/common/http';
import { ActivatedRoute, Router } from '@angular/router';
import { BehaviorSubject, Subject, of } from 'rxjs';
import { take } from 'rxjs/operators';
import { RouterTestingModule } from '@angular/router/testing';
import { SharedModule } from '~/app/shared/shared.module';

import { NvmeofService } from '../../../shared/api/nvmeof.service';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { NvmeofSubsystemsComponent } from './nvmeof-subsystems.component';
import { NvmeofSubsystemsDetailsComponent } from '../nvmeof-subsystems-details/nvmeof-subsystems-details.component';
import { NvmeofGatewayGroupFilterComponent } from '../nvmeof-gateway-group-filter/nvmeof-gateway-group-filter.component';
import { NvmeofStateService } from '../nvmeof-state.service';

const mockSubsystems = [
  {
    nqn: 'nqn.2001-07.com.ceph:1720603703820',
    enable_ha: true,
    serial_number: 'Ceph30487186726692',
    model_number: 'Ceph bdev Controller',
    min_cntlid: 1,
    max_cntlid: 2040,
    namespace_count: 0,
    subtype: 'NVMe',
    max_namespaces: 256
  }
];

const mockGroups = [
  [
    {
      service_name: 'nvmeof.default',
      service_type: 'nvmeof',
      service_id: 'default',
      spec: { group: 'default' }
    }
  ]
];

class MockNvmeOfService {
  listGatewayGroups() {
    return of([
      [
        {
          service_name: 'nvmeof.default',
          service_type: 'nvmeof',
          service_id: 'default',
          spec: { group: 'default' }
        }
      ]
    ]);
  }

  formatGwGroupsList(response: any) {
    return (response?.[0] || []).map((g: any) => ({
      content: g.spec.group,
      selected: false
    }));
  }

  listSubsystems() {
    return of(mockSubsystems);
  }

  getInitiators() {
    return of([]);
  }
}

class MockAuthStorageService {
  getPermissions() {
    return { nvmeof: {} };
  }
}

class MockModalService {}

class MockTaskWrapperService {}

describe('NvmeofSubsystemsComponent', () => {
  let component: NvmeofSubsystemsComponent;
  let fixture: ComponentFixture<NvmeofSubsystemsComponent>;
  let queryParams$: BehaviorSubject<Record<string, string>>;
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
      declarations: [NvmeofSubsystemsComponent, NvmeofSubsystemsDetailsComponent],
      imports: [
        HttpClientModule,
        RouterTestingModule,
        SharedModule,
        NvmeofGatewayGroupFilterComponent
      ],
      providers: [
        { provide: NvmeofService, useClass: MockNvmeOfService },
        { provide: AuthStorageService, useClass: MockAuthStorageService },
        { provide: ModalCdsService, useClass: MockModalService },
        { provide: TaskWrapperService, useClass: MockTaskWrapperService },
        { provide: ActivatedRoute, useValue: activatedRouteMock },
        {
          provide: NvmeofStateService,
          useValue: { refresh$: refresh$.asObservable(), requestRefresh: jest.fn() }
        }
      ]
    }).compileComponents();

    const router = TestBed.inject(Router);
    jest.spyOn(router, 'navigate').mockImplementation((_commands, extras?) => {
      const group = extras?.queryParams?.['group'];
      const params = group ? { group: String(group) } : {};
      activatedRouteMock.snapshot.queryParams = params;
      queryParams$.next(params);
      return Promise.resolve(true);
    });

    fixture = TestBed.createComponent(NvmeofSubsystemsComponent);
    component = fixture.componentInstance;
    component.ngOnInit();
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should retrieve subsystems', (done) => {
    const expected = mockSubsystems.map((s) => ({
      ...s,
      gw_group: component.groupHandler.group,
      auth: 'No authentication',
      initiator_count: 0
    }));
    component.subsystems$.pipe(take(1)).subscribe((subsystems) => {
      expect(subsystems).toEqual(expected);
      done();
    });
    component.getSubsystems();
  });

  it('should set first group as default initially', () => {
    expect(component.groupHandler.group).toBe(mockGroups[0][0].spec.group);
  });

  it('should show subsystems across groups when dropdown selection is cleared', (done) => {
    component.groupHandler.onGroupClear();
    component.subsystems$.pipe(take(1)).subscribe((subsystems) => {
      expect(subsystems.length).toBeGreaterThan(0);
      done();
    });
    component.getSubsystems();
  });

  it('should clear selected group and stop fetching subsystems', () => {
    component.groupHandler.group = 'default';

    component.groupHandler.onGroupChange(null);

    expect(component.groupHandler.group).toBeNull();
  });
});
