import { ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientModule } from '@angular/common/http';
import { ActivatedRoute, Router } from '@angular/router';
import { BehaviorSubject, of, Subject } from 'rxjs';
import { skip, take } from 'rxjs/operators';
import { RouterTestingModule } from '@angular/router/testing';
import { SharedModule } from '~/app/shared/shared.module';

import { NvmeofService } from '../../../shared/api/nvmeof.service';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { NvmeofSubsystemsComponent } from './nvmeof-subsystems.component';
import { NvmeofSubsystemsDetailsComponent } from '../nvmeof-subsystems-details/nvmeof-subsystems-details.component';
import { NvmeofGatewayGroupFilterComponent } from '../nvmeof-gateway-group-filter/nvmeof-gateway-group-filter.component';
import { ComboBoxModule, GridModule } from 'carbon-components-angular';
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
  let nvmeofService: MockNvmeOfService;
  let queryParams$: BehaviorSubject<Record<string, string>>;
  const activatedRouteMock = {
    queryParams: null as any,
    snapshot: { queryParams: {} as Record<string, string> }
  };

  beforeEach(async () => {
    queryParams$ = new BehaviorSubject<Record<string, string>>({});
    activatedRouteMock.queryParams = queryParams$.asObservable();
    activatedRouteMock.snapshot.queryParams = queryParams$.value;

    const nvmeofStateServiceMock = {
      refresh$: new Subject<void>(),
      requestRefresh: jest.fn()
    };

    await TestBed.configureTestingModule({
      declarations: [NvmeofSubsystemsComponent, NvmeofSubsystemsDetailsComponent],
      imports: [
        HttpClientModule,
        RouterTestingModule,
        SharedModule,
        ComboBoxModule,
        GridModule,
        NvmeofGatewayGroupFilterComponent
      ],
      providers: [
        { provide: NvmeofService, useClass: MockNvmeOfService },
        { provide: AuthStorageService, useClass: MockAuthStorageService },
        { provide: ModalCdsService, useClass: MockModalService },
        { provide: TaskWrapperService, useClass: MockTaskWrapperService },
        { provide: ActivatedRoute, useValue: activatedRouteMock },
        { provide: NvmeofStateService, useValue: nvmeofStateServiceMock }
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
    nvmeofService = TestBed.inject(NvmeofService) as any;
    component.ngOnInit();
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should retrieve subsystems', (done) => {
    const expected = mockSubsystems.map((s) => ({
      ...s,
      gw_group: 'default',
      auth: 'No authentication',
      initiator_count: 0
    }));
    component.onGroupChange('default');
    component.subsystems$.pipe(skip(1), take(1)).subscribe((subsystems) => {
      expect(subsystems).toEqual(expected);
      done();
    });
    component.fetchData();
  });

  it('should not fetch subsystems when group is not selected', (done) => {
    const listSubsystemsSpy = jest.spyOn(nvmeofService, 'listSubsystems');
    component.group = null;
    component.fetchData();

    component.subsystems$.pipe(take(1)).subscribe((subsystems) => {
      expect(subsystems).toEqual([]);
      expect(listSubsystemsSpy).not.toHaveBeenCalled();
      done();
    });
  });

  it('should set first group as default initially', () => {
    expect(component.group).toBe('default');
  });
});
