import { ComponentFixture, TestBed, fakeAsync, tick } from '@angular/core/testing';
import { HttpClientModule } from '@angular/common/http';
import { of } from 'rxjs';
import { RouterTestingModule } from '@angular/router/testing';
import { SharedModule } from '~/app/shared/shared.module';

import { NvmeofService } from '../../../shared/api/nvmeof.service';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalService } from '~/app/shared/services/modal.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { NvmeofSubsystemsComponent } from './nvmeof-subsystems.component';
import { NvmeofTabsComponent } from '../nvmeof-tabs/nvmeof-tabs.component';
import { NvmeofSubsystemsDetailsComponent } from '../nvmeof-subsystems-details/nvmeof-subsystems-details.component';
import { ComboBoxModule, GridModule } from 'carbon-components-angular';
import { CephServiceSpec } from '~/app/shared/models/service.interface';

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
      service_name: 'nvmeof.rbd.default',
      service_type: 'nvmeof',
      unmanaged: false,
      spec: {
        group: 'default'
      }
    },
    {
      service_name: 'nvmeof.rbd.foo',
      service_type: 'nvmeof',
      unmanaged: false,
      spec: {
        group: 'foo'
      }
    }
  ],
  2
];

const mockformattedGwGroups = [
  {
    content: 'default'
  },
  {
    content: 'foo'
  }
];

class MockNvmeOfService {
  listSubsystems() {
    return of(mockSubsystems);
  }

  formatGwGroupsList(_data: CephServiceSpec[][]) {
    return mockformattedGwGroups;
  }

  listGatewayGroups() {
    return of(mockGroups);
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

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [
        NvmeofSubsystemsComponent,
        NvmeofTabsComponent,
        NvmeofSubsystemsDetailsComponent
      ],
      imports: [HttpClientModule, RouterTestingModule, SharedModule, ComboBoxModule, GridModule],
      providers: [
        { provide: NvmeofService, useClass: MockNvmeOfService },
        { provide: AuthStorageService, useClass: MockAuthStorageService },
        { provide: ModalService, useClass: MockModalService },
        { provide: TaskWrapperService, useClass: MockTaskWrapperService }
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(NvmeofSubsystemsComponent);
    component = fixture.componentInstance;
    component.ngOnInit();
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should retrieve subsystems', fakeAsync(() => {
    component.getSubsystems();
    tick();
    expect(component.subsystems).toEqual(mockSubsystems);
  }));

  it('should load gateway groups correctly', () => {
    expect(component.gwGroups.length).toBe(2);
  });

  it('should set first group as default initially', () => {
    expect(component.group).toBe(mockGroups[0][0].spec.group);
  });
});
