import { ComponentFixture, TestBed, fakeAsync, tick } from '@angular/core/testing';
import { HttpClientModule } from '@angular/common/http';
import { of } from 'rxjs';
import { RouterTestingModule } from '@angular/router/testing';
import { SharedModule } from '~/app/shared/shared.module';

import { NvmeofService } from '../../../shared/api/nvmeof.service';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalService } from '~/app/shared/services/modal.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { NvmeofTabsComponent } from '../nvmeof-tabs/nvmeof-tabs.component';
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
  listNamespaces() {
    return of(mockNamespaces);
  }
}

class MockAuthStorageService {
  getPermissions() {
    return { nvmeof: {} };
  }
}

class MockModalService {}

class MockTaskWrapperService {}

describe('NvmeofNamespacesListComponent', () => {
  let component: NvmeofNamespacesListComponent;
  let fixture: ComponentFixture<NvmeofNamespacesListComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [
        NvmeofNamespacesListComponent,
        NvmeofTabsComponent,
        NvmeofSubsystemsDetailsComponent
      ],
      imports: [HttpClientModule, RouterTestingModule, SharedModule],
      providers: [
        { provide: NvmeofService, useClass: MockNvmeOfService },
        { provide: AuthStorageService, useClass: MockAuthStorageService },
        { provide: ModalService, useClass: MockModalService },
        { provide: TaskWrapperService, useClass: MockTaskWrapperService }
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(NvmeofNamespacesListComponent);
    component = fixture.componentInstance;
    component.ngOnInit();
    component.subsystemNQN = 'nqn.2001-07.com.ceph:1721040751436';
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should retrieve namespaces', fakeAsync(() => {
    component.listNamespaces();
    tick();
    expect(component.namespaces).toEqual(mockNamespaces);
  }));
});
