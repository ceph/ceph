import { ComponentFixture, TestBed, fakeAsync, tick } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { ActivatedRoute } from '@angular/router';
import { of } from 'rxjs';

import { NvmeofSubsystemNamespacesListComponent } from './nvmeof-subsystem-namespaces-list.component';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { SharedModule } from '~/app/shared/shared.module';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';

describe('NvmeofSubsystemNamespacesListComponent', () => {
  let component: NvmeofSubsystemNamespacesListComponent;
  let fixture: ComponentFixture<NvmeofSubsystemNamespacesListComponent>;
  let nvmeofService: NvmeofService;

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
            listNamespaces: jest.fn().mockReturnValue(of(mockNamespaces))
          }
        },
        { provide: AuthStorageService, useClass: MockAuthStorageService }
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
});
