import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { of } from 'rxjs';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { CoreModule } from '../../../core/core.module';
import { CephServiceService } from '../../../shared/api/ceph-service.service';
import { OrchestratorService } from '../../../shared/api/orchestrator.service';
import { CdTableFetchDataContext } from '../../../shared/models/cd-table-fetch-data-context';
import { Permissions } from '../../../shared/models/permissions';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { SharedModule } from '../../../shared/shared.module';
import { CephModule } from '../../ceph.module';
import { ServicesComponent } from './services.component';

describe('ServicesComponent', () => {
  let component: ServicesComponent;
  let fixture: ComponentFixture<ServicesComponent>;

  const fakeAuthStorageService = {
    getPermissions: () => {
      return new Permissions({ hosts: ['read'] });
    }
  };

  const services = [
    {
      service_type: 'osd',
      service_name: 'osd',
      status: {
        container_image_id: 'e70344c77bcbf3ee389b9bf5128f635cf95f3d59e005c5d8e67fc19bcc74ed23',
        container_image_name: 'docker.io/ceph/daemon-base:latest-master-devel',
        size: 3,
        running: 3,
        last_refresh: '2020-02-25T04:33:26.465699'
      }
    },
    {
      service_type: 'crash',
      service_name: 'crash',
      status: {
        container_image_id: 'e70344c77bcbf3ee389b9bf5128f635cf95f3d59e005c5d8e67fc19bcc74ed23',
        container_image_name: 'docker.io/ceph/daemon-base:latest-master-devel',
        size: 1,
        running: 1,
        last_refresh: '2020-02-25T04:33:26.465766'
      }
    }
  ];

  configureTestBed({
    imports: [
      BrowserAnimationsModule,
      CephModule,
      CoreModule,
      SharedModule,
      HttpClientTestingModule,
      RouterTestingModule
    ],
    providers: [{ provide: AuthStorageService, useValue: fakeAuthStorageService }]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ServicesComponent);
    component = fixture.componentInstance;
    const orchService = TestBed.inject(OrchestratorService);
    const cephServiceService = TestBed.inject(CephServiceService);
    spyOn(orchService, 'status').and.returnValue(of({ available: true }));
    spyOn(cephServiceService, 'list').and.returnValue(of(services));
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should have columns that are sortable', () => {
    expect(
      component.columns
        .filter((column) => !(column.cellClass === 'cd-datatable-expand-collapse'))
        .every((column) => Boolean(column.prop))
    ).toBeTruthy();
  });

  it('should return all services', () => {
    component.getServices(new CdTableFetchDataContext(() => undefined));
    expect(component.services.length).toBe(2);
  });
});
