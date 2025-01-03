import { HttpHeaders } from '@angular/common/http';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastrModule } from 'ngx-toastr';
import { of } from 'rxjs';

import { CephModule } from '~/app/ceph/ceph.module';
import { CoreModule } from '~/app/core/core.module';
import { CephServiceService } from '~/app/shared/api/ceph-service.service';
import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
import { PaginateObservable } from '~/app/shared/api/paginate.model';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { Permissions } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { ServicesComponent } from './services.component';

describe('ServicesComponent', () => {
  let component: ServicesComponent;
  let fixture: ComponentFixture<ServicesComponent>;
  let headers: HttpHeaders;

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
        size: 3,
        running: 3,
        last_refresh: '2020-02-25T04:33:26.465699'
      }
    },
    {
      service_type: 'crash',
      service_name: 'crash',
      status: {
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
      RouterTestingModule,
      ToastrModule.forRoot()
    ],
    providers: [{ provide: AuthStorageService, useValue: fakeAuthStorageService }]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ServicesComponent);
    component = fixture.componentInstance;
    const orchService = TestBed.inject(OrchestratorService);
    const cephServiceService = TestBed.inject(CephServiceService);
    spyOn(orchService, 'status').and.returnValue(of({ available: true }));
    headers = new HttpHeaders().set('X-Total-Count', '2');
    const paginate_obs = new PaginateObservable<any>(of({ body: services, headers: headers }));

    spyOn(cephServiceService, 'list').and.returnValue(paginate_obs);
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should have columns that are sortable', () => {
    expect(
      component.columns
        // Filter the 'Expand/Collapse Row' column.
        .filter((column) => !(column.cellClass === 'cd-datatable-expand-collapse'))
        // Filter the 'Placement' column.
        .filter((column) => !(column.prop === ''))
        .every((column) => Boolean(column.prop))
    ).toBeTruthy();
  });

  it('should return all services', () => {
    const context = new CdTableFetchDataContext(() => undefined);
    context.pageInfo.offset = 0;
    context.pageInfo.limit = -1;
    component.getServices(context);
    expect(component.services.length).toBe(2);
  });

  it('should not display doc panel if orchestrator is available', () => {
    expect(component.showDocPanel).toBeFalsy();
  });
});
