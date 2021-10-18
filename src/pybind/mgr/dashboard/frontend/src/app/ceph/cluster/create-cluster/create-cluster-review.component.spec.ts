import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import _ from 'lodash';
import { ToastrModule } from 'ngx-toastr';
import { of } from 'rxjs';

import { CephModule } from '~/app/ceph/ceph.module';
import { CoreModule } from '~/app/core/core.module';
import { CephServiceService } from '~/app/shared/api/ceph-service.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { CreateClusterReviewComponent } from './create-cluster-review.component';

describe('CreateClusterReviewComponent', () => {
  let component: CreateClusterReviewComponent;
  let fixture: ComponentFixture<CreateClusterReviewComponent>;
  let cephServiceService: CephServiceService;
  let serviceListSpy: jasmine.Spy;

  configureTestBed({
    imports: [HttpClientTestingModule, SharedModule, ToastrModule.forRoot(), CephModule, CoreModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CreateClusterReviewComponent);
    component = fixture.componentInstance;
    cephServiceService = TestBed.inject(CephServiceService);
    serviceListSpy = spyOn(cephServiceService, 'list');
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should verify host metadata calculations', () => {
    const hostnames = ['ceph.test1', 'ceph.test2'];
    const payload = [
      {
        hostname: hostnames[0],
        service_type: ['mgr', 'mon']
      },
      {
        hostname: hostnames[1],
        service_type: ['mgr', 'alertmanager']
      }
    ];
    serviceListSpy.and.callFake(() => of(payload));
    fixture.detectChanges();
    expect(serviceListSpy).toHaveBeenCalled();

    expect(component.serviceCount).toBe(2);
    expect(component.uniqueServices.size).toBe(2);
  });
});
