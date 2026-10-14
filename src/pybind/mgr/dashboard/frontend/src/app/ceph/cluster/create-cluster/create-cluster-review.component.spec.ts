import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import _ from 'lodash';
import { of } from 'rxjs';

import { CephModule } from '~/app/ceph/ceph.module';
import { CoreModule } from '~/app/core/core.module';
import { OsdService } from '~/app/shared/api/osd.service';
import { Permissions } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { CreateClusterReviewComponent } from './create-cluster-review.component';

describe('CreateClusterReviewComponent', () => {
  let component: CreateClusterReviewComponent;
  let fixture: ComponentFixture<CreateClusterReviewComponent>;

  configureTestBed({
    imports: [HttpClientTestingModule, SharedModule, CephModule, CoreModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CreateClusterReviewComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('getDeploymentOptions permission guard', () => {
    let osdService: OsdService;
    let authStorageService: AuthStorageService;

    beforeEach(() => {
      osdService = TestBed.inject(OsdService);
      authStorageService = TestBed.inject(AuthStorageService);
      spyOn(osdService, 'getDeploymentOptions').and.returnValue(of({} as any));
      component.isSimpleDeployment = true;
    });

    it('should not call getDeploymentOptions when osd.read is false', () => {
      spyOn(authStorageService, 'getPermissions').and.returnValue(new Permissions({ osd: [] }));
      fixture.detectChanges();
      expect(osdService.getDeploymentOptions).not.toHaveBeenCalled();
    });

    it('should call getDeploymentOptions when osd.read is true', () => {
      spyOn(authStorageService, 'getPermissions').and.returnValue(
        new Permissions({ osd: ['read'] })
      );
      fixture.detectChanges();
      expect(osdService.getDeploymentOptions).toHaveBeenCalled();
    });
  });
});
