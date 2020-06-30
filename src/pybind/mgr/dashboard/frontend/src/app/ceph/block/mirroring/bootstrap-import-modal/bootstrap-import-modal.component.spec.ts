import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { BsModalRef, BsModalService } from 'ngx-bootstrap/modal';
import { ToastrModule } from 'ngx-toastr';
import { of } from 'rxjs';

import {
  configureTestBed,
  FormHelper,
  i18nProviders
} from '../../../../../testing/unit-test-helper';
import { RbdMirroringService } from '../../../../shared/api/rbd-mirroring.service';
import { NotificationService } from '../../../../shared/services/notification.service';
import { SharedModule } from '../../../../shared/shared.module';
import { BootstrapImportModalComponent } from './bootstrap-import-modal.component';

describe('BootstrapImportModalComponent', () => {
  let component: BootstrapImportModalComponent;
  let fixture: ComponentFixture<BootstrapImportModalComponent>;
  let notificationService: NotificationService;
  let rbdMirroringService: RbdMirroringService;
  let formHelper: FormHelper;

  configureTestBed({
    declarations: [BootstrapImportModalComponent],
    imports: [
      HttpClientTestingModule,
      ReactiveFormsModule,
      RouterTestingModule,
      SharedModule,
      ToastrModule.forRoot()
    ],
    providers: [BsModalRef, BsModalService, i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(BootstrapImportModalComponent);
    component = fixture.componentInstance;
    component.siteName = 'site-A';

    notificationService = TestBed.inject(NotificationService);
    spyOn(notificationService, 'show').and.stub();

    rbdMirroringService = TestBed.inject(RbdMirroringService);

    formHelper = new FormHelper(component.importBootstrapForm);

    spyOn(rbdMirroringService, 'getSiteName').and.callFake(() => of({ site_name: 'site-A' }));
    spyOn(rbdMirroringService, 'subscribeSummary').and.callFake((call) =>
      of({
        content_data: {
          pools: [
            { name: 'pool1', mirror_mode: 'disabled' },
            { name: 'pool2', mirror_mode: 'disabled' },
            { name: 'pool3', mirror_mode: 'disabled' }
          ]
        }
      }).subscribe(call)
    );
  });

  it('should import', () => {
    expect(component).toBeTruthy();
  });

  describe('import token', () => {
    beforeEach(() => {
      spyOn(rbdMirroringService, 'refresh').and.stub();
      spyOn(component.modalRef, 'hide').and.callThrough();
      fixture.detectChanges();
    });

    afterEach(() => {
      expect(rbdMirroringService.getSiteName).toHaveBeenCalledTimes(1);
      expect(rbdMirroringService.subscribeSummary).toHaveBeenCalledTimes(1);
      expect(rbdMirroringService.refresh).toHaveBeenCalledTimes(1);
    });

    it('should generate a bootstrap token', () => {
      spyOn(rbdMirroringService, 'setSiteName').and.callFake(() => of({ site_name: 'new-site-A' }));
      spyOn(rbdMirroringService, 'updatePool').and.callFake(() => of({}));
      spyOn(rbdMirroringService, 'importBootstrapToken').and.callFake(() => of({ token: 'token' }));

      component.importBootstrapForm.patchValue({
        siteName: 'new-site-A',
        pools: { pool1: true, pool3: true },
        token: 'e30='
      });
      component.import();
      expect(rbdMirroringService.setSiteName).toHaveBeenCalledWith('new-site-A');
      expect(rbdMirroringService.updatePool).toHaveBeenCalledWith('pool1', {
        mirror_mode: 'image'
      });
      expect(rbdMirroringService.updatePool).toHaveBeenCalledWith('pool3', {
        mirror_mode: 'image'
      });
      expect(rbdMirroringService.importBootstrapToken).toHaveBeenCalledWith(
        'pool1',
        'rx-tx',
        'e30='
      );
      expect(rbdMirroringService.importBootstrapToken).toHaveBeenCalledWith(
        'pool3',
        'rx-tx',
        'e30='
      );
    });
  });

  describe('form validation', () => {
    beforeEach(() => {
      fixture.detectChanges();
    });

    it('should require a site name', () => {
      formHelper.expectErrorChange('siteName', '', 'required');
    });

    it('should require at least one pool', () => {
      formHelper.expectError(component.importBootstrapForm.get('pools'), 'requirePool');
    });

    it('should require a token', () => {
      formHelper.expectErrorChange('token', '', 'required');
    });

    it('should verify token is base64-encoded JSON', () => {
      formHelper.expectErrorChange('token', 'VEVTVA==', 'invalidToken');
      formHelper.expectErrorChange('token', 'e2RmYXNqZGZrbH0=', 'invalidToken');
    });
  });
});
