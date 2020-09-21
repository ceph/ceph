import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { ToastrModule } from 'ngx-toastr';
import { of } from 'rxjs';

import { configureTestBed, FormHelper } from '../../../../../testing/unit-test-helper';
import { RbdMirroringService } from '../../../../shared/api/rbd-mirroring.service';
import { NotificationService } from '../../../../shared/services/notification.service';
import { SharedModule } from '../../../../shared/shared.module';
import { BootstrapCreateModalComponent } from './bootstrap-create-modal.component';

describe('BootstrapCreateModalComponent', () => {
  let component: BootstrapCreateModalComponent;
  let fixture: ComponentFixture<BootstrapCreateModalComponent>;
  let notificationService: NotificationService;
  let rbdMirroringService: RbdMirroringService;
  let formHelper: FormHelper;

  configureTestBed({
    declarations: [BootstrapCreateModalComponent],
    imports: [
      HttpClientTestingModule,
      ReactiveFormsModule,
      RouterTestingModule,
      SharedModule,
      ToastrModule.forRoot()
    ],
    providers: [NgbActiveModal]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(BootstrapCreateModalComponent);
    component = fixture.componentInstance;
    component.siteName = 'site-A';

    notificationService = TestBed.inject(NotificationService);
    spyOn(notificationService, 'show').and.stub();

    rbdMirroringService = TestBed.inject(RbdMirroringService);

    formHelper = new FormHelper(component.createBootstrapForm);

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

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('generate token', () => {
    beforeEach(() => {
      spyOn(rbdMirroringService, 'refresh').and.stub();
      spyOn(component.activeModal, 'close').and.callThrough();
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
      spyOn(rbdMirroringService, 'createBootstrapToken').and.callFake(() => of({ token: 'token' }));

      component.createBootstrapForm.patchValue({
        siteName: 'new-site-A',
        pools: { pool1: true, pool3: true }
      });
      component.generate();
      expect(rbdMirroringService.setSiteName).toHaveBeenCalledWith('new-site-A');
      expect(rbdMirroringService.updatePool).toHaveBeenCalledWith('pool1', {
        mirror_mode: 'image'
      });
      expect(rbdMirroringService.updatePool).toHaveBeenCalledWith('pool3', {
        mirror_mode: 'image'
      });
      expect(rbdMirroringService.createBootstrapToken).toHaveBeenCalledWith('pool3');
      expect(component.createBootstrapForm.getValue('token')).toBe('token');
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
      formHelper.expectError(component.createBootstrapForm.get('pools'), 'requirePool');
    });
  });
});
