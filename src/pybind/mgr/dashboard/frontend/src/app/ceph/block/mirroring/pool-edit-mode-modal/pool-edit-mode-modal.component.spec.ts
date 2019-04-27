import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastModule } from 'ng2-toastr';
import { BsModalRef, BsModalService } from 'ngx-bootstrap/modal';
import { of } from 'rxjs';

import {
  configureTestBed,
  FormHelper,
  i18nProviders
} from '../../../../../testing/unit-test-helper';
import { RbdMirroringService } from '../../../../shared/api/rbd-mirroring.service';
import { NotificationService } from '../../../../shared/services/notification.service';
import { SharedModule } from '../../../../shared/shared.module';
import { PoolEditModeModalComponent } from './pool-edit-mode-modal.component';

describe('PoolEditModeModalComponent', () => {
  let component: PoolEditModeModalComponent;
  let fixture: ComponentFixture<PoolEditModeModalComponent>;
  let notificationService: NotificationService;
  let rbdMirroringService: RbdMirroringService;
  let formHelper: FormHelper;

  configureTestBed({
    declarations: [PoolEditModeModalComponent],
    imports: [
      HttpClientTestingModule,
      ReactiveFormsModule,
      RouterTestingModule,
      SharedModule,
      ToastModule.forRoot()
    ],
    providers: [BsModalRef, BsModalService, i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(PoolEditModeModalComponent);
    component = fixture.componentInstance;
    component.poolName = 'somePool';

    notificationService = TestBed.get(NotificationService);
    spyOn(notificationService, 'show').and.stub();

    rbdMirroringService = TestBed.get(RbdMirroringService);

    formHelper = new FormHelper(component.editModeForm);
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('update pool mode', () => {
    beforeEach(() => {
      spyOn(component.modalRef, 'hide').and.callThrough();
    });

    afterEach(() => {
      expect(component.modalRef.hide).toHaveBeenCalledTimes(1);
    });

    it('should call updatePool', () => {
      spyOn(rbdMirroringService, 'updatePool').and.callFake(() => of(''));

      component.editModeForm.patchValue({ mirrorMode: 'disabled' });
      component.update();
      expect(rbdMirroringService.updatePool).toHaveBeenCalledWith('somePool', {
        mirror_mode: 'disabled'
      });
    });
  });

  describe('form validation', () => {
    it('should prevent disabling mirroring if peers exist', () => {
      component.peerExists = true;
      formHelper.expectErrorChange('mirrorMode', 'disabled', 'cannotDisable');
    });
  });
});
