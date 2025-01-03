import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { ActivatedRoute } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastrModule } from 'ngx-toastr';
import { of } from 'rxjs';

import { RbdMirroringService } from '~/app/shared/api/rbd-mirroring.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { SharedModule } from '~/app/shared/shared.module';
import { ActivatedRouteStub } from '~/testing/activated-route-stub';
import { configureTestBed, FormHelper } from '~/testing/unit-test-helper';
import { PoolEditModeModalComponent } from './pool-edit-mode-modal.component';
import { ModalModule, SelectModule } from 'carbon-components-angular';

describe('PoolEditModeModalComponent', () => {
  let component: PoolEditModeModalComponent;
  let fixture: ComponentFixture<PoolEditModeModalComponent>;
  let notificationService: NotificationService;
  let rbdMirroringService: RbdMirroringService;
  let formHelper: FormHelper;
  let activatedRoute: ActivatedRouteStub;

  configureTestBed({
    declarations: [PoolEditModeModalComponent],
    imports: [
      HttpClientTestingModule,
      ReactiveFormsModule,
      RouterTestingModule,
      SharedModule,
      ToastrModule.forRoot(),
      ModalModule,
      SelectModule
    ],
    providers: [
      {
        provide: ActivatedRoute,
        useValue: new ActivatedRouteStub({ pool_name: 'somePool' })
      }
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(PoolEditModeModalComponent);
    component = fixture.componentInstance;
    component.poolName = 'somePool';

    notificationService = TestBed.inject(NotificationService);
    spyOn(notificationService, 'show').and.stub();

    rbdMirroringService = TestBed.inject(RbdMirroringService);
    activatedRoute = <ActivatedRouteStub>TestBed.inject(ActivatedRoute);

    formHelper = new FormHelper(component.editModeForm);
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('update pool mode', () => {
    beforeEach(() => {
      spyOn(component, 'closeModal').and.callThrough();
    });

    it('should call updatePool', () => {
      activatedRoute.setParams({ pool_name: 'somePool' });
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
