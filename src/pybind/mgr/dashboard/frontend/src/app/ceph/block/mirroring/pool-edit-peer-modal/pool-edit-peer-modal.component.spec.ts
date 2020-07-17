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
import { PoolEditPeerModalComponent } from './pool-edit-peer-modal.component';
import { PoolEditPeerResponseModel } from './pool-edit-peer-response.model';

describe('PoolEditPeerModalComponent', () => {
  let component: PoolEditPeerModalComponent;
  let fixture: ComponentFixture<PoolEditPeerModalComponent>;
  let notificationService: NotificationService;
  let rbdMirroringService: RbdMirroringService;
  let formHelper: FormHelper;

  configureTestBed({
    declarations: [PoolEditPeerModalComponent],
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
    fixture = TestBed.createComponent(PoolEditPeerModalComponent);
    component = fixture.componentInstance;
    component.mode = 'add';
    component.poolName = 'somePool';

    notificationService = TestBed.inject(NotificationService);
    spyOn(notificationService, 'show').and.stub();

    rbdMirroringService = TestBed.inject(RbdMirroringService);

    formHelper = new FormHelper(component.editPeerForm);
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('add pool peer', () => {
    beforeEach(() => {
      component.mode = 'add';
      component.peerUUID = undefined;
      spyOn(rbdMirroringService, 'refresh').and.stub();
      spyOn(component.activeModal, 'close').and.callThrough();
      fixture.detectChanges();
    });

    afterEach(() => {
      expect(rbdMirroringService.refresh).toHaveBeenCalledTimes(1);
      expect(component.activeModal.close).toHaveBeenCalledTimes(1);
    });

    it('should call addPeer', () => {
      spyOn(rbdMirroringService, 'addPeer').and.callFake(() => of(''));

      component.editPeerForm.patchValue({
        clusterName: 'cluster',
        clientID: 'id',
        monAddr: 'mon_host',
        key: 'dGVzdA=='
      });

      component.update();
      expect(rbdMirroringService.addPeer).toHaveBeenCalledWith('somePool', {
        cluster_name: 'cluster',
        client_id: 'id',
        mon_host: 'mon_host',
        key: 'dGVzdA=='
      });
    });
  });

  describe('edit pool peer', () => {
    beforeEach(() => {
      component.mode = 'edit';
      component.peerUUID = 'somePeer';

      const response = new PoolEditPeerResponseModel();
      response.uuid = 'somePeer';
      response.cluster_name = 'cluster';
      response.client_id = 'id';
      response.mon_host = '1.2.3.4:1234';
      response.key = 'dGVzdA==';

      spyOn(rbdMirroringService, 'getPeer').and.callFake(() => of(response));
      spyOn(rbdMirroringService, 'refresh').and.stub();
      spyOn(component.activeModal, 'close').and.callThrough();
      fixture.detectChanges();
    });

    afterEach(() => {
      expect(rbdMirroringService.getPeer).toHaveBeenCalledWith('somePool', 'somePeer');
      expect(rbdMirroringService.refresh).toHaveBeenCalledTimes(1);
      expect(component.activeModal.close).toHaveBeenCalledTimes(1);
    });

    it('should call updatePeer', () => {
      spyOn(rbdMirroringService, 'updatePeer').and.callFake(() => of(''));

      component.update();
      expect(rbdMirroringService.updatePeer).toHaveBeenCalledWith('somePool', 'somePeer', {
        cluster_name: 'cluster',
        client_id: 'id',
        mon_host: '1.2.3.4:1234',
        key: 'dGVzdA=='
      });
    });
  });

  describe('form validation', () => {
    beforeEach(() => {
      fixture.detectChanges();
    });

    it('should validate cluster name', () => {
      formHelper.expectErrorChange('clusterName', '', 'required');
      formHelper.expectErrorChange('clusterName', ' ', 'invalidClusterName');
    });

    it('should validate client ID', () => {
      formHelper.expectErrorChange('clientID', '', 'required');
      formHelper.expectErrorChange('clientID', 'client.id', 'invalidClientID');
    });

    it('should validate monitor address', () => {
      formHelper.expectErrorChange('monAddr', '@', 'invalidMonAddr');
    });

    it('should validate key', () => {
      formHelper.expectErrorChange('key', '(', 'invalidKey');
    });
  });
});
