import { ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { SmbClusterFormComponent } from './smb-cluster-form.component';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { SharedModule } from '~/app/shared/shared.module';
import { RouterTestingModule } from '@angular/router/testing';
import { FormArray, ReactiveFormsModule, Validators } from '@angular/forms';

import { ComboBoxModule, GridModule, InputModule, SelectModule } from 'carbon-components-angular';
import { AUTHMODE, CLIENT_COMPAT, PERSISTENT_HANDLES_DURABILITY, SMBCluster } from '../smb.model';
import { FOO_USERSGROUPS } from '../smb-usersgroups-form/smb-usersgroups-form.component.spec';
import { of } from 'rxjs';
import { By } from '@angular/platform-browser';
import { USER } from '~/app/shared/constants/app.constants';
import { SMB_CEPHFS_CONTEXT, SMB_CONTEXT } from '~/app/shared/constants/smb.constant';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';

describe('SmbClusterFormComponent', () => {
  let component: SmbClusterFormComponent;
  let fixture: ComponentFixture<SmbClusterFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [
        BrowserAnimationsModule,
        SharedModule,
        HttpClientTestingModule,
        RouterTestingModule,
        ReactiveFormsModule,
        GridModule,
        InputModule,
        SelectModule,
        ComboBoxModule
      ],
      declarations: [SmbClusterFormComponent],
      providers: [{ provide: SMB_CONTEXT, useValue: SMB_CEPHFS_CONTEXT }]
    }).compileComponents();

    fixture = TestBed.createComponent(SmbClusterFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should have cluster_id and domain_settings as required fields', () => {
    fixture.detectChanges();

    const clusterIdControl = component.smbForm.get('cluster_id');
    const domainSettingsControl = component.smbForm.get('domain_settings');

    const isClusterId = [clusterIdControl.validator].includes(Validators.required);
    const isDomainSettings = [domainSettingsControl.validator].includes(Validators.required);

    expect(isClusterId).toBe(false);
    expect(isDomainSettings).toBe(true);
  });

  it('should add and remove user group settings', () => {
    const defaultLength = component.joinSources.length;
    component.addUserGroupSetting();
    expect(component.joinSources.length).toBe(defaultLength + 1);
    component.removeUserGroupSetting(0);
    expect(component.joinSources.length).toBe(defaultLength);
  });

  it('should add and remove custom dns settings (custom_dns)', () => {
    const defaultLength = component.custom_dns.length;
    component.addCustomDns();
    expect(component.custom_dns.length).toBe(defaultLength + 1);
    component.removeCustomDNS(0);
    expect(component.custom_dns.length).toBe(defaultLength);
  });

  it('should change the form when authmode is changed', () => {
    const authModeControl = component.smbForm.get('auth_mode');
    authModeControl?.setValue(USER);
    component.onAuthModeChange();
    fixture.detectChanges();
    const joinSourcesControl = component.smbForm.get('joinSources') as FormArray;
    expect(joinSourcesControl.length).toBe(1);
  });

  it('should check submit request', () => {
    component.smbForm.get('auth_mode').setValue(AUTHMODE.ActiveDirectory);
    component.smbForm.get('domain_settings').setValue('test-realm');
    component.smbForm.get('cluster_id').setValue('cluster-id');
    component.submitAction();
  });

  it('should have client_compat control defaulting to false', () => {
    const clientCompatControl = component.smbForm.get('client_compat');
    expect(clientCompatControl).toBeTruthy();
    expect(clientCompatControl.value).toBe(false);
  });

  it('should include client_compat macos in request when toggled on', () => {
    component.smbForm.get('auth_mode').setValue(AUTHMODE.ActiveDirectory);
    component.smbForm.get('domain_settings').setValue('test-realm');
    component.smbForm.get('cluster_id').setValue('cluster-id');
    component.smbForm.get('client_compat').setValue(true);
    const request = (component as any).buildRequest();
    expect(request.cluster_resource.client_compat).toBe(CLIENT_COMPAT.MacOS);
  });

  it('should include client_compat default in request when toggled off', () => {
    component.smbForm.get('auth_mode').setValue(AUTHMODE.ActiveDirectory);
    component.smbForm.get('domain_settings').setValue('test-realm');
    component.smbForm.get('cluster_id').setValue('cluster-id');
    component.smbForm.get('client_compat').setValue(false);
    const request = (component as any).buildRequest();
    expect(request.cluster_resource.client_compat).toBe(CLIENT_COMPAT.Default);
  });

  it('should delete domain', () => {
    component.deleteDomainSettingsModal();
    expect(component).toBeTruthy();
  });

  it('should default persistent_handles to false and durability to full_outage', () => {
    expect(component.smbForm.get('persistent_handles').value).toBe(false);
    expect(component.smbForm.get('persistent_handles_durability').value).toBe(
      PERSISTENT_HANDLES_DURABILITY.FullOutage
    );
  });

  it('should map persistent_handles true and durability to buildRequest', () => {
    component.smbForm.get('cluster_id').setValue('test-cluster');
    component.smbForm.get('auth_mode').setValue(AUTHMODE.ActiveDirectory);
    component.smbForm.get('domain_settings').setValue('test-realm');
    component.smbForm.get('persistent_handles').setValue(true);
    component.smbForm
      .get('persistent_handles_durability')
      .setValue(PERSISTENT_HANDLES_DURABILITY.PartialOutage);
    const request = (component as any).buildRequest();
    expect(request.cluster_resource.persistent_handles).toBeUndefined();
    expect(request.cluster_resource.share_defaults.availability.persistent_handles).toBe(true);
    expect(request.cluster_resource.persistent_handles_durability).toBe(
      PERSISTENT_HANDLES_DURABILITY.PartialOutage
    );
  });

  it('should omit share_defaults and durability on create when persistent_handles is false', () => {
    component.smbForm.get('cluster_id').setValue('test-cluster');
    component.smbForm.get('auth_mode').setValue(AUTHMODE.ActiveDirectory);
    component.smbForm.get('domain_settings').setValue('test-realm');
    component.smbForm.get('persistent_handles').setValue(false);
    const request = (component as any).buildRequest();
    expect(request.cluster_resource.persistent_handles).toBeUndefined();
    expect(request.cluster_resource.share_defaults).toBeUndefined();
    expect(request.cluster_resource.persistent_handles_durability).toBeUndefined();
  });

  it('should not include persistent_handles in buildRequest when isRgw is true', () => {
    (component as any).isRgw = true;
    component.smbForm.get('cluster_id').setValue('test-cluster');
    component.smbForm.get('auth_mode').setValue(AUTHMODE.ActiveDirectory);
    component.smbForm.get('domain_settings').setValue('test-realm');
    component.smbForm.get('persistent_handles').setValue(true);
    const request = (component as any).buildRequest();
    expect(request.cluster_resource.share_defaults).toBeUndefined();
    expect(request.cluster_resource.persistent_handles_durability).toBeUndefined();
  });

  describe('editing persistent handles', () => {
    beforeEach(() => {
      component.isEdit = true;
      component.clusterResponse = {
        cluster_id: 'test-cluster',
        resource_type: 'ceph.smb.cluster',
        auth_mode: AUTHMODE.ActiveDirectory,
        persistent_handles_durability: PERSISTENT_HANDLES_DURABILITY.PartialOutage,
        share_defaults: {
          acl_support: 'posix',
          availability: { persistent_handles: true }
        }
      } as SMBCluster;
      component.smbForm.get('cluster_id').setValue('test-cluster');
      component.smbForm.get('auth_mode').setValue(AUTHMODE.ActiveDirectory);
      component.smbForm.get('domain_settings').setValue('test-realm');
      component.smbForm
        .get('persistent_handles_durability')
        .setValue(PERSISTENT_HANDLES_DURABILITY.PartialOutage);
    });

    it('should keep durability and unrelated share_defaults when persistent handles stay on', () => {
      component.smbForm.get('persistent_handles').setValue(true);
      const request = (component as any).buildRequest();
      expect(request.cluster_resource.persistent_handles_durability).toBe(
        PERSISTENT_HANDLES_DURABILITY.PartialOutage
      );
      expect(request.cluster_resource.share_defaults).toEqual({
        acl_support: 'posix',
        availability: { persistent_handles: true }
      });
    });

    it('should keep durability and acl_support but drop the availability default when disabling', () => {
      component.smbForm.get('persistent_handles').setValue(false);
      const request = (component as any).buildRequest();
      expect(request.cluster_resource.persistent_handles_durability).toBe(
        PERSISTENT_HANDLES_DURABILITY.PartialOutage
      );
      expect(request.cluster_resource.share_defaults).toEqual({ acl_support: 'posix' });
    });

    it('should keep the durability control editable', () => {
      expect(component.smbForm.get('persistent_handles_durability').disabled).toBe(false);
    });

    it('should ask for confirmation before disabling the cluster default', () => {
      const modalService = TestBed.inject(ModalCdsService);
      const showSpy = jest.spyOn(modalService, 'show').mockImplementation();
      const requestSpy = jest.spyOn(component, 'handleTaskRequest').mockImplementation();
      component.smbForm.get('persistent_handles').setValue(false);
      component.submitAction();
      expect(showSpy).toHaveBeenCalled();
      expect(requestSpy).not.toHaveBeenCalled();
    });

    it('should not ask for confirmation when persistent handles stay enabled', () => {
      const modalService = TestBed.inject(ModalCdsService);
      const showSpy = jest.spyOn(modalService, 'show').mockImplementation();
      const requestSpy = jest.spyOn(component, 'handleTaskRequest').mockImplementation();
      component.smbForm.get('persistent_handles').setValue(true);
      component.submitAction();
      expect(showSpy).not.toHaveBeenCalled();
      expect(requestSpy).toHaveBeenCalledWith('edit');
    });
  });

  it('should get usersgroups resources on user authmode', () => {
    component.smbForm.get('auth_mode').setValue(AUTHMODE.User);
    component.usersGroups$ = of([FOO_USERSGROUPS]);
    fixture.whenStable().then(() => {
      const options = fixture.debugElement.queryAll(By.css('select option'));

      expect(options.length).toBe(1);
      expect(options[0].nativeElement.value).toBe('foo');
      expect(options[0].nativeElement.textContent).toBe('foo');
    });
  });
});
