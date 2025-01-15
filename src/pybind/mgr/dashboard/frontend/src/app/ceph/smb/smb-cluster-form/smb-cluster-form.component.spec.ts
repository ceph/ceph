import { ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { SmbClusterFormComponent } from './smb-cluster-form.component';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { SharedModule } from '~/app/shared/shared.module';
import { RouterTestingModule } from '@angular/router/testing';
import { FormArray, ReactiveFormsModule, Validators } from '@angular/forms';
import { ToastrModule } from 'ngx-toastr';
import { ComboBoxModule, GridModule, InputModule, SelectModule } from 'carbon-components-angular';
import { AuthMode } from '../smb.model';

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
        ToastrModule.forRoot(),
        GridModule,
        InputModule,
        SelectModule,
        ComboBoxModule
      ],
      declarations: [SmbClusterFormComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(SmbClusterFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should be required field for cluster_id and domain_settings', () => {
    fixture.detectChanges();
    const clusterIdControl = component.smbForm.get('cluster_id');
    const domainSettingsControl = component.smbForm.get('domain_settings');
    const clusterValidator = clusterIdControl?.validator ? [clusterIdControl?.validator] : [];
    const domainSettingsValidator = domainSettingsControl?.validator
      ? [domainSettingsControl?.validator]
      : [];
    const isClusterId = clusterValidator.some(
      (validator: any) => validator === Validators.required
    );
    const isDomainSettings = domainSettingsValidator.some(
      (validator: any) => validator === Validators.required
    );
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

  it('should change the form when authmode is changed', () => {
    const authModeControl = component.smbForm.get('auth_mode');
    authModeControl?.setValue('user');
    component.onAuthModeChange();
    fixture.detectChanges();
    const joinSourcesControl = component.smbForm.get('joinSources') as FormArray;
    expect(joinSourcesControl.length).toBe(1);
  });

  it('should add and remove custom dns settings (custom_dns)', () => {
    const defaultLength = component.custom_dns.length;
    component.addCustomDns();
    expect(component.custom_dns.length).toBe(defaultLength + 1);
    component.removeCustomDNS(0);
    expect(component.custom_dns.length).toBe(defaultLength);
  });

  it('on submit', () => {
    component.smbForm.get('auth_mode')?.setValue(AuthMode.activeDirectory);
    component.smbForm.get('domain_settings')?.setValue('test-realm');
    component.smbForm.get('cluster_id')?.setValue('cluster-id');
    component.submitAction();
  });

  it('delete domain', () => {
    component.deleteDomainSettingsModal();
    expect(component).toBeTruthy();
  });
});
