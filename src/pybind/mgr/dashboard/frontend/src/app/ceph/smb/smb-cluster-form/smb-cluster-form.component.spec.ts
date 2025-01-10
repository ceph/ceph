import { ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { SmbClusterFormComponent } from './smb-cluster-form.component';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { SharedModule } from '~/app/shared/shared.module';
import { RouterTestingModule } from '@angular/router/testing';
import { FormArray, ReactiveFormsModule, Validators } from '@angular/forms';
import { ToastrModule } from 'ngx-toastr';
import { ComboBoxModule, GridModule, InputModule, SelectModule } from 'carbon-components-angular';
import { AUTHMODE } from '../smb.model';
import { FOO_USERSGROUPS } from '../smb-usersgroups-form/smb-usersgroups-form.component.spec';
import { of } from 'rxjs';
import { By } from '@angular/platform-browser';

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
    authModeControl?.setValue('user');
    component.onAuthModeChange();
    fixture.detectChanges();
    const joinSourcesControl = component.smbForm.get('joinSources') as FormArray;
    expect(joinSourcesControl.length).toBe(1);
  });

  it('should check submit request', () => {
    component.smbForm.get('auth_mode').setValue(AUTHMODE.activeDirectory);
    component.smbForm.get('domain_settings').setValue('test-realm');
    component.smbForm.get('cluster_id').setValue('cluster-id');
    component.submitAction();
  });

  it('should delete domain', () => {
    component.deleteDomainSettingsModal();
    expect(component).toBeTruthy();
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
