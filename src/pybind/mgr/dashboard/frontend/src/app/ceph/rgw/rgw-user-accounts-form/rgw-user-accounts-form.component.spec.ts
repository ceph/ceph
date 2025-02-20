import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RgwUserAccountsFormComponent } from './rgw-user-accounts-form.component';
import { ComponentsModule } from '~/app/shared/components/components.module';
import { RouterTestingModule } from '@angular/router/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ToastrModule } from 'ngx-toastr';
import { PipesModule } from '~/app/shared/pipes/pipes.module';
import { of } from 'rxjs';
import { RgwUserAccountsService } from '~/app/shared/api/rgw-user-accounts.service';
import { ModalModule } from 'carbon-components-angular';
import { ReactiveFormsModule } from '@angular/forms';
import { RgwUserAccountsComponent } from '../rgw-user-accounts/rgw-user-accounts.component';

class MockRgwUserAccountsService {
  create = jest.fn().mockReturnValue(of(null));
  modify = jest.fn().mockReturnValue(of(null));
  setQuota = jest.fn().mockReturnValue(of(null));
}

describe('RgwUserAccountsFormComponent', () => {
  let component: RgwUserAccountsFormComponent;
  let fixture: ComponentFixture<RgwUserAccountsFormComponent>;
  let rgwUserAccountsService: MockRgwUserAccountsService;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [RgwUserAccountsFormComponent],
      imports: [
        ComponentsModule,
        ToastrModule.forRoot(),
        HttpClientTestingModule,
        PipesModule,
        RouterTestingModule.withRoutes([
          { path: 'rgw/accounts', component: RgwUserAccountsComponent }
        ]),
        ModalModule,
        ReactiveFormsModule
      ],
      providers: [{ provide: RgwUserAccountsService, useClass: MockRgwUserAccountsService }]
    }).compileComponents();

    fixture = TestBed.createComponent(RgwUserAccountsFormComponent);
    rgwUserAccountsService = (TestBed.inject(
      RgwUserAccountsService
    ) as unknown) as MockRgwUserAccountsService;
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should call create method of MockRgwUserAccountsService and show success notification', () => {
    component.editing = false;
    component.accountForm.get('name').setValue('test');
    const payload = {
      account_name: 'test',
      email: '',
      tenant: '',
      max_users: 1000,
      max_buckets: 1000,
      max_roles: 1000,
      max_group: 1000,
      max_access_keys: 4
    };
    const spy = jest.spyOn(component, 'submit');
    const createDataSpy = jest.spyOn(rgwUserAccountsService, 'create').mockReturnValue(of(null));
    component.submit();
    expect(component.accountForm.valid).toBe(true);
    expect(spy).toHaveBeenCalled();
    expect(createDataSpy).toHaveBeenCalled();
    expect(createDataSpy).toHaveBeenCalledWith(payload);
  });

  it('should call modify method of MockRgwUserAccountsService and show success notification', () => {
    component.editing = true;
    component.accountForm.get('name').setValue('test');
    component.accountForm.get('id').setValue('RGW12312312312312312');
    component.accountForm.get('email').setValue('test@test.com');
    const payload = {
      account_id: 'RGW12312312312312312',
      account_name: 'test',
      email: 'test@test.com',
      tenant: '',
      max_users: 1000,
      max_buckets: 1000,
      max_roles: 1000,
      max_group: 1000,
      max_access_keys: 4
    };
    const spy = jest.spyOn(component, 'submit');
    const modifyDataSpy = jest.spyOn(rgwUserAccountsService, 'modify').mockReturnValue(of(null));
    component.submit();
    expect(component.accountForm.valid).toBe(true);
    expect(spy).toHaveBeenCalled();
    expect(modifyDataSpy).toHaveBeenCalled();
    expect(modifyDataSpy).toHaveBeenCalledWith(payload);
  });

  it('should call setQuota for "account" if account quota is dirty', () => {
    component.accountForm.get('id').setValue('123');
    component.accountForm.get('account_quota_enabled').setValue(true);
    component.accountForm.get('account_quota_max_size_unlimited').setValue(false);
    component.accountForm.get('account_quota_max_size').setValue('1 GiB');
    component.accountForm.get('account_quota_max_objects_unlimited').setValue(false);
    component.accountForm.get('account_quota_max_objects').setValue('100');
    component.accountForm.get('account_quota_max_size').markAsDirty();
    const accountId = '123';

    const spySetQuota = jest.spyOn(rgwUserAccountsService, 'setQuota').mockReturnValue(of(null));
    const spyGoToListView = jest.spyOn(component, 'goToListView');

    component.setQuotaConfig();
    const accountQuotaArgs = {
      quota_type: 'account',
      enabled: component.accountForm.getValue('account_quota_enabled'),
      max_size: '1073741824',
      max_objects: component.accountForm.getValue('account_quota_max_objects')
    };
    expect(spySetQuota).toHaveBeenCalledWith(accountId, accountQuotaArgs);
    expect(spyGoToListView).toHaveBeenCalled();
  });

  it('should call setQuota for "bucket" if account quota is dirty', () => {
    component.accountForm.get('id').setValue('123');
    component.accountForm.get('bucket_quota_enabled').setValue(true);
    component.accountForm.get('bucket_quota_max_size_unlimited').setValue(false);
    component.accountForm.get('bucket_quota_max_size').setValue('1 GiB');
    component.accountForm.get('bucket_quota_max_objects_unlimited').setValue(false);
    component.accountForm.get('bucket_quota_max_objects').setValue('100');
    component.accountForm.get('bucket_quota_max_size').markAsDirty();
    const accountId = '123';

    const spySetQuota = jest.spyOn(rgwUserAccountsService, 'setQuota').mockReturnValue(of(null));
    const spyGoToListView = jest.spyOn(component, 'goToListView');

    component.setQuotaConfig();
    const bucketQuotaArgs = {
      quota_type: 'bucket',
      enabled: component.accountForm.getValue('bucket_quota_enabled'),
      max_size: '1073741824',
      max_objects: component.accountForm.getValue('bucket_quota_max_objects')
    };
    expect(spySetQuota).toHaveBeenCalledWith(accountId, bucketQuotaArgs);
    expect(spyGoToListView).toHaveBeenCalled();
  });
});
