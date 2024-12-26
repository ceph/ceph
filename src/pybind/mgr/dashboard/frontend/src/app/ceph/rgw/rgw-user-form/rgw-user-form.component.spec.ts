import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { FormControl, ReactiveFormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbTooltipModule } from '@ng-bootstrap/ng-bootstrap';
import { NgxPipeFunctionModule } from 'ngx-pipe-function';
import { ToastrModule } from 'ngx-toastr';
import { of as observableOf, throwError } from 'rxjs';

import { RgwUserService } from '~/app/shared/api/rgw-user.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { NotificationService } from '~/app/shared/services/notification.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed, FormHelper } from '~/testing/unit-test-helper';
import { RgwUserCapabilities } from '../models/rgw-user-capabilities';
import { RgwUserCapability } from '../models/rgw-user-capability';
import { RgwUserS3Key } from '../models/rgw-user-s3-key';
import { RgwUserFormComponent } from './rgw-user-form.component';
import { DUE_TIMER } from '~/app/shared/forms/cd-validators';
import { CommonModule } from '@angular/common';
import { PipesModule } from '~/app/shared/pipes/pipes.module';

describe('RgwUserFormComponent', () => {
  let component: RgwUserFormComponent;
  let fixture: ComponentFixture<RgwUserFormComponent>;
  let rgwUserService: RgwUserService;
  let formHelper: FormHelper;
  let modalRef: any;
  configureTestBed({
    declarations: [RgwUserFormComponent],
    imports: [
      HttpClientTestingModule,
      ReactiveFormsModule,
      RouterTestingModule,
      SharedModule,
      ToastrModule.forRoot(),
      NgbTooltipModule,
      NgxPipeFunctionModule
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwUserFormComponent);
    component = fixture.componentInstance;
    component.ngOnInit();
    rgwUserService = TestBed.inject(RgwUserService);
    formHelper = new FormHelper(component.userForm);
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('s3 key management', () => {
    beforeEach(() => {
      spyOn(rgwUserService, 'addS3Key').and.stub();
    });

    it('should not update key', () => {
      component.setS3Key(new RgwUserS3Key(), 3);
      expect(component.s3Keys.length).toBe(0);
      expect(rgwUserService.addS3Key).not.toHaveBeenCalled();
    });

    it('should set user defined key', () => {
      const key = new RgwUserS3Key();
      key.user = 'test1:subuser2';
      key.access_key = 'my-access-key';
      key.secret_key = 'my-secret-key';
      component.setS3Key(key);
      expect(component.s3Keys.length).toBe(1);
      expect(component.s3Keys[0].user).toBe('test1:subuser2');
      expect(rgwUserService.addS3Key).toHaveBeenCalledWith('test1', {
        subuser: 'subuser2',
        generate_key: 'false',
        access_key: 'my-access-key',
        secret_key: 'my-secret-key'
      });
    });

    it('should set params for auto-generating key', () => {
      const key = new RgwUserS3Key();
      key.user = 'test1:subuser2';
      key.generate_key = true;
      key.access_key = 'my-access-key';
      key.secret_key = 'my-secret-key';
      component.setS3Key(key);
      expect(component.s3Keys.length).toBe(1);
      expect(component.s3Keys[0].user).toBe('test1:subuser2');
      expect(rgwUserService.addS3Key).toHaveBeenCalledWith('test1', {
        subuser: 'subuser2',
        generate_key: 'true'
      });
    });

    it('should set key w/o subuser', () => {
      const key = new RgwUserS3Key();
      key.user = 'test1';
      component.setS3Key(key);
      expect(component.s3Keys.length).toBe(1);
      expect(component.s3Keys[0].user).toBe('test1');
      expect(rgwUserService.addS3Key).toHaveBeenCalledWith('test1', {
        subuser: '',
        generate_key: 'false',
        access_key: undefined,
        secret_key: undefined
      });
    });
  });

  describe('quotaMaxSizeValidator', () => {
    it('should validate max size (1)', () => {
      const resp = component.quotaMaxSizeValidator(new FormControl(''));
      expect(resp).toBe(null);
    });

    it('should validate max size (2)', () => {
      const resp = component.quotaMaxSizeValidator(new FormControl('xxxx'));
      expect(resp.quotaMaxSize).toBeTruthy();
    });

    it('should validate max size (3)', () => {
      const resp = component.quotaMaxSizeValidator(new FormControl('1023'));
      expect(resp.quotaMaxSize).toBeTruthy();
    });

    it('should validate max size (4)', () => {
      const resp = component.quotaMaxSizeValidator(new FormControl('1024'));
      expect(resp).toBe(null);
    });

    it('should validate max size (5)', () => {
      const resp = component.quotaMaxSizeValidator(new FormControl('1M'));
      expect(resp).toBe(null);
    });

    it('should validate max size (6)', () => {
      const resp = component.quotaMaxSizeValidator(new FormControl('1024 gib'));
      expect(resp).toBe(null);
    });

    it('should validate max size (7)', () => {
      const resp = component.quotaMaxSizeValidator(new FormControl('10 X'));
      expect(resp.quotaMaxSize).toBeTruthy();
    });

    it('should validate max size (8)', () => {
      const resp = component.quotaMaxSizeValidator(new FormControl('1.085 GiB'));
      expect(resp).toBe(null);
    });

    it('should validate max size (9)', () => {
      const resp = component.quotaMaxSizeValidator(new FormControl('1,085 GiB'));
      expect(resp.quotaMaxSize).toBeTruthy();
    });
  });

  describe('username validation', () => {
    it('should validate that username is required', () => {
      formHelper.expectErrorChange('user_id', '', 'required', true);
    });

    it('should validate that username is valid', fakeAsync(() => {
      spyOn(rgwUserService, 'get').and.returnValue(throwError('foo'));
      formHelper.setValue('user_id', 'ab', true);
      tick(DUE_TIMER);
      formHelper.expectValid('user_id');
    }));

    it('should validate that username is invalid', fakeAsync(() => {
      spyOn(rgwUserService, 'get').and.returnValue(observableOf({}));
      formHelper.setValue('user_id', 'abc', true);
      tick(DUE_TIMER);
      formHelper.expectError('user_id', 'notUnique');
    }));
  });

  describe('max buckets', () => {
    it('disable creation (create)', () => {
      spyOn(rgwUserService, 'create');
      formHelper.setValue('max_buckets_mode', -1, true);
      component.onSubmit();
      expect(rgwUserService.create).toHaveBeenCalledWith({
        access_key: '',
        display_name: null,
        email: '',
        generate_key: true,
        max_buckets: -1,
        secret_key: '',
        suspended: false,
        system: false,
        uid: null
      });
    });

    it('disable creation (edit)', () => {
      spyOn(rgwUserService, 'update');
      component.editing = true;
      formHelper.setValue('max_buckets_mode', -1, true);
      component.onSubmit();
      expect(rgwUserService.update).toHaveBeenCalledWith(null, {
        display_name: null,
        email: null,
        max_buckets: -1,
        suspended: false,
        system: false
      });
    });

    it('unlimited buckets (create)', () => {
      spyOn(rgwUserService, 'create');
      formHelper.setValue('max_buckets_mode', 0, true);
      component.onSubmit();
      expect(rgwUserService.create).toHaveBeenCalledWith({
        access_key: '',
        display_name: null,
        email: '',
        generate_key: true,
        max_buckets: 0,
        secret_key: '',
        suspended: false,
        system: false,
        uid: null
      });
    });

    it('unlimited buckets (edit)', () => {
      spyOn(rgwUserService, 'update');
      component.editing = true;
      formHelper.setValue('max_buckets_mode', 0, true);
      component.onSubmit();
      expect(rgwUserService.update).toHaveBeenCalledWith(null, {
        display_name: null,
        email: null,
        max_buckets: 0,
        suspended: false,
        system: false
      });
    });

    it('custom (create)', () => {
      spyOn(rgwUserService, 'create');
      formHelper.setValue('max_buckets_mode', 1, true);
      formHelper.setValue('max_buckets', 100, true);
      component.onSubmit();
      expect(rgwUserService.create).toHaveBeenCalledWith({
        access_key: '',
        display_name: null,
        email: '',
        generate_key: true,
        max_buckets: 100,
        secret_key: '',
        suspended: false,
        system: false,
        uid: null
      });
    });

    it('custom (edit)', () => {
      spyOn(rgwUserService, 'update');
      component.editing = true;
      formHelper.setValue('max_buckets_mode', 1, true);
      formHelper.setValue('max_buckets', 100, true);
      component.onSubmit();
      expect(rgwUserService.update).toHaveBeenCalledWith(null, {
        display_name: null,
        email: null,
        max_buckets: 100,
        suspended: false,
        system: false
      });
    });
  });

  describe('submit form', () => {
    let notificationService: NotificationService;

    beforeEach(() => {
      spyOn(TestBed.inject(Router), 'navigate').and.stub();
      notificationService = TestBed.inject(NotificationService);
      spyOn(notificationService, 'show');
    });

    it('should be able to clear the mail field on update', () => {
      spyOn(rgwUserService, 'update');
      component.editing = true;
      formHelper.setValue('email', '', true);
      component.onSubmit();
      expect(rgwUserService.update).toHaveBeenCalledWith(null, {
        display_name: null,
        email: '',
        max_buckets: 1000,
        suspended: false,
        system: false
      });
    });

    it('tests create success notification', () => {
      spyOn(rgwUserService, 'create').and.returnValue(observableOf([]));
      component.editing = false;
      formHelper.setValue('suspended', true, true);
      component.onSubmit();
      expect(notificationService.show).toHaveBeenCalledWith(
        NotificationType.success,
        `Created Object Gateway user 'null'`
      );
    });

    it('tests update success notification', () => {
      spyOn(rgwUserService, 'update').and.returnValue(observableOf([]));
      component.editing = true;
      formHelper.setValue('suspended', true, true);
      component.onSubmit();
      expect(notificationService.show).toHaveBeenCalledWith(
        NotificationType.success,
        `Updated Object Gateway user 'null'`
      );
    });
  });

  describe('RgwUserCapabilities', () => {
    it('capability button disabled when all capabilities are added', () => {
      component.editing = true;
      for (const capabilityType of RgwUserCapabilities.getAll()) {
        const capability = new RgwUserCapability();
        capability.type = capabilityType;
        capability.perm = 'read';
        component.setCapability(capability);
      }

      fixture.detectChanges();

      expect(component.hasAllCapabilities(component.capabilities)).toBeTruthy();
      const capabilityButton = fixture.debugElement.nativeElement.querySelector('.tc_addCapButton');
      expect(capabilityButton.disabled).toBeTruthy();
    });

    it('capability button not disabled when not all capabilities are added', () => {
      component.editing = true;

      fixture.detectChanges();

      const capabilityButton = fixture.debugElement.nativeElement.querySelector('.tc_addCapButton');
      expect(capabilityButton.disabled).toBeFalsy();
    });
  })
    it('should not modify max_buckets if mode is not "1"', () => {
      formHelper.setValue('max_buckets', '', false);
      component.onMaxBucketsModeChange('2');
      const s=spyOn(component.userForm,'patchValue');
      expect(s).not.toHaveBeenCalled();
    });
    // it('should set max_buckets to 1000 if it is invalid and mode is "1"', () => {

      // const patchValueSpy = jest.spyOn(component.userForm, 'patchValue');
      // formHelper.setValue('max_buckets_mode', -1, true);
      // component.userForm.controls['max_buckets'].setValue('');
      // // formHelper.setValue('max_buckets', '', false); 
      // component.onMaxBucketsModeChange('1');
      // expect(patchValueSpy).toHaveBeenCalledWith({ max_buckets: 1000 });

// ******************
      // formHelper.setValue('max_buckets', '', false);
      // component.onMaxBucketsModeChange('1');
      // const s=spyOn(component.userForm,'patchValue')
      // expect(s).toHaveBeenCalledWith({ max_buckets: 1000 });
// ******************
      // const patchValueSpy = jest.spyOn(component.userForm, 'patchValue');
  
      // // Mock the `get` method of `userForm` to return a control that is invalid
      // const mockInvalidControl = { valid: false };
      // component.userForm.get = jest.fn().mockReturnValue(mockInvalidControl);
      
      // // Simulate an invalid value for 'max_buckets' (this might already be done in your formHelper)
      // formHelper.setValue('max_buckets', '', false);  // Assuming this sets 'max_buckets' to invalid
      
      // // Act: Trigger the `onMaxBucketsModeChange` method with mode "1" (Custom mode)
      // component.onMaxBucketsModeChange('1');
      
      // // Assert: Check that `patchValue` was called with the correct argument
      // expect(patchValueSpy).toHaveBeenCalledWith({ max_buckets: 1000 });
    // });

    it('should call _setRateLimitProperty when value is equal to 0 ',()=>{

      const mockrateLimitKey='user_rate_limit_max_readBytes';
      const mockunlimitedKey='user_rate_limit_max_readBytes_unlimited';
      const mockproperty=0;

      component['_setRateLimitProperty'](formHelper, mockrateLimitKey, mockunlimitedKey, mockproperty);
      expect(component.userForm.getValue('user_rate_limit_max_readBytes_unlimited')).toEqual(true);
      expect(component.userForm.getValue('user_rate_limit_max_readBytes')).toEqual(null);
    })
    // it('should call _setRateLimitProperty when value is greater than to 0 ',()=>{

    //   const mockrateLimitKey='user_rate_limit_max_readBytes';
    //   const mockunlimitedKey='user_rate_limit_max_readBytes_unlimited';
    //   const mockproperty=100;
    //   spyOn(component['rgwUserService'],'getUserRateLimit').and.returnValue({"user_ratelimit": {
    //             "max_read_ops": 100,
    //             "max_write_ops": 100,
    //             "max_read_bytes": 100,
    //             "max_write_bytes": 100,
    //             "enabled": 'true'
    //             }
    //         })
    //   component['_setRateLimitProperty'](formHelper, mockrateLimitKey, mockunlimitedKey, mockproperty);
    //   expect(component.userForm.getValue('user_rate_limit_max_readBytes_unlimited')).toEqual(false);
    //   expect(component.userForm.getValue('user_rate_limit_max_readBytes')).toEqual(100);
    // })

    // it('should call updateFieldsWhenTenanted',()=>{
    //   formHelper.setValue('show_tenant', true, true);
    //   component.updateFieldsWhenTenanted();
    //   const s=spyOn(component.userForm.get,'markAsUntouched');
    //   expect(s).toHaveBeenCalledWith();
    // })

    describe('updateFieldsWhenTenanted()', () => {
        it('should reset the form when showTenant is falsy', () => {
          component.showTenant = false;

          component.previousTenant = 'true';
    
          component.userForm.get('tenant').setValue('test1');
          component.userForm.get('user_id').setValue('user-123');
    
          component.updateFieldsWhenTenanted();
          expect(formHelper.getControl('user_id').untouched).toBeTruthy(); // user_id should be untouched
          expect(formHelper.getControl('tenant').value).toBe('true'); // tenant should be reset to 
        });

        // it('should update the form when showTenant is truthy',  fakeAsync(() =>  {
        //   component.showTenant = true;

        //   component.previousTenant = 'mockPreviousTenantValue';

        //   component.userForm.get('tenant').setValue('test');
        //   component.userForm.get('user_id').setValue('tuser-1234');
        //   console.log('Before calling x():', component.userForm.get('tenant').value);
        //   fixture.detectChanges();
        //   component.updateFieldsWhenTenanted();
        //   tick();
          
        //   console.log('After calling x():', component.userForm.get('tenant').value);
        //   // expect(formHelper.getControl('user_id').touched).toBeTruthy(); // user_id should be touched
        //   // expect(component.previousTenant).toEqual('mockvalue'); // previousTenant should be updated
        //   expect(component.userForm.get('tenant').value).toBeNull(); // tenant should be null
          
        // }));
    
    });
    // it('should remove capability from cap array and mark form as dirty when y() is called', () => {
    //   const index = 1;
    //   const initialLength = component.capabilities.length;
    
    //   // Call the method
    //   component.deleteCapability(index);
  
    //   // Test that the service.y() was called with the correct parameters
    //   expect(component['rgwUserService'].deleteCapability).toHaveBeenCalledWith(component.getUID(), component.capabilities[index].type, component.capabilities[index].perm);
  
    //   // Check that the capability at the given index was removed
    //   expect(component.capabilities.length).toBe(initialLength - 1);  // One item should be removed
    //   expect(component.capabilities).toEqual([{ type: 'type1', perm: 'perm1' }]); // Only the first capability should remain
  
    //   // Check that the submitObservables array has the correct observable
    //   expect(component.submitObservables.length).toBe(1);
    //   expect(component.submitObservables[0]).toEqual(of('mockResponse'));
  
    //   // Check that the form is marked as dirty
    //   expect(component.userForm.dirty).toBeTrue();  // The form should be marked as dirty
    // });
  

  it('should call deletecapab',()=>{
    component.capabilities=[ {type: 'users', perm: 'read'}]
    component.deleteCapability(0);
    expect(component.capabilities.length).toBe(0);
    expect(component.userForm.dirty).toBeTruthy();
  })
  it('should call deleteS3Key',()=>{
    component.s3Keys= [{user: 'test5$test11', access_key: 'A009', secret_key: 'ABCKEY', generate_key: true}]; 
    component.deleteS3Key(0);
    expect(component.userForm.dirty).toBeTruthy();
  })
  // it('should call _getUserRateLimitArgs',()=>{
  //   component.userForm.get('user_id').setValue('test');
  //   component.userForm.get('user_rate_limit_enabled').setValue('true');
  //   component.userForm.get('user_rate_limit_max_readOps').setValue('100');
  //   component.userForm.get('user_rate_limit_max_readBytes').setValue('100');
  //   component.userForm.get('user_rate_limit_max_writeOps').setValue('200');
  //   component.userForm.get('user_rate_limit_max_writeBytes').setValue('200');

  //   component.userForm.get('user_rate_limit_max_readOps_unlimited').setValue('false'  );
  //   component.userForm.get('user_rate_limit_max_writeOps_unlimited').setValue('false');
  //   component.userForm.get('user_rate_limit_max_readBytes_unlimited').setValue('false');
  //   component.userForm.get('user_rate_limit_max_writeBytes_unlimited').setValue('false');
  
  //   const result = component['_getUserRateLimitArgs']();
  //   expect(result.max_read_ops).toBe("100");
  //   expect(result.max_write_ops).toBe("200");
  //   expect(result.max_read_bytes).toBe("100");
  //   expect(result.max_write_bytes).toBe("200");
  // })
  it('should call showCapabilityModal',()=>{
   const s=spyOn(component['modalService'], 'show').and.callFake(() => {
      modalRef = {
        componentInstance: {
          setEditing: jest.fn(),
          setValues: jest.fn(),
          setCapabilities:jest.fn(),
          submitAction:{subscribe:jest.fn()}
        }
      };
      return modalRef;
    });
    component.capabilities=[ {type: 'users', perm: 'read'}]
    component.showCapabilityModal(0);
    expect(s).toHaveBeenCalled();
  })
  it('should call showS3KeyModal',()=>{
    const s=spyOn(component['modalService'], 'show').and.callFake(() => {
       modalRef = {
         componentInstance: {
          setValues: jest.fn(),
          setViewing: jest.fn(),
          setUserCandidates:jest.fn(),
          submitAction:{subscribe:jest.fn()}
         }
       };
       return modalRef;
     });
     component.s3Keys= [{user: 'test5$test11', access_key: 'A009', secret_key: 'ABCKEY', generate_key: true}];
     component.showS3KeyModal(0);
     expect(s).toHaveBeenCalled();
   })
  //  it('should call showS3KeyModal',()=>{
  //   const s=spyOn(component['modalService'], 'show').and.callFake(() => {
  //      modalRef = {
  //        componentInstance: {
  //         setValues: jest.fn(),
  //         setEditing: jest.fn(),
  //         setSubusers:jest.fn(),
  //         submitAction:{subscribe:jest.fn()}
  //        }
  //      };
  //      return modalRef;
  //    });
  //    component.s3Keys= [{user: 'test5$test11', access_key: 'A009', secret_key: 'ABCKEY', generate_key: true}];
  //    component.showSubuserModal(0);
  //    expect(s).toHaveBeenCalled();
  //  })
  it('should call _getS3KeyUserCandidates',()=>{
    spyOn(component,'getUID').and.returnValue('mockUID');
    component.s3Keys=[{user: 'mockUID', access_key: 'test', secret_key: 'TestKey', generate_key: true}]
    const s=component['_getS3KeyUserCandidates']();
    const result= ['mockUID'];
    expect(s).toEqual(result);
  })
});
