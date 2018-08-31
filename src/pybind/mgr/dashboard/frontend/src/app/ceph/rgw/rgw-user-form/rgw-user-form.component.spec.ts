import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { FormControl, ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { BsModalService } from 'ngx-bootstrap/modal';
import { of as observableOf } from 'rxjs';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { RgwUserService } from '../../../shared/api/rgw-user.service';
import { SharedModule } from '../../../shared/shared.module';
import { RgwUserS3Key } from '../models/rgw-user-s3-key';
import { RgwUserFormComponent } from './rgw-user-form.component';

describe('RgwUserFormComponent', () => {
  let component: RgwUserFormComponent;
  let fixture: ComponentFixture<RgwUserFormComponent>;

  configureTestBed({
    declarations: [RgwUserFormComponent],
    imports: [HttpClientTestingModule, ReactiveFormsModule, RouterTestingModule, SharedModule],
    providers: [BsModalService, RgwUserService]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwUserFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('s3 key management', () => {
    let rgwUserService: RgwUserService;

    beforeEach(() => {
      rgwUserService = TestBed.get(RgwUserService);
      spyOn(rgwUserService, 'addS3Key').and.stub();
    });

    it('should not update key', () => {
      component.setS3Key(new RgwUserS3Key(), 3);
      expect(component.s3Keys.length).toBe(0);
      expect(rgwUserService.addS3Key).not.toHaveBeenCalled();
    });

    it('should set key', () => {
      const key = new RgwUserS3Key();
      key.user = 'test1:subuser2';
      component.setS3Key(key);
      expect(component.s3Keys.length).toBe(1);
      expect(component.s3Keys[0].user).toBe('test1:subuser2');
      expect(rgwUserService.addS3Key).toHaveBeenCalledWith('test1', {
        subuser: 'subuser2',
        generate_key: 'false',
        access_key: undefined,
        secret_key: undefined
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
    it('should validate max size (1/7)', () => {
      const resp = component.quotaMaxSizeValidator(new FormControl(''));
      expect(resp).toBe(null);
    });

    it('should validate max size (2/7)', () => {
      const resp = component.quotaMaxSizeValidator(new FormControl('xxxx'));
      expect(resp.quotaMaxSize).toBeTruthy();
    });

    it('should validate max size (3/7)', () => {
      const resp = component.quotaMaxSizeValidator(new FormControl('1023'));
      expect(resp.quotaMaxSize).toBeTruthy();
    });

    it('should validate max size (4/7)', () => {
      const resp = component.quotaMaxSizeValidator(new FormControl('1024'));
      expect(resp).toBe(null);
    });

    it('should validate max size (5/7)', () => {
      const resp = component.quotaMaxSizeValidator(new FormControl('1M'));
      expect(resp).toBe(null);
    });

    it('should validate max size (6/7)', () => {
      const resp = component.quotaMaxSizeValidator(new FormControl('1024 gib'));
      expect(resp).toBe(null);
    });

    it('should validate max size (7/7)', () => {
      const resp = component.quotaMaxSizeValidator(new FormControl('10 X'));
      expect(resp.quotaMaxSize).toBeTruthy();
    });
  });

  describe('username validation', () => {
    let rgwUserService: RgwUserService;

    beforeEach(() => {
      rgwUserService = TestBed.get(RgwUserService);
      spyOn(rgwUserService, 'enumerate').and.returnValue(observableOf(['abc', 'xyz']));
    });

    it('should validate that username is required', () => {
      const user_id = component.userForm.get('user_id');
      user_id.markAsDirty();
      user_id.setValue('');
      expect(user_id.hasError('required')).toBeTruthy();
      expect(user_id.valid).toBeFalsy();
    });

    it(
      'should validate that username is valid',
      fakeAsync(() => {
        const user_id = component.userForm.get('user_id');
        user_id.markAsDirty();
        user_id.setValue('ab');
        tick(500);
        expect(user_id.hasError('notUnique')).toBeFalsy();
        expect(user_id.valid).toBeTruthy();
      })
    );

    it(
      'should validate that username is invalid',
      fakeAsync(() => {
        const user_id = component.userForm.get('user_id');
        user_id.markAsDirty();
        user_id.setValue('abc');
        tick(500);
        expect(user_id.hasError('notUnique')).toBeTruthy();
        expect(user_id.valid).toBeFalsy();
      })
    );
  });
});
