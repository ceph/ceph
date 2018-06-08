import { HttpClientTestingModule } from '@angular/common/http/testing';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { FormControl, ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { BsModalService } from 'ngx-bootstrap/modal';
import 'rxjs/add/observable/of';
import { Observable } from 'rxjs/Observable';

import { RgwUserService } from '../../../shared/api/rgw-user.service';
import { SharedModule } from '../../../shared/shared.module';
import { RgwUserS3Key } from '../models/rgw-user-s3-key';
import { RgwUserFormComponent } from './rgw-user-form.component';

describe('RgwUserFormComponent', () => {
  let component: RgwUserFormComponent;
  let fixture: ComponentFixture<RgwUserFormComponent>;
  let queryResult: Array<string> = [];

  class MockRgwUserService extends RgwUserService {
    enumerate() {
      return Observable.of(queryResult);
    }
  }

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ RgwUserFormComponent ],
      imports: [
        HttpClientTestingModule,
        ReactiveFormsModule,
        RouterTestingModule,
        SharedModule
      ],
      providers: [
        BsModalService,
        { provide: RgwUserService, useClass: MockRgwUserService }
      ]
    })
    .compileComponents();
  }));

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
      expect(rgwUserService.addS3Key).toHaveBeenCalledWith(
        'test1',
        'subuser2',
        undefined,
        undefined,
        undefined
      );
    });

    it('should set key w/o subuser', () => {
      const key = new RgwUserS3Key();
      key.user = 'test1';
      component.setS3Key(key);
      expect(component.s3Keys.length).toBe(1);
      expect(component.s3Keys[0].user).toBe('test1');
      expect(rgwUserService.addS3Key).toHaveBeenCalledWith(
        'test1',
        '',
        undefined,
        undefined,
        undefined
      );
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

  describe('userIdValidator', () => {
    it('should validate user id (1/3)', () => {
      const validatorFn = component.userIdValidator();
      const ctrl = new FormControl('');
      const validatorPromise = validatorFn(ctrl);
      expect(validatorPromise instanceof Promise).toBeTruthy();
      if (validatorPromise instanceof Promise) {
        validatorPromise.then((resp) => {
          expect(resp).toBe(null);
        });
      }
    });

    it('should validate user id (2/3)', () => {
      const validatorFn = component.userIdValidator();
      const ctrl = new FormControl('ab');
      ctrl.markAsDirty();
      const validatorPromise = validatorFn(ctrl);
      expect(validatorPromise instanceof Promise).toBeTruthy();
      if (validatorPromise instanceof Promise) {
        validatorPromise.then((resp) => {
          expect(resp).toBe(null);
        });
      }
    });

    it('should validate user id (3/3)', () => {
      queryResult = ['abc'];
      const validatorFn = component.userIdValidator();
      const ctrl = new FormControl('abc');
      ctrl.markAsDirty();
      const validatorPromise = validatorFn(ctrl);
      expect(validatorPromise instanceof Promise).toBeTruthy();
      if (validatorPromise instanceof Promise) {
        validatorPromise.then((resp) => {
          expect(resp instanceof Object).toBeTruthy();
          expect(resp.userIdExists).toBeTruthy();
        });
      }
    });
  });
});
