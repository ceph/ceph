import { HttpClientTestingModule } from '@angular/common/http/testing';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { FormControl, ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { BsModalService } from 'ngx-bootstrap/modal';
import 'rxjs/add/observable/of';
import { Observable } from 'rxjs/Observable';

import { RgwUserService } from '../../../shared/api/rgw-user.service';
import { SharedModule } from '../../../shared/shared.module';
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
