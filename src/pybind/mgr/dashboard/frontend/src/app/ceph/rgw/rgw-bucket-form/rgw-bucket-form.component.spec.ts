import { HttpClientTestingModule } from '@angular/common/http/testing';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { FormControl, ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import 'rxjs/add/observable/of';
import { Observable } from 'rxjs/Observable';

import { RgwBucketService } from '../../../shared/api/rgw-bucket.service';
import { RgwUserService } from '../../../shared/api/rgw-user.service';
import { SharedModule } from '../../../shared/shared.module';
import { RgwBucketFormComponent } from './rgw-bucket-form.component';

describe('RgwBucketFormComponent', () => {
  let component: RgwBucketFormComponent;
  let fixture: ComponentFixture<RgwBucketFormComponent>;
  let queryResult: Array<string> = [];

  class MockRgwBucketService extends RgwBucketService {
    enumerate() {
      return Observable.of(queryResult);
    }
  }

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ RgwBucketFormComponent ],
      imports: [
        HttpClientTestingModule,
        ReactiveFormsModule,
        RouterTestingModule,
        SharedModule
      ],
      providers: [
        RgwUserService,
        { provide: RgwBucketService, useClass: MockRgwBucketService }
      ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwBucketFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('bucketNameValidator', () => {
    it('should validate name (1/4)', () => {
      const validatorFn = component.bucketNameValidator();
      const ctrl = new FormControl('');
      const validatorPromise = validatorFn(ctrl);
      expect(validatorPromise instanceof Promise).toBeTruthy();
      if (validatorPromise instanceof Promise) {
        validatorPromise.then((resp) => {
          expect(resp).toBe(null);
        });
      }
    });

    it('should validate name (2/4)', () => {
      const validatorFn = component.bucketNameValidator();
      const ctrl = new FormControl('ab');
      ctrl.markAsDirty();
      const validatorPromise = validatorFn(ctrl);
      expect(validatorPromise instanceof Promise).toBeTruthy();
      if (validatorPromise instanceof Promise) {
        validatorPromise.then((resp) => {
          expect(resp.bucketNameInvalid).toBeTruthy();
        });
      }
    });

    it('should validate name (3/4)', () => {
      const validatorFn = component.bucketNameValidator();
      const ctrl = new FormControl('abc');
      ctrl.markAsDirty();
      const validatorPromise = validatorFn(ctrl);
      expect(validatorPromise instanceof Promise).toBeTruthy();
      if (validatorPromise instanceof Promise) {
        validatorPromise.then((resp) => {
          expect(resp).toBe(null);
        });
      }
    });

    it('should validate name (4/4)', () => {
      queryResult = ['abcd'];
      const validatorFn = component.bucketNameValidator();
      const ctrl = new FormControl('abcd');
      ctrl.markAsDirty();
      const validatorPromise = validatorFn(ctrl);
      expect(validatorPromise instanceof Promise).toBeTruthy();
      if (validatorPromise instanceof Promise) {
        validatorPromise.then((resp) => {
          expect(resp instanceof Object).toBeTruthy();
          expect(resp.bucketNameExists).toBeTruthy();
        });
      }
    });
  });
});
