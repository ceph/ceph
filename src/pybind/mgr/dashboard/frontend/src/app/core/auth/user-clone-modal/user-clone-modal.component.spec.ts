import { HttpClientTestingModule } from '@angular/common/http/testing';
import { async, ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { FormControl, ReactiveFormsModule } from '@angular/forms';

import { BsModalRef } from 'ngx-bootstrap/modal';
import { Observable, of as observableOf } from 'rxjs';

import { UserService } from '../../../shared/api/user.service';
import { SharedModule } from '../../../shared/shared.module';
import { UserCloneModalComponent } from './user-clone-modal.component';

describe('UserCloneModalComponent', () => {
  let component: UserCloneModalComponent;
  let fixture: ComponentFixture<UserCloneModalComponent>;

  class MockUserService extends UserService {
    list() {
      return observableOf([{ username: 'foo' }, { username: 'bar' }]);
    }
  }

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [UserCloneModalComponent],
      imports: [HttpClientTestingModule, ReactiveFormsModule, SharedModule],
      providers: [BsModalRef, { provide: UserService, useClass: MockUserService }]
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(UserCloneModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('usernameValidator', () => {
    it('should validate username (1/3)', () => {
      const validatorFn = component.usernameValidator();
      const ctrl = new FormControl('');
      const validator$ = validatorFn(ctrl);
      expect(validator$ instanceof Observable).toBeTruthy();
      if (validator$ instanceof Observable) {
        validator$.subscribe((resp) => {
          expect(resp).toBe(null);
        });
      }
    });

    it(
      'should validate username (2/3)',
      fakeAsync(() => {
        const validatorFn = component.usernameValidator(0);
        const ctrl = new FormControl('baz');
        ctrl.markAsDirty();
        const validator$ = validatorFn(ctrl);
        expect(validator$ instanceof Observable).toBeTruthy();
        if (validator$ instanceof Observable) {
          validator$.subscribe((resp) => {
            expect(resp).toBe(null);
          });
          tick();
        }
      })
    );

    it(
      'should validate username (3/3)',
      fakeAsync(() => {
        const validatorFn = component.usernameValidator(0);
        const ctrl = new FormControl('foo');
        ctrl.markAsDirty();
        const validator$ = validatorFn(ctrl);
        expect(validator$ instanceof Observable).toBeTruthy();
        if (validator$ instanceof Observable) {
          validator$.subscribe((resp) => {
            expect(resp instanceof Object).toBeTruthy();
            expect(resp.usernameExists).toBeTruthy();
          });
          tick();
        }
      })
    );
  });
});
