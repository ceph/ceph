import {
  HttpClientTestingModule,
  HttpTestingController,
  TestRequest
} from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { By } from '@angular/platform-browser';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { ToastrModule } from 'ngx-toastr';

import {
  configureTestBed,
  FormHelper,
  i18nProviders,
  IscsiHelper
} from '../../../../testing/unit-test-helper';
import { Permission } from '../../../shared/models/permissions';
import { SharedModule } from '../../../shared/shared.module';
import { IscsiTargetDiscoveryModalComponent } from './iscsi-target-discovery-modal.component';

describe('IscsiTargetDiscoveryModalComponent', () => {
  let component: IscsiTargetDiscoveryModalComponent;
  let fixture: ComponentFixture<IscsiTargetDiscoveryModalComponent>;
  let httpTesting: HttpTestingController;
  let req: TestRequest;

  const elem = (css: string) => fixture.debugElement.query(By.css(css));
  const elemDisabled = (css: string) => elem(css).nativeElement.disabled;

  configureTestBed({
    declarations: [IscsiTargetDiscoveryModalComponent],
    imports: [
      HttpClientTestingModule,
      ReactiveFormsModule,
      SharedModule,
      ToastrModule.forRoot(),
      RouterTestingModule
    ],
    providers: [i18nProviders, NgbActiveModal]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(IscsiTargetDiscoveryModalComponent);
    component = fixture.componentInstance;
    httpTesting = TestBed.inject(HttpTestingController);
  });

  describe('with update permissions', () => {
    beforeEach(() => {
      component.permission = new Permission(['update']);
      fixture.detectChanges();
      req = httpTesting.expectOne('api/iscsi/discoveryauth');
    });

    it('should create', () => {
      expect(component).toBeTruthy();
    });

    it('should create form', () => {
      expect(component.discoveryForm.value).toEqual({
        user: '',
        password: '',
        mutual_user: '',
        mutual_password: ''
      });
    });

    it('should patch form', () => {
      req.flush({
        user: 'foo',
        password: 'bar',
        mutual_user: 'mutual_foo',
        mutual_password: 'mutual_bar'
      });
      expect(component.discoveryForm.value).toEqual({
        user: 'foo',
        password: 'bar',
        mutual_user: 'mutual_foo',
        mutual_password: 'mutual_bar'
      });
    });

    it('should submit new values', () => {
      component.discoveryForm.patchValue({
        user: 'new_user',
        password: 'new_pass',
        mutual_user: 'mutual_new_user',
        mutual_password: 'mutual_new_pass'
      });
      component.submitAction();

      const submit_req = httpTesting.expectOne('api/iscsi/discoveryauth');
      expect(submit_req.request.method).toBe('PUT');
      expect(submit_req.request.body).toEqual({
        user: 'new_user',
        password: 'new_pass',
        mutual_user: 'mutual_new_user',
        mutual_password: 'mutual_new_pass'
      });
    });

    it('should enable form if user has update permission', () => {
      expect(elemDisabled('input#user')).toBeFalsy();
      expect(elemDisabled('input#password')).toBeFalsy();
      expect(elemDisabled('input#mutual_user')).toBeFalsy();
      expect(elemDisabled('input#mutual_password')).toBeFalsy();
      expect(elem('cd-submit-button')).toBeDefined();
    });
  });

  it('should disabled form if user does not have update permission', () => {
    component.permission = new Permission(['read', 'create', 'delete']);
    fixture.detectChanges();
    req = httpTesting.expectOne('api/iscsi/discoveryauth');

    expect(elemDisabled('input#user')).toBeTruthy();
    expect(elemDisabled('input#password')).toBeTruthy();
    expect(elemDisabled('input#mutual_user')).toBeTruthy();
    expect(elemDisabled('input#mutual_password')).toBeTruthy();
    expect(elem('cd-submit-button')).toBeNull();
  });

  it('should validate authentication', () => {
    component.permission = new Permission(['read', 'create', 'update', 'delete']);
    fixture.detectChanges();
    const control = component.discoveryForm;
    const formHelper = new FormHelper(control);
    formHelper.expectValid(control);

    IscsiHelper.validateUser(formHelper, 'user');
    IscsiHelper.validatePassword(formHelper, 'password');
    IscsiHelper.validateUser(formHelper, 'mutual_user');
    IscsiHelper.validatePassword(formHelper, 'mutual_password');
  });
});
