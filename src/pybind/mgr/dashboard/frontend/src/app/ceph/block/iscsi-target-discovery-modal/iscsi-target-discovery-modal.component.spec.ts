import {
  HttpClientTestingModule,
  HttpTestingController,
  TestRequest
} from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastModule } from 'ng2-toastr';
import { BsModalRef } from 'ngx-bootstrap/modal';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { SharedModule } from '../../../shared/shared.module';
import { IscsiTargetDiscoveryModalComponent } from './iscsi-target-discovery-modal.component';

describe('IscsiTargetDiscoveryModalComponent', () => {
  let component: IscsiTargetDiscoveryModalComponent;
  let fixture: ComponentFixture<IscsiTargetDiscoveryModalComponent>;
  let httpTesting: HttpTestingController;
  let req: TestRequest;

  configureTestBed({
    declarations: [IscsiTargetDiscoveryModalComponent],
    imports: [
      HttpClientTestingModule,
      ReactiveFormsModule,
      SharedModule,
      ToastModule.forRoot(),
      RouterTestingModule
    ],
    providers: [i18nProviders, BsModalRef]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(IscsiTargetDiscoveryModalComponent);
    component = fixture.componentInstance;
    httpTesting = TestBed.get(HttpTestingController);
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
});
