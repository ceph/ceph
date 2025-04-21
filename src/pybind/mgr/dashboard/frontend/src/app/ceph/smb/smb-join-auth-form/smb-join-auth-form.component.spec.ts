import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SmbJoinAuthFormComponent } from './smb-join-auth-form.component';
import { ToastrModule } from 'ngx-toastr';
import { SharedModule } from '~/app/shared/shared.module';
import { provideHttpClient } from '@angular/common/http';
import { provideHttpClientTesting } from '@angular/common/http/testing';
import { provideRouter } from '@angular/router';
import { ReactiveFormsModule } from '@angular/forms';
import { SmbService } from '~/app/shared/api/smb.service';
import { JOIN_AUTH_RESOURCE } from '../smb.model';
import { of } from 'rxjs';

export const FOO_JOIN_AUTH = {
  auth_id: 'foo',
  auth: {
    username: 'user',
    password: 'pass'
  },
  resource_type: JOIN_AUTH_RESOURCE
};

describe('SmbJoinAuthFormComponent', () => {
  let component: SmbJoinAuthFormComponent;
  let fixture: ComponentFixture<SmbJoinAuthFormComponent>;
  let createJoinAuth: jasmine.Spy;
  let getJoinAuth: jasmine.Spy;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ToastrModule.forRoot(), SharedModule, ReactiveFormsModule],
      declarations: [SmbJoinAuthFormComponent],
      providers: [provideHttpClient(), provideHttpClientTesting(), provideRouter([])]
    }).compileComponents();

    fixture = TestBed.createComponent(SmbJoinAuthFormComponent);
    component = fixture.componentInstance;
    component.ngOnInit();
    createJoinAuth = spyOn(TestBed.inject(SmbService), 'createJoinAuth');
    getJoinAuth = spyOn(TestBed.inject(SmbService), 'getJoinAuth');
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should set form invalid if any required fields are missing', () => {
    component.form.controls['authId'].setValue('');
    component.form.controls['username'].setValue('');
    component.form.controls['password'].setValue('');
    expect(component.form.valid).not.toBeNull();
  });

  it('should submit the form', () => {
    component.form.controls['authId'].setValue('foo');
    component.form.controls['username'].setValue('user');
    component.form.controls['password'].setValue('pass');
    component.form.controls['linkedToCluster'].setValue(undefined);

    component.submit();

    expect(createJoinAuth).toHaveBeenCalledWith(FOO_JOIN_AUTH);
  });

  describe('when editing', () => {
    beforeEach(() => {
      component.editing = true;
      getJoinAuth.and.returnValue(of(FOO_JOIN_AUTH));
      component.ngOnInit();
      fixture.detectChanges();
    });

    it('should get resource data and set form fields with it', () => {
      expect(getJoinAuth).toHaveBeenCalled();
      expect(component.form.value).toEqual({
        authId: 'foo',
        username: 'user',
        password: 'pass',
        linkedToCluster: undefined
      });
    });
  });
});
