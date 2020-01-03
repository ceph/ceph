import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastrModule } from 'ngx-toastr';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { SharedModule } from '../../../shared/shared.module';
import { UserPasswordLoginFormComponent } from './user-password-login-form.component';

describe('UserPasswordLoginFormComponent', () => {
  let component: UserPasswordLoginFormComponent;
  let fixture: ComponentFixture<UserPasswordLoginFormComponent>;

  configureTestBed(
    {
      imports: [
        HttpClientTestingModule,
        RouterTestingModule,
        ReactiveFormsModule,
        ToastrModule.forRoot(),
        SharedModule
      ],
      declarations: [UserPasswordLoginFormComponent],
      providers: i18nProviders
    },
    true
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(UserPasswordLoginFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
