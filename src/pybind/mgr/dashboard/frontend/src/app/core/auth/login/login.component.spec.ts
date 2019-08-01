import { HttpClientTestingModule } from '@angular/common/http/testing';
import { Injectable, NgModule } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastPackage, ToastRef, ToastrModule } from 'ngx-toastr';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { AuthModule } from '../auth.module';
import { LoginComponent } from './login.component';

import { BrowserAnimationsModule } from '@angular/platform-browser/animations';

@Injectable() // https://github.com/scttcper/ngx-toastr/issues/339
class MockToastPackage extends ToastPackage {
  constructor() {
    super(1, null, 'Test Message', 'Test Title', 'show', new ToastRef(null));
  }
}
@NgModule({
  providers: [{ provide: ToastPackage, useClass: MockToastPackage }],
  imports: [ToastrModule.forRoot(), BrowserAnimationsModule],
  exports: [ToastrModule]
})
export class ToastrTestingModule {}

describe('LoginComponent', () => {
  let component: LoginComponent;
  let fixture: ComponentFixture<LoginComponent>;

  configureTestBed({
    imports: [RouterTestingModule, HttpClientTestingModule, AuthModule, ToastrTestingModule],
    providers: i18nProviders
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(LoginComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should ensure no modal dialogs are opened', () => {
    component['bsModalService']['modalsCount'] = 2;
    component.ngOnInit();
    expect(component['bsModalService'].getModalsCount()).toBe(0);
  });
});
