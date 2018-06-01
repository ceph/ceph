import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastModule } from 'ng2-toastr';

import { AppComponent } from './app.component';
import { AuthStorageService } from './shared/services/auth-storage.service';
import { configureTestBed } from './shared/unit-test-helper';

describe('AppComponent', () => {
  let component: AppComponent;
  let fixture: ComponentFixture<AppComponent>;

  const fakeService = {
    isLoggedIn: () => {
      return true;
    }
  };

  configureTestBed({
    imports: [RouterTestingModule, ToastModule.forRoot()],
    declarations: [AppComponent],
    schemas: [NO_ERRORS_SCHEMA],
    providers: [{ provide: AuthStorageService, useValue: fakeService }]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(AppComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
