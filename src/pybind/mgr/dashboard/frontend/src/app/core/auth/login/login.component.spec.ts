import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { AuthService } from '../../../shared/api/auth.service';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { configureTestBed } from '../../../shared/unit-test-helper';
import { LoginComponent } from './login.component';

describe('LoginComponent', () => {
  let component: LoginComponent;
  let fixture: ComponentFixture<LoginComponent>;

  const fakeService = {};

  configureTestBed({
    imports: [FormsModule, RouterTestingModule],
    declarations: [LoginComponent],
    providers: [{ provide: AuthService, useValue: fakeService }, AuthStorageService]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(LoginComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
