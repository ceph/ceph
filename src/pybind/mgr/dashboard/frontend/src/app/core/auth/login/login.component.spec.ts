import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { AuthModule } from '../auth.module';
import { LoginComponent } from './login.component';

describe('LoginComponent', () => {
  let component: LoginComponent;
  let fixture: ComponentFixture<LoginComponent>;

  configureTestBed({
    imports: [RouterTestingModule, HttpClientTestingModule, AuthModule]
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
