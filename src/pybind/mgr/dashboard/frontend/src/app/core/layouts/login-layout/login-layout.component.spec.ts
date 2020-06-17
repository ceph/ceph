import { HttpClientTestingModule } from '@angular/common/http/testing';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';

import { BsDatepickerModule } from 'ngx-bootstrap/datepicker';

import { RouterTestingModule } from '@angular/router/testing';
import { SharedModule } from '../../../shared/shared.module';
import { LoginLayoutComponent } from './login-layout.component';

describe('LoginLayoutComponent', () => {
  let component: LoginLayoutComponent;
  let fixture: ComponentFixture<LoginLayoutComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [LoginLayoutComponent],
      imports: [
        BrowserAnimationsModule,
        BsDatepickerModule.forRoot(),
        HttpClientTestingModule,
        RouterTestingModule,
        SharedModule
      ]
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(LoginLayoutComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
