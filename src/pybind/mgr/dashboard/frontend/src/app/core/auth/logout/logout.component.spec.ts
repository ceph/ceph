import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { AuthService } from '../../../shared/api/auth.service';
import { configureTestBed } from '../../../shared/unit-test-helper';
import { LogoutComponent } from './logout.component';

describe('LogoutComponent', () => {
  let component: LogoutComponent;
  let fixture: ComponentFixture<LogoutComponent>;

  const fakeService = {};

  configureTestBed({
    imports: [RouterTestingModule],
    declarations: [LogoutComponent],
    providers: [{ provide: AuthService, useValue: fakeService }]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(LogoutComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
