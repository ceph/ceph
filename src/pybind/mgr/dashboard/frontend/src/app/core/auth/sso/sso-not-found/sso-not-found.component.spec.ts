import { ComponentFixture, TestBed } from '@angular/core/testing';

import { configureTestBed } from '../../../../../testing/unit-test-helper';
import { SsoNotFoundComponent } from './sso-not-found.component';

describe('SsoNotFoundComponent', () => {
  let component: SsoNotFoundComponent;
  let fixture: ComponentFixture<SsoNotFoundComponent>;

  configureTestBed({
    declarations: [SsoNotFoundComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(SsoNotFoundComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should render the correct logout url', () => {
    const expectedUrl = `http://localhost/auth/saml2/slo`;
    const logoutAnchor = fixture.debugElement.nativeElement.querySelector('.sso-logout');

    expect(logoutAnchor.href).toEqual(expectedUrl);
  });
});
