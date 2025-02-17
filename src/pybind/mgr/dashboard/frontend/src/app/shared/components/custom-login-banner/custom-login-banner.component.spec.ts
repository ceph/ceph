import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { configureTestBed } from '~/testing/unit-test-helper';
import { CustomLoginBannerComponent } from './custom-login-banner.component';

describe('CustomLoginBannerComponent', () => {
  let component: CustomLoginBannerComponent;
  let fixture: ComponentFixture<CustomLoginBannerComponent>;

  configureTestBed({
    declarations: [CustomLoginBannerComponent],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CustomLoginBannerComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
