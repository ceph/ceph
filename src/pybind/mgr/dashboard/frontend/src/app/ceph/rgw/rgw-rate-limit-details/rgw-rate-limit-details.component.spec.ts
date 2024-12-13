import { SharedModule } from '~/app/shared/shared.module';
import { RgwRateLimitDetailsComponent } from './rgw-rate-limit-details.component';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('RgwRateLimitDetailsComponent', () => {
  let component: RgwRateLimitDetailsComponent;
  let fixture: ComponentFixture<RgwRateLimitDetailsComponent>;
  configureTestBed({
    declarations: [RgwRateLimitDetailsComponent],
    imports: [HttpClientTestingModule, SharedModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwRateLimitDetailsComponent);
    component = fixture.componentInstance;
    component.type = 'user';
    component.rateLimitConfig = {
      enabled: true,
      max_read_bytes: 987648,
      max_read_ops: 0,
      max_write_bytes: 0,
      max_write_ops: 9876543212
    };
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
