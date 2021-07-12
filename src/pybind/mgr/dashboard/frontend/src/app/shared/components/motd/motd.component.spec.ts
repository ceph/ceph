import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DashboardModule } from '~/app/ceph/dashboard/dashboard.module';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { MotdComponent } from './motd.component';

describe('MotdComponent', () => {
  let component: MotdComponent;
  let fixture: ComponentFixture<MotdComponent>;

  configureTestBed({
    imports: [DashboardModule, HttpClientTestingModule, SharedModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(MotdComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
