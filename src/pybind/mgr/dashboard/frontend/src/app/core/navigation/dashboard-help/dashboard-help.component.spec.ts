import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { DashboardHelpComponent } from './dashboard-help.component';

describe('DashboardHelpComponent', () => {
  let component: DashboardHelpComponent;
  let fixture: ComponentFixture<DashboardHelpComponent>;

  configureTestBed({
    imports: [HttpClientTestingModule, SharedModule, RouterTestingModule],
    declarations: [DashboardHelpComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(DashboardHelpComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
