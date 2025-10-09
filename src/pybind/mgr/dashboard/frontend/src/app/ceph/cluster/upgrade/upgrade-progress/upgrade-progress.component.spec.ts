import { ComponentFixture, TestBed } from '@angular/core/testing';

import { UpgradeProgressComponent } from './upgrade-progress.component';
import { ToastrModule } from 'ngx-toastr';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { SharedModule } from '~/app/shared/shared.module';
import { RouterTestingModule } from '@angular/router/testing';
import { LogsComponent } from '../../logs/logs.component';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('UpgradeProgressComponent', () => {
  let component: UpgradeProgressComponent;
  let fixture: ComponentFixture<UpgradeProgressComponent>;

  configureTestBed({
    declarations: [UpgradeProgressComponent, LogsComponent],
    imports: [ToastrModule.forRoot(), HttpClientTestingModule, SharedModule, RouterTestingModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(UpgradeProgressComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
