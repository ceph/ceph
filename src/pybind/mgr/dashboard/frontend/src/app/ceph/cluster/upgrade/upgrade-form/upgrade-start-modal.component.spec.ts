import { ComponentFixture, TestBed } from '@angular/core/testing';

import { UpgradeComponent } from '../upgrade.component';
import { configureTestBed } from '~/testing/unit-test-helper';
import { UpgradeService } from '~/app/shared/api/upgrade.service';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { SharedModule } from '~/app/shared/shared.module';
import { ToastrModule } from 'ngx-toastr';
import { RouterTestingModule } from '@angular/router/testing';

describe('UpgradeComponent', () => {
  let component: UpgradeComponent;
  let fixture: ComponentFixture<UpgradeComponent>;

  configureTestBed({
    imports: [HttpClientTestingModule, SharedModule, ToastrModule.forRoot(), RouterTestingModule],
    schemas: [NO_ERRORS_SCHEMA],
    declarations: [UpgradeComponent],
    providers: [UpgradeService]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(UpgradeComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
