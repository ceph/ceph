import { HttpClientTestingModule } from '@angular/common/http/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastrModule } from 'ngx-toastr';

import { RbdService } from '~/app/shared/api/rbd.service';
import { CssHelper } from '~/app/shared/classes/css-helper';
import { PipesModule } from '~/app/shared/pipes/pipes.module';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { configureTestBed } from '~/testing/unit-test-helper';
import { WorkbenchLayoutComponent } from './workbench-layout.component';

describe('WorkbenchLayoutComponent', () => {
  let component: WorkbenchLayoutComponent;
  let fixture: ComponentFixture<WorkbenchLayoutComponent>;

  configureTestBed({
    imports: [RouterTestingModule, ToastrModule.forRoot(), PipesModule, HttpClientTestingModule],
    declarations: [WorkbenchLayoutComponent],
    schemas: [NO_ERRORS_SCHEMA],
    providers: [AuthStorageService, CssHelper, RbdService]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(WorkbenchLayoutComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
