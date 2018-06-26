import { HttpClientTestingModule } from '@angular/common/http/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { ModalModule } from 'ngx-bootstrap';

import { RgwUserService } from '../../../shared/api/rgw-user.service';
import { SharedModule } from '../../../shared/shared.module';
import { configureTestBed } from '../../../shared/unit-test-helper';
import { RgwUserListComponent } from './rgw-user-list.component';

describe('RgwUserListComponent', () => {
  let component: RgwUserListComponent;
  let fixture: ComponentFixture<RgwUserListComponent>;

  configureTestBed({
    declarations: [RgwUserListComponent],
    imports: [RouterTestingModule, HttpClientTestingModule, ModalModule.forRoot(), SharedModule],
    providers: [RgwUserService],
    schemas: [NO_ERRORS_SCHEMA]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwUserListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
