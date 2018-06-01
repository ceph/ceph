import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { BsModalService } from 'ngx-bootstrap';

import { RgwUserService } from '../../../shared/api/rgw-user.service';
import { configureTestBed } from '../../../shared/unit-test-helper';
import { RgwUserListComponent } from './rgw-user-list.component';

describe('RgwUserListComponent', () => {
  let component: RgwUserListComponent;
  let fixture: ComponentFixture<RgwUserListComponent>;

  const fakeService = {};

  configureTestBed({
    declarations: [RgwUserListComponent],
    imports: [RouterTestingModule],
    providers: [
      { provide: RgwUserService, useValue: fakeService },
      { provide: BsModalService, useValue: fakeService }
    ],
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
