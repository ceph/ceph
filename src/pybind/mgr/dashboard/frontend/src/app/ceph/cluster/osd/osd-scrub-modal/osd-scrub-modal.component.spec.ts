import { NO_ERRORS_SCHEMA } from '@angular/core';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';

import { BsModalRef } from 'ngx-bootstrap';

import { OsdService } from '../../../../shared/api/osd.service';
import { NotificationService } from '../../../../shared/services/notification.service';
import { OsdScrubModalComponent } from './osd-scrub-modal.component';

describe('OsdScrubModalComponent', () => {
  let component: OsdScrubModalComponent;
  let fixture: ComponentFixture<OsdScrubModalComponent>;

  const fakeService = {
    list: () => {
      return new Promise(function(resolve, reject) {
        return {};
      });
    },
    scrub: (data: any) => {
      return new Promise(function(resolve, reject) {
        return {};
      });
    },
    scrub_many: (data: any) => {
      return new Promise(function(resolve, reject) {
        return {};
      });
    }
  };

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [ReactiveFormsModule],
      declarations: [OsdScrubModalComponent],
      schemas: [NO_ERRORS_SCHEMA],
      providers: [
        BsModalRef,
        { provide: OsdService, useValue: fakeService },
        { provide: NotificationService, useValue: fakeService }
      ]
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(OsdScrubModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
