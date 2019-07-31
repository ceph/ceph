import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';

import { BsModalRef } from 'ngx-bootstrap/modal';

import { configureTestBed, i18nProviders } from '../../../../../testing/unit-test-helper';
import { OsdService } from '../../../../shared/api/osd.service';
import { NotificationService } from '../../../../shared/services/notification.service';
import { OsdScrubModalComponent } from './osd-scrub-modal.component';

describe('OsdScrubModalComponent', () => {
  let component: OsdScrubModalComponent;
  let fixture: ComponentFixture<OsdScrubModalComponent>;

  const fakeService = {
    list: () => {
      return new Promise(() => {});
    },
    scrub: () => {
      return new Promise(() => {});
    },
    scrub_many: () => {
      return new Promise(() => {});
    }
  };

  configureTestBed({
    imports: [ReactiveFormsModule],
    declarations: [OsdScrubModalComponent],
    schemas: [NO_ERRORS_SCHEMA],
    providers: [
      BsModalRef,
      { provide: OsdService, useValue: fakeService },
      { provide: NotificationService, useValue: fakeService },
      i18nProviders
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(OsdScrubModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
