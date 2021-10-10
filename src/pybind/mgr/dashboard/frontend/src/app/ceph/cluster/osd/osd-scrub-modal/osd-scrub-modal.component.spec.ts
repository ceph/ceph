import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

import { OsdScrubModalComponent } from './osd-scrub-modal.component';
import { OsdService } from '~/app/shared/api/osd.service';
import { JoinPipe } from '~/app/shared/pipes/join.pipe';
import { NotificationService } from '~/app/shared/services/notification.service';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('OsdScrubModalComponent', () => {
  let component: OsdScrubModalComponent;
  let fixture: ComponentFixture<OsdScrubModalComponent>;

  const fakeService = {
    list: () => new Promise(() => undefined),
    scrub: () => new Promise(() => undefined),
    scrub_many: () => new Promise(() => undefined)
  };

  configureTestBed({
    imports: [ReactiveFormsModule],
    declarations: [OsdScrubModalComponent, JoinPipe],
    schemas: [NO_ERRORS_SCHEMA],
    providers: [
      NgbActiveModal,
      JoinPipe,
      { provide: OsdService, useValue: fakeService },
      { provide: NotificationService, useValue: fakeService }
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
