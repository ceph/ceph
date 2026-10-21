import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';

import { ModalModule } from 'carbon-components-angular';

import { PoolAction, PoolBusyReason } from '../pool';
import { PoolBusyModalComponent } from './pool-busy-modal.component';

describe('PoolBusyModalComponent', () => {
  let fixture: ComponentFixture<PoolBusyModalComponent>;

  const createComponent = (
    reason: PoolBusyReason,
    taskDescription?: string,
    action: PoolAction = PoolAction.MIGRATE
  ) => {
    TestBed.configureTestingModule({
      imports: [ModalModule],
      declarations: [PoolBusyModalComponent],
      providers: [
        { provide: 'reason', useValue: reason },
        { provide: 'taskDescription', useValue: taskDescription },
        { provide: 'action', useValue: action }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    });
    fixture = TestBed.createComponent(PoolBusyModalComponent);
    fixture.detectChanges();
  };

  const title = () =>
    fixture.debugElement
      .query(By.css('[cdsModalHeaderHeading]'))
      .nativeElement.textContent.replace(/\s+/g, ' ')
      .trim();

  const description = () =>
    fixture.debugElement
      .query(By.css('[data-testid="description"]'))
      .nativeElement.textContent.replace(/\s+/g, ' ')
      .trim();

  it('should create', () => {
    createComponent(PoolBusyReason.MIGRATING);
    expect(fixture.componentInstance).toBeTruthy();
  });

  describe('migrate action', () => {
    it('should show the migrate title', () => {
      createComponent(PoolBusyReason.MIGRATING, undefined, PoolAction.MIGRATE);
      expect(title()).toBe('Cannot migrate pool');
    });

    it('should tell the user to wait for an ongoing migration', () => {
      createComponent(PoolBusyReason.MIGRATING, undefined, PoolAction.MIGRATE);
      expect(description()).toContain('already part of an ongoing migration');
      expect(description()).toContain('Wait for it to finish');
    });

    it('should tell the user to wait for the PG count change', () => {
      createComponent(PoolBusyReason.PG_CHANGE, undefined, PoolAction.MIGRATE);
      expect(description()).toContain('number of PGs of this pool is being adjusted');
      expect(description()).toContain('Wait for the change to finish');
    });

    it('should include the running task for any other operation', () => {
      createComponent(PoolBusyReason.TASK, 'Deleting...', PoolAction.MIGRATE);
      expect(description()).toContain(
        'another operation is in progress on this pool (Deleting...)'
      );
      expect(description()).toContain('Wait for it to finish');
    });
  });

  describe('edit action', () => {
    it('should show the edit title', () => {
      createComponent(PoolBusyReason.MIGRATING, undefined, PoolAction.EDIT);
      expect(title()).toBe('Cannot edit pool');
    });

    it('should tell the user to wait for an ongoing migration', () => {
      createComponent(PoolBusyReason.MIGRATING, undefined, PoolAction.EDIT);
      expect(description()).toContain('Pool editing is currently unavailable');
      expect(description()).toContain('already part of an ongoing migration');
      expect(description()).toContain('Wait for it to finish');
    });

    it('should tell the user to wait for the PG count change', () => {
      createComponent(PoolBusyReason.PG_CHANGE, undefined, PoolAction.EDIT);
      expect(description()).toContain('Pool editing is currently unavailable');
      expect(description()).toContain('number of PGs of this pool is being adjusted');
      expect(description()).toContain('Wait for the change to finish');
    });

    it('should include the running task for any other operation', () => {
      createComponent(PoolBusyReason.TASK, 'Updating...', PoolAction.EDIT);
      expect(description()).toContain('Pool editing is currently unavailable');
      expect(description()).toContain(
        'another operation is in progress on this pool (Updating...)'
      );
      expect(description()).toContain('Wait for it to finish');
    });
  });

  describe('delete action', () => {
    it('should show the delete title', () => {
      createComponent(PoolBusyReason.MIGRATING, undefined, PoolAction.DELETE);
      expect(title()).toBe('Cannot delete pool');
    });

    it('should tell the user to wait for an ongoing migration', () => {
      createComponent(PoolBusyReason.MIGRATING, undefined, PoolAction.DELETE);
      expect(description()).toContain('Pool deletion is currently unavailable');
      expect(description()).toContain('already part of an ongoing migration');
      expect(description()).toContain('Wait for it to finish');
    });

    it('should tell the user to wait for the PG count change', () => {
      createComponent(PoolBusyReason.PG_CHANGE, undefined, PoolAction.DELETE);
      expect(description()).toContain('Pool deletion is currently unavailable');
      expect(description()).toContain('number of PGs of this pool is being adjusted');
      expect(description()).toContain('Wait for the change to finish');
    });

    it('should include the running task for any other operation', () => {
      createComponent(PoolBusyReason.TASK, 'Migrating...', PoolAction.DELETE);
      expect(description()).toContain('Pool deletion is currently unavailable');
      expect(description()).toContain(
        'another operation is in progress on this pool (Migrating...)'
      );
      expect(description()).toContain('Wait for it to finish');
    });
  });

  it('should offer no actions, only the close button of the header', () => {
    createComponent(PoolBusyReason.MIGRATING);
    const body = fixture.debugElement.query(By.css('[cdsModalContent]')).nativeElement;
    expect(body.querySelector('button, a, cd-form-button-panel')).toBeNull();
    expect(fixture.debugElement.query(By.css('cds-modal-footer'))).toBeNull();
  });
});
