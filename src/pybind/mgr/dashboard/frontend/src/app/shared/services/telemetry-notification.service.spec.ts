import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { TelemetryNotificationService } from './telemetry-notification.service';

describe('TelemetryNotificationService', () => {
  let service: TelemetryNotificationService;

  configureTestBed({
    providers: [TelemetryNotificationService]
  });

  beforeEach(() => {
    service = TestBed.inject(TelemetryNotificationService);
    spyOn(service.update, 'emit');
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should set notification visibility to true', () => {
    service.setVisibility(true);
    expect(service.visible).toBe(true);
    expect(service.update.emit).toHaveBeenCalledWith(true);
  });

  it('should set notification visibility to false', () => {
    service.setVisibility(false);
    expect(service.visible).toBe(false);
    expect(service.update.emit).toHaveBeenCalledWith(false);
  });
});
