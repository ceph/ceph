import { TestBed } from '@angular/core/testing';

import { configureTestBed, i18nProviders } from '../../../testing/unit-test-helper';
import { RbdConfigurationType } from '../models/configuration';
import { RbdConfigurationService } from './rbd-configuration.service';

describe('RbdConfigurationService', () => {
  let service: RbdConfigurationService;

  configureTestBed({
    providers: [RbdConfigurationService, i18nProviders]
  });

  beforeEach(() => {
    service = TestBed.inject(RbdConfigurationService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should filter config options', () => {
    const result = service.getOptionByName('rbd_qos_write_iops_burst');
    expect(result).toEqual({
      name: 'rbd_qos_write_iops_burst',
      displayName: 'Write IOPS Burst',
      description: 'The desired burst limit of write operations.',
      type: RbdConfigurationType.iops
    });
  });

  it('should return the display name', () => {
    const displayName = service.getDisplayName('rbd_qos_write_iops_burst');
    expect(displayName).toBe('Write IOPS Burst');
  });

  it('should return the description', () => {
    const description = service.getDescription('rbd_qos_write_iops_burst');
    expect(description).toBe('The desired burst limit of write operations.');
  });

  it('should have a class for each section', () => {
    service.sections.forEach((section) => expect(section.class).toBeTruthy());
  });
});
