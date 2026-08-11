import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute } from '@angular/router';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { BehaviorSubject } from 'rxjs';

import { RbdImageResourcePageComponent } from './rbd-image-resource-page.component';
import { RbdImageResourceStateService } from '../../../shared/services/rbd-image-resource-state.service';
import { RbdConfigurationService } from '~/app/shared/services/rbd-configuration.service';
import { CdDatePipe } from '~/app/shared/pipes/cd-date.pipe';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';
import { DimlessPipe } from '~/app/shared/pipes/dimless.pipe';
import { RbdFormModel } from '../rbd-form/rbd-form.model';

describe('RbdImageResourcePageComponent', () => {
  let component: RbdImageResourcePageComponent;
  let fixture: ComponentFixture<RbdImageResourcePageComponent>;
  let rbdConfigurationServiceSpy: jest.Mocked<RbdConfigurationService>;
  let imageSubject: BehaviorSubject<RbdFormModel | null>;

  const baseMockImage: any = {
    name: 'test-image',
    pool_name: 'test-pool',
    namespace: 'test-namespace',
    data_pool: 'test-data-pool',
    timestamp: '2024-01-01T00:00:00Z',
    size: 1024,
    num_objs: 10,
    obj_size: 102,
    features_name: ['fast-diff', 'exclusive-lock'],
    disk_usage: 512,
    total_disk_usage: 1024,
    stripe_unit: 128,
    stripe_count: 1,
    parent: {
      pool_name: 'parent-pool',
      pool_namespace: 'parent-ns',
      image_name: 'parent-image',
      snap_name: 'parent-snap'
    },
    block_name_prefix: 'rbd_data.123',
    order: 22,
    image_format: 2,
    mirror_mode: ['image', 'journal'],
    primary: true,
    configuration: [{ name: 'rbd_cache', source: 0, value: 'true' }]
  };

  class MockDimlessBinaryPipe {
    transform(value: any): string {
      return `${value} B`;
    }
  }

  class MockDimlessPipe {
    transform(value: any): string {
      return `${value} items`;
    }
  }

  class MockCdDatePipe {
    transform(value: any): string {
      return `Date: ${value}`;
    }
  }

  beforeEach(async () => {
    imageSubject = new BehaviorSubject<RbdFormModel | null>(null);

    // Create Jest mocks for services
    const rbdImageResourceStateServiceMock = {
      image$: imageSubject.asObservable()
    };

    rbdConfigurationServiceSpy = {
      getOptionByName: jest.fn().mockReturnValue({ displayName: 'RBD Cache', type: 'boolean' })
    } as any;

    const activatedRouteMock = {
      snapshot: { data: { section: 'overview' } }
    };

    await TestBed.configureTestingModule({
      declarations: [RbdImageResourcePageComponent],
      providers: [
        { provide: ActivatedRoute, useValue: activatedRouteMock },
        { provide: RbdImageResourceStateService, useValue: rbdImageResourceStateServiceMock },
        { provide: RbdConfigurationService, useValue: rbdConfigurationServiceSpy },
        { provide: DimlessBinaryPipe, useClass: MockDimlessBinaryPipe },
        { provide: DimlessPipe, useClass: MockDimlessPipe },
        { provide: CdDatePipe, useClass: MockCdDatePipe }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RbdImageResourcePageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges(); // Triggers ngOnInit
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should initialize section from route data', () => {
    expect(component.section).toBe('overview');
  });

  it('should handle null image gracefully', () => {
    imageSubject.next(null);
    fixture.detectChanges();

    expect(component.notFound).toBe(true);
    expect(component.selection).toBeUndefined();
    expect(component.overviewFields).toEqual([]);
    expect(component.rbdDashboardUrl).toBe('');
  });

  it('should process a valid image and enrich configuration', () => {
    imageSubject.next({ ...baseMockImage } as RbdFormModel);
    fixture.detectChanges();

    expect(component.notFound).toBe(false);
    expect(component.selection).toBeDefined();
    expect(component.rbdDashboardUrl).toBe('rbd-details?var-pool=test-pool&var-image=test-image');

    // Verify configuration was enriched
    expect(rbdConfigurationServiceSpy.getOptionByName).toHaveBeenCalledWith('rbd_cache');
    expect(component.selection.configuration?.[0]).toEqual(
      expect.objectContaining({ name: 'rbd_cache', displayName: 'RBD Cache' })
    );
  });

  describe('Overview Fields Generation', () => {
    beforeEach(() => {
      imageSubject.next({ ...baseMockImage } as RbdFormModel);
      fixture.detectChanges();
    });

    it('should map base properties and pipes correctly', () => {
      const getField = (label: string) =>
        component.overviewFields.find((f) => f.label === label)?.value;

      expect(getField('Name')).toBe('test-image');
      expect(getField('Size')).toBe('1024 B');
      expect(getField('Objects')).toBe('10 items');
      expect(getField('Created')).toBe('Date: 2024-01-01T00:00:00Z');
      expect(getField('Order')).toBe(22);
    });

    it('should calculate disk usage percentage when fast-diff is present', () => {
      // 512 / 1024 = 50%
      const usageField = component.overviewFields.find((f) => f.label === 'Usage');
      expect(usageField?.value).toBe('50.00%');
    });
  });

  describe('Edge cases and helper methods', () => {
    let testImage: any;

    beforeEach(() => {
      // Create a fresh clone of the base mock for each test to modify
      testImage = JSON.parse(JSON.stringify(baseMockImage));
    });

    it('should return N/A for usage if fast-diff is missing', () => {
      testImage.features_name = ['exclusive-lock'];
      imageSubject.next(testImage as RbdFormModel);

      const usageField = component.overviewFields.find((f) => f.label === 'Usage');
      expect(usageField?.value).toBe('N/A');
    });

    it('should return 0% for usage if size is 0 or falsy', () => {
      testImage.size = 0;
      imageSubject.next(testImage as RbdFormModel);

      const usageField = component.overviewFields.find((f) => f.label === 'Usage');
      expect(usageField?.value).toBe('0%');
    });

    it('should return N/A for provisioned if fast-diff is missing', () => {
      testImage.features_name = [];
      imageSubject.next(testImage as RbdFormModel);

      const provField = component.overviewFields.find((f) => f.label === 'Provisioned');
      expect(provField?.value).toBe('N/A');
    });

    it('should calculate provisioned values when fast-diff is present', () => {
      imageSubject.next(testImage as RbdFormModel);

      const provField = component.overviewFields.find((f) => f.label === 'Provisioned');
      const totalProvField = component.overviewFields.find((f) => f.label === 'Total provisioned');

      expect(provField?.value).toBe('512 B');
      expect(totalProvField?.value).toBe('1024 B');
    });

    it('should format parent string with and without namespace', () => {
      // With namespace (from base mock)
      imageSubject.next(testImage as RbdFormModel);
      expect(component.overviewFields.find((f) => f.label === 'Parent')?.value).toBe(
        'parent-pool/parent-ns/parent-image@parent-snap'
      );

      // Without namespace
      delete testImage.parent.pool_namespace;
      imageSubject.next(testImage as RbdFormModel);
      expect(component.overviewFields.find((f) => f.label === 'Parent')?.value).toBe(
        'parent-pool/parent-image@parent-snap'
      );

      // Null parent
      testImage.parent = null;
      imageSubject.next(testImage as RbdFormModel);
      expect(component.overviewFields.find((f) => f.label === 'Parent')?.value).toBeUndefined();
    });

    it('should format mirroring string properly based on array/string and primary flag', () => {
      // Array mode + primary
      imageSubject.next(testImage as RbdFormModel);
      expect(component.overviewFields.find((f) => f.label === 'Mirroring')?.value).toBe(
        'image / journal / primary'
      );

      // String mode + secondary
      testImage.mirror_mode = 'pool';
      testImage.primary = false;
      imageSubject.next(testImage as RbdFormModel);
      expect(component.overviewFields.find((f) => f.label === 'Mirroring')?.value).toBe(
        'pool / secondary'
      );
    });

    it('should determine next scheduled snapshot properly', () => {
      // From mirror_mode array
      testImage.mirror_mode = ['image', 'journal', '2024-12-31T00:00:00Z'];
      imageSubject.next(testImage as RbdFormModel);
      expect(
        component.overviewFields.find((f) => f.label === 'Next Scheduled Snapshot')?.value
      ).toBe('Date: 2024-12-31T00:00:00Z');

      // From schedule_info
      testImage.mirror_mode = 'pool';
      testImage.schedule_info = { schedule_time: '2025-01-01T00:00:00Z' };
      imageSubject.next(testImage as RbdFormModel);
      expect(
        component.overviewFields.find((f) => f.label === 'Next Scheduled Snapshot')?.value
      ).toBe('Date: 2025-01-01T00:00:00Z');

      // Fallback
      delete testImage.schedule_info;
      imageSubject.next(testImage as RbdFormModel);
      expect(
        component.overviewFields.find((f) => f.label === 'Next Scheduled Snapshot')?.value
      ).toBeUndefined();
    });
  });

  it('should unsubscribe on destroy', () => {
    const subSpy = jest.spyOn(component['sub'], 'unsubscribe');
    component.ngOnDestroy();
    expect(subSpy).toHaveBeenCalled();
  });
});
