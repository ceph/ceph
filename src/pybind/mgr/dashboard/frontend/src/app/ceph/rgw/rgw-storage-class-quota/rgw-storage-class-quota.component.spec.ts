import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { of, throwError } from 'rxjs';
import { CheckboxModule, InputModule, NumberModule } from 'carbon-components-angular';

import { RgwZonegroupService } from '~/app/shared/api/rgw-zonegroup.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { RgwStorageClassQuotaComponent } from './rgw-storage-class-quota.component';

describe('RgwStorageClassQuotaComponent', () => {
  let component: RgwStorageClassQuotaComponent;
  let fixture: ComponentFixture<RgwStorageClassQuotaComponent>;
  let zonegroupService: RgwZonegroupService;

  const zonegroupInfo = {
    zonegroups: [
      {
        name: 'default',
        placement_targets: [
          {
            name: 'default-placement',
            storage_classes: ['STANDARD', 'COLD'],
            tier_targets: []
          }
        ]
      }
    ]
  };

  configureTestBed({
    imports: [
      HttpClientTestingModule,
      ReactiveFormsModule,
      SharedModule,
      CheckboxModule,
      InputModule,
      NumberModule
    ],
    declarations: [RgwStorageClassQuotaComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwStorageClassQuotaComponent);
    component = fixture.componentInstance;
    zonegroupService = TestBed.inject(RgwZonegroupService);
  });

  it('should show STANDARD before zonegroup info loads', () => {
    spyOn(zonegroupService, 'getAllZonegroupsInfo').and.returnValue(of(zonegroupInfo));
    expect(component.getStorageClassQuotas().map((quota) => quota.storage_class)).toEqual([
      'STANDARD'
    ]);
    fixture.detectChanges();
    expect(component.quotas.length).toBe(2);
  });

  it('should create quota rows for each storage class', () => {
    spyOn(zonegroupService, 'getAllZonegroupsInfo').and.returnValue(of(zonegroupInfo));
    fixture.detectChanges();
    expect(component.quotas.length).toBe(2);
    expect(component.getStorageClassQuotas().map((quota) => quota.storage_class)).toEqual([
      'STANDARD',
      'COLD'
    ]);
  });

  it('should fall back to STANDARD when zonegroup lookup fails', () => {
    spyOn(zonegroupService, 'getAllZonegroupsInfo').and.returnValue(
      throwError(() => new Error('unavailable'))
    );
    fixture.detectChanges();
    expect(component.getStorageClassQuotas()).toEqual([
      {
        storage_class: 'STANDARD',
        enabled: false,
        max_size: -1,
        max_objects: -1
      }
    ]);
  });

  it('should render a custom title and description', () => {
    spyOn(zonegroupService, 'getAllZonegroupsInfo').and.returnValue(of(zonegroupInfo));
    component.title = 'User storage class quotas';
    component.description = 'Limits this user per storage class.';
    fixture.detectChanges();
    const compiled: HTMLElement = fixture.nativeElement;
    expect(compiled.textContent).toContain('User storage class quotas');
    expect(compiled.textContent).toContain('Limits this user per storage class.');
  });

  it('should populate saved quotas for a storage class', () => {
    spyOn(zonegroupService, 'getAllZonegroupsInfo').and.returnValue(of(zonegroupInfo));
    component.savedQuotas = [
      {
        storage_class: 'COLD',
        enabled: true,
        max_size: 1073741824,
        max_objects: 50
      }
    ];
    fixture.detectChanges();
    const cold = component.getStorageClassQuotas().find((quota) => quota.storage_class === 'COLD');
    expect(cold).toEqual({
      storage_class: 'COLD',
      enabled: true,
      max_size: 1073741824,
      max_objects: 50
    });
  });
});
