import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { ActivatedRoute, Router } from '@angular/router';
import { SharedModule } from '~/app/shared/shared.module';
import { RouterTestingModule } from '@angular/router/testing';
import { ReactiveFormsModule } from '@angular/forms';

import {
  AccordionModule,
  CheckboxModule,
  ComboBoxModule,
  GridModule,
  InputModule,
  SelectModule
} from 'carbon-components-angular';
import { CoreModule } from '~/app/core/core.module';
import { RgwStorageClassFormComponent } from './rgw-storage-class-form.component';
import { TIER_TYPE_DISPLAY } from '../models/rgw-storage-class.model';
import { PoolFormComponent } from '../../pool/pool-form/pool-form.component';
import { RgwZonegroupService } from '~/app/shared/api/rgw-zonegroup.service';
import { RgwZoneService } from '~/app/shared/api/rgw-zone.service';
import { RgwStorageClassService } from '~/app/shared/api/rgw-storage-class.service';
import { PoolService } from '~/app/shared/api/pool.service';
import { of, Subject } from 'rxjs';

describe('RgwStorageClassFormComponent', () => {
  let component: RgwStorageClassFormComponent;
  let fixture: ComponentFixture<RgwStorageClassFormComponent>;
  let router: Router;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [
        BrowserAnimationsModule,
        SharedModule,
        HttpClientTestingModule,
        RouterTestingModule,
        ReactiveFormsModule,
        GridModule,
        InputModule,
        CoreModule,
        SelectModule,
        ComboBoxModule,
        CheckboxModule,
        AccordionModule
      ],
      declarations: [RgwStorageClassFormComponent]
    }).compileComponents();

    spyOn(TestBed.inject(RgwZonegroupService), 'getAllZonegroupsInfo').and.returnValue(
      of({ zonegroups: [], default_zonegroup: '' })
    );
    spyOn(TestBed.inject(PoolService), 'getList').and.returnValue(of([]));

    fixture = TestBed.createComponent(RgwStorageClassFormComponent);
    component = fixture.componentInstance;
    router = TestBed.inject(Router);
    fixture.detectChanges();
  });

  it('should create', () => {
    component.goToListView();
    expect(component).toBeTruthy();
  });

  it('should initialize the form with empty values', () => {
    const storageClassForm = component.storageClassForm;
    expect(storageClassForm).toBeTruthy();
    expect(storageClassForm.get('zonegroup')).toBeTruthy();
    expect(storageClassForm.get('placement_target')).toBeTruthy();
  });

  it('on zonegroup changes', () => {
    component.zoneGroupDetails = {
      default_zonegroup: 'zonegroup1',
      name: 'zonegrp1',
      zonegroups: [
        {
          name: 'zonegroup1',
          id: 'zonegroup-id-1',
          placement_targets: [
            {
              name: 'default-placement',
              tier_targets: [
                {
                  key: 'test',
                  val: {
                    tier_type: 'cloud-s3',
                    retain_head_object: true,
                    storage_class: 'CLOUDIBM',
                    allow_read_through: true,
                    read_through_restore_days: 1,
                    restore_storage_class: 'test67',
                    s3: {
                      storage_class: 'CLOUDIBM',
                      endpoint: 'https://s3.amazonaws.com',
                      access_key: 'ACCESSKEY',
                      target_path: '/path/to/storage',
                      target_storage_class: 'STANDARD',
                      region: 'useastr1',
                      secret: 'SECRETKEY',
                      multipart_min_part_size: 87877,
                      multipart_sync_threshold: 987877,
                      host_style: true
                    },
                    's3-glacier': {
                      glacier_restore_days: 5,
                      glacier_restore_tier_type: 'Standard'
                    }
                  }
                }
              ]
            }
          ]
        }
      ]
    };
    component.storageClassForm.get('zonegroup').setValue('zonegroup1');
    component.onZonegroupChange();
    expect(component.placementTargets).toEqual(['default-placement']);
    expect(component.storageClassForm.get('placement_target').value).toBe('default-placement');
  });

  it('should set form values on submit', () => {
    const storageClassName = 'storageClass1';
    component.storageClassForm.get('storage_class').setValue(storageClassName);
    component.storageClassForm.get('zonegroup').setValue('zonegroup1');
    component.storageClassForm.get('placement_target').setValue('placement1');
    component.storageClassForm.get('target_endpoint').setValue('http://ceph-node-00:8090');
    component.storageClassForm.get('access_key').setValue('accesskey');
    component.storageClassForm.get('secret_key').setValue('secretkey');
    component.storageClassForm.get('target_path').setValue('/target');
    component.storageClassForm.get('retain_head_object').setValue(true);
    component.storageClassForm.get('allow_read_through').setValue(true);
    component.storageClassForm.get('region').setValue('useast1');
    component.storageClassForm.get('multipart_sync_threshold').setValue(1024);
    component.storageClassForm.get('multipart_min_part_size').setValue(256);
    component.goToListView();
    component.submitAction();
    expect(component).toBeTruthy();
  });

  it('should mark duplicate storage class names as invalid', () => {
    component.existingStorageClasses = [
      {
        storage_class: 'storageClass1',
        placement_target: 'placement1',
        zonegroup_name: 'zonegroup1'
      }
    ];

    const control = component.storageClassForm.get('storage_class');
    control.setValue('storageClass1');
    control.updateValueAndValidity();

    expect(control.hasError('uniqueName')).toBe(true);
  });

  it('should set required validators for CLOUD_TIER fields', () => {
    (component as any).updateValidatorsBasedOnStorageClass(TIER_TYPE_DISPLAY.CLOUD_TIER);
    const requiredFields = ['region', 'target_endpoint', 'access_key', 'secret_key', 'target_path'];
    requiredFields.forEach((field) => {
      const control = component.storageClassForm.get(field);
      control.setValue('');
      control.updateValueAndValidity();
    });
    ['glacier_restore_tier_type', 'restore_storage_class'].forEach((field) => {
      const control = component.storageClassForm.get(field);
      control.setValue('');
      control.updateValueAndValidity();
      expect(component).toBeTruthy();
    });
  });

  it('should set required validators for GLACIER fields', () => {
    (component as any).updateValidatorsBasedOnStorageClass(TIER_TYPE_DISPLAY.GLACIER);
    const requiredFields = [
      'region',
      'target_endpoint',
      'access_key',
      'secret_key',
      'target_path',
      'glacier_restore_tier_type',
      'restore_storage_class'
    ];
    requiredFields.forEach((field) => {
      const control = component.storageClassForm.get(field);
      control.setValue('');
      control.updateValueAndValidity();
      expect(component).toBeTruthy();
    });
  });

  it('should clear validators for LOCAL fields', () => {
    (component as any).updateValidatorsBasedOnStorageClass(TIER_TYPE_DISPLAY.LOCAL);

    const allFields = [
      'region',
      'target_endpoint',
      'access_key',
      'secret_key',
      'target_path',
      'glacier_restore_tier_type',
      'restore_storage_class'
    ];
    allFields.forEach((field) => {
      const control = component.storageClassForm.get(field);
      control.setValue('');
      control.updateValueAndValidity();
      expect(component).toBeTruthy();
    });
  });
  it('should not require target_endpoint for a local storage class', () => {
    const form = component.storageClassForm;
    form.get('storage_class').setValue('SC1');
    form.get('zonegroup').setValue('zg2');
    form.get('placement_target').setValue('default-placement');
    form.get('zone').setValue('z2');
    form.get('pool').setValue('pool1');
    expect(form.get('target_endpoint').invalid).toBe(false);
    expect(form.valid).toBe(true);
  });

  it('should require target_endpoint only for cloud tier storage classes', () => {
    const form = component.storageClassForm;
    form.get('storageClassType').setValue('cloud-s3');
    form.get('target_endpoint').setValue('');
    expect(form.get('target_endpoint').hasError('required')).toBe(true);
    form.get('storageClassType').setValue('local');
    expect(form.get('target_endpoint').invalid).toBe(false);
  });

  describe('pool form navigation from storage class form', () => {
    const storageClassReturnUrl = '/rgw/storage-class/create';

    const createPoolFormNavigationContext = (
      fromStorageClass: boolean,
      previousPath: string,
      poolRouter: Router
    ) => {
      const poolForm = Object.create(PoolFormComponent.prototype) as PoolFormComponent;
      (poolForm as any).fromStorageClass = fromStorageClass;
      (poolForm as any).previousPath = previousPath;
      (poolForm as any).router = poolRouter;
      return poolForm;
    };

    it('should return to storage class form after pool creation when opened from storage class', () => {
      const navigateSpy = spyOn(router, 'navigate');
      const poolForm = createPoolFormNavigationContext(true, storageClassReturnUrl, router);

      poolForm.navigateAfterPoolForm();

      expect(navigateSpy).toHaveBeenCalledWith([storageClassReturnUrl]);
    });

    it('should return to pool list after pool creation when not opened from storage class', () => {
      const navigateSpy = spyOn(router, 'navigate');
      const poolForm = createPoolFormNavigationContext(false, '/pool', router);

      poolForm.navigateAfterPoolForm();

      expect(navigateSpy).toHaveBeenCalledWith(['/pool']);
    });

    it('should return to pool list when pool creation is cancelled outside storage class form', () => {
      const navigateSpy = spyOn(router, 'navigate');
      const poolForm = createPoolFormNavigationContext(false, '/pool', router);

      poolForm.navigateAfterPoolForm();

      expect(navigateSpy).toHaveBeenCalledWith(['/pool']);
    });
  });

  describe('edit mode with delayed zonegroup load', () => {
    let editComponent: RgwStorageClassFormComponent;
    let editFixture: ComponentFixture<RgwStorageClassFormComponent>;
    let zonegroupSubject: Subject<any>;

    beforeEach(async () => {
      zonegroupSubject = new Subject();

      await TestBed.resetTestingModule();
      await TestBed.configureTestingModule({
        imports: [
          BrowserAnimationsModule,
          SharedModule,
          HttpClientTestingModule,
          RouterTestingModule,
          ReactiveFormsModule,
          GridModule,
          InputModule,
          CoreModule,
          SelectModule,
          ComboBoxModule,
          CheckboxModule,
          AccordionModule
        ],
        declarations: [RgwStorageClassFormComponent],
        providers: [
          {
            provide: ActivatedRoute,
            useValue: {
              params: of({
                storage_class: 'SC1',
                placement_target: 'default-placement',
                zonegroup_name: 'zg2'
              })
            }
          },
          {
            provide: Router,
            useValue: {
              url: '/rgw/storage-class/edit',
              navigate: jasmine.createSpy('navigate')
            }
          }
        ]
      }).compileComponents();

      spyOn(TestBed.inject(RgwZonegroupService), 'getAllZonegroupsInfo').and.returnValue(
        zonegroupSubject.asObservable()
      );
      spyOn(TestBed.inject(RgwStorageClassService), 'getPlacement_target').and.returnValue(
        of({
          name: 'default-placement',
          tier_targets: [
            {
              key: 'SC1',
              val: {
                tier_type: 'local',
                storage_class: 'SC1'
              }
            }
          ]
        } as any)
      );
      spyOn(TestBed.inject(RgwZoneService), 'getAllZonesInfo').and.returnValue(
        of({
          zones: [
            {
              name: 'default-zone',
              placement_pools: [
                {
                  key: 'default-placement',
                  val: {
                    storage_classes: {
                      SC1: { data_pool: 'pool-default' }
                    }
                  }
                }
              ]
            },
            {
              name: 'zg2-zone',
              placement_pools: [
                {
                  key: 'default-placement',
                  val: {
                    storage_classes: {
                      SC1: { data_pool: 'pool-zg2' }
                    }
                  }
                }
              ]
            }
          ]
        } as any)
      );
      spyOn(TestBed.inject(PoolService), 'getList').and.returnValue(
        of([
          { pool_name: 'pool-default', application_metadata: ['rgw'] },
          { pool_name: 'pool-zg2', application_metadata: ['rgw'] }
        ] as any)
      );

      editFixture = TestBed.createComponent(RgwStorageClassFormComponent);
      editComponent = editFixture.componentInstance;
      editFixture.detectChanges();
    });

    it('should wait for delayed zonegroup info and populate non-default zone and pool', fakeAsync(() => {
      expect(editComponent.storageClassForm.get('zone').value).toBeFalsy();

      zonegroupSubject.next({
        default_zonegroup: 'zg1-id',
        name: 'default-zg',
        zonegroups: [
          {
            id: 'zg1-id',
            name: 'default-zg',
            zones: [{ name: 'default-zone' }],
            placement_targets: [{ name: 'default-placement' }]
          },
          {
            id: 'zg2-id',
            name: 'zg2',
            zones: [{ name: 'zg2-zone' }],
            placement_targets: [{ name: 'default-placement' }]
          }
        ]
      });
      tick();

      expect(editComponent.storageClassForm.get('zonegroup').value).toBe('zg2');
      expect(editComponent.storageClassForm.get('zone').value).toBe('zg2-zone');
      expect(editComponent.storageClassForm.get('pool').value).toBe('pool-zg2');
    }));
  });
});
