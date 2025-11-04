import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RgwMultisiteZoneFormComponent } from './rgw-multisite-zone-form.component'; // Adjust path as necessary
import { of } from 'rxjs';
import { RgwZoneService } from '~/app/shared/api/rgw-zone.service';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { ToastrModule } from 'ngx-toastr';
import { SharedModule } from '~/app/shared/shared.module';
import { RgwZone } from '../models/rgw-multisite';
import { NO_ERRORS_SCHEMA } from '@angular/compiler';
import { CheckboxModule, InputModule, ModalModule, SelectModule } from 'carbon-components-angular';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';

describe('RgwMultisiteZoneFormComponent', () => {
  let component: RgwMultisiteZoneFormComponent;
  let fixture: ComponentFixture<RgwMultisiteZoneFormComponent>;
  let rgwZoneService: RgwZoneService;
  let rgwZoneServiceSpy: jasmine.Spy;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [
        SharedModule,
        ReactiveFormsModule,
        RouterTestingModule,
        HttpClientTestingModule,
        ModalModule,
        InputModule,
        ToastrModule.forRoot(),
        CheckboxModule,
        SelectModule
      ],
      providers: [
        NgbActiveModal,
        { provide: 'multisiteInfo', useValue: [[]] },
        { provide: 'info', useValue: { data: { name: 'null' } } },
        { provide: 'defaultsInfo', useValue: { defaultZonegroupName: 'zonegroup1' } },
        {
          provide: ActionLabelsI18n,
          useValue: { CREATE: 'create', EDIT: 'edit', DELETE: 'delete' }
        }
      ],
      schemas: [NO_ERRORS_SCHEMA],
      declarations: [RgwMultisiteZoneFormComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(RgwMultisiteZoneFormComponent);
    component = fixture.componentInstance;
    rgwZoneService = TestBed.inject(RgwZoneService);

    rgwZoneServiceSpy = spyOn(rgwZoneService, 'get');

    rgwZoneServiceSpy.and.returnValue(
      of({
        placement_pools: [
          {
            key: 'default-placement',
            val: {
              storage_classes: {
                STANDARD: {
                  data_pool: 'standard-data-pool',
                  compression_type: 'gzip'
                }
              },
              index_pool: 'index-pool',
              data_extra_pool: 'extra-data-pool'
            }
          }
        ]
      })
    );

    component.info = {
      parent: {
        data: {
          name: 'zonegroup2',
          placement_targets: [
            { name: 'default-placement', tags: [], storage_classes: ['STANDARD'] }
          ],
          default_placement: 'default-placement'
        }
      },
      data: {
        name: 'zone2',
        zoneName: 'zone2',
        parent: 'zonegroup2',
        is_default: true,
        is_master: true,
        endpoints: ['http://192.168.100.100:80'],
        access_key: 'zxcftyuuhgg',
        secret_key: 'Qwsdcfgghuiioklpoozsd'
      }
    };

    component.zone = new RgwZone();
    component.zone.name = component.info.data.name;
    component.multisiteZoneForm.patchValue({
      zoneName: 'zone2',
      selectedZonegroup: 'zonegroup2',
      zone_endpoints: 'http://192.168.100.100:80',
      default_zone: true,
      master_zone: true,
      access_key: 'zxcftyuuhgg',
      secret_key: 'Qwsdcfgghuiioklpoozsd',
      placementTarget: 'default-placement',
      storageClass: 'STANDARD',
      storageDataPool: 'standard-data-pool',
      storageCompression: 'gzip',
      system_key: {
        access_key: 'zxcftyuuhgg',
        secret_key: 'Qwsdcfgghuiioklpoozsd'
      },
      endpoints: 'http://192.168.100.100:80',
      name: 'zone2'
    });

    component.action = 'edit';
    component.ngOnInit();

    fixture.detectChanges();

    component.getZonePlacementData('default-placement');
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should set correct values in the form on edit', () => {
    expect(component.multisiteZoneForm?.get('zoneName')?.value).toBe('zone2');
    expect(component.multisiteZoneForm?.get('selectedZonegroup')?.value).toBe('zonegroup2');
    expect(component.multisiteZoneForm?.get('default_zone')?.value).toBe(true);
    expect(component.multisiteZoneForm?.get('master_zone')?.value).toBe(true);
    expect(component.multisiteZoneForm?.get('zone_endpoints')?.value).toBe(
      'http://192.168.100.100:80'
    );
    expect(component.multisiteZoneForm.get('access_key')?.value).toBe('zxcftyuuhgg');
    expect(component.multisiteZoneForm.get('secret_key')?.value).toBe('Qwsdcfgghuiioklpoozsd');
    expect(component.multisiteZoneForm.get('placementTarget')?.value).toBe('default-placement');
    expect(component.multisiteZoneForm.get('storageClass')?.value).toBe('STANDARD');
    expect(component.multisiteZoneForm.get('storageDataPool')?.value).toBe('standard-data-pool');
    expect(component.multisiteZoneForm.get('storageCompression')?.value).toBe('gzip');
  });

  it('should create a new zone', () => {
    component.action = 'create';
    component.submit();
    expect(component).toBeTruthy();
  });
});
