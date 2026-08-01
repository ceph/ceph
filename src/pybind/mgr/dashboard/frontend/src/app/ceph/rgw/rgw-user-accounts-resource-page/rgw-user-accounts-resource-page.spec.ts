import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute } from '@angular/router';
import { of } from 'rxjs';

import { RgwUserAccountsResourcePageComponent } from './rgw-user-accounts-resource-page.component';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';

describe('RgwUserAccountsResourcePageComponent', () => {
  let component: RgwUserAccountsResourcePageComponent;
  let fixture: ComponentFixture<RgwUserAccountsResourcePageComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [RgwUserAccountsResourcePageComponent],
      providers: [
        DimlessBinaryPipe,
        {
          provide: ActivatedRoute,
          useValue: {
            parent: {
              data: of({
                account: {
                  id: 'RGW11111111111111111',
                  tenant: '',
                  name: 'account1',
                  email: 'account1@ceph.com',
                  quota: {
                    enabled: false,
                    check_on_raw: false,
                    max_size: -1,
                    max_size_kb: -1,
                    max_objects: -1
                  },
                  bucket_quota: {
                    enabled: false,
                    check_on_raw: false,
                    max_size: -1,
                    max_size_kb: -1,
                    max_objects: -1
                  },
                  max_users: 1000,
                  max_roles: 1000,
                  max_groups: 1000,
                  max_buckets: 1000,
                  max_access_keys: 4
                }
              })
            },
            snapshot: {
              data: {
                section: 'overview'
              }
            }
          }
        }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();

    fixture = TestBed.createComponent(RgwUserAccountsResourcePageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should load account overview data', () => {
    expect(component.selection?.name).toBe('account1');
    expect(component.overviewField.length).toBeGreaterThan(0);
    expect(component.overviewField.find((field) => field.label === 'Max users')?.value).toBe(1000);
  });
});
