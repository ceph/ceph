import { ComponentFixture, TestBed } from '@angular/core/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { of } from 'rxjs';

import { RgwMultisiteSyncPolicyResourcePageComponent } from './rgw-multisite-sync-policy-resource-page.component';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { RgwMultisiteService } from '~/app/shared/api/rgw-multisite.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';

describe('RgwMultisiteSyncPolicyResourcePageComponent', () => {
  let component: RgwMultisiteSyncPolicyResourcePageComponent;
  let fixture: ComponentFixture<RgwMultisiteSyncPolicyResourcePageComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [RgwMultisiteSyncPolicyResourcePageComponent],
      providers: [
        {
          provide: ActivatedRoute,
          useValue: {
            data: of({ section: 'overview' }),
            queryParamMap: of({ get: () => null }),
            parent: {
              paramMap: of({
                get: (key: string) => (key === 'groupName' ? 'sync-group-a' : null)
              })
            }
          }
        },
        {
          provide: ActionLabelsI18n,
          useValue: {
            CREATE: 'Create',
            EDIT: 'Edit',
            DELETE: 'Delete'
          }
        },
        {
          provide: RgwMultisiteService,
          useValue: {
            getSyncPolicyGroup: () =>
              of({
                data_flow: {
                  symmetrical: [],
                  directional: []
                },
                pipes: []
              })
          }
        },
        {
          provide: TaskWrapperService,
          useValue: {
            wrapTaskAroundCall: () => of({})
          }
        },
        {
          provide: ModalCdsService,
          useValue: {
            show: jest.fn()
          }
        },
        {
          provide: AuthStorageService,
          useValue: {
            getPermissions: () => ({ rgw: {} })
          }
        }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();

    fixture = TestBed.createComponent(RgwMultisiteSyncPolicyResourcePageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
