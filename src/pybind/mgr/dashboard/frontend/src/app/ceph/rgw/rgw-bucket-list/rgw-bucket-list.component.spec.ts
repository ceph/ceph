import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { RouterTestingModule } from '@angular/router/testing';

import { ModalModule } from 'ngx-bootstrap/modal';
import { TabsModule } from 'ngx-bootstrap/tabs';
import { of } from 'rxjs';

import {
  configureTestBed,
  i18nProviders,
  PermissionHelper
} from '../../../../testing/unit-test-helper';
import { RgwBucketService } from '../../../shared/api/rgw-bucket.service';
import { ActionLabels } from '../../../shared/constants/app.constants';
import { TableActionsComponent } from '../../../shared/datatable/table-actions/table-actions.component';
import { SharedModule } from '../../../shared/shared.module';
import { RgwBucketDetailsComponent } from '../rgw-bucket-details/rgw-bucket-details.component';
import { RgwBucketListComponent } from './rgw-bucket-list.component';

describe('RgwBucketListComponent', () => {
  let component: RgwBucketListComponent;
  let fixture: ComponentFixture<RgwBucketListComponent>;
  let rgwBucketService: RgwBucketService;
  let rgwBucketServiceListSpy: jasmine.Spy;

  configureTestBed({
    declarations: [RgwBucketListComponent, RgwBucketDetailsComponent],
    imports: [
      RouterTestingModule,
      ModalModule.forRoot(),
      SharedModule,
      TabsModule.forRoot(),
      HttpClientTestingModule
    ],
    providers: i18nProviders
  });

  beforeEach(() => {
    rgwBucketService = TestBed.get(RgwBucketService);
    rgwBucketServiceListSpy = spyOn(rgwBucketService, 'list');
    rgwBucketServiceListSpy.and.returnValue(of(null));
    fixture = TestBed.createComponent(RgwBucketListComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  describe('show action buttons and drop down actions depending on permissions', () => {
    let tableActions: TableActionsComponent;
    let scenario: { fn; empty; single };
    let permissionHelper: PermissionHelper;

    const getTableActionComponent = (): TableActionsComponent => {
      fixture.detectChanges();
      return fixture.debugElement.query(By.directive(TableActionsComponent)).componentInstance;
    };

    beforeEach(() => {
      permissionHelper = new PermissionHelper(component.permission, () =>
        getTableActionComponent()
      );
      scenario = {
        fn: () => tableActions.getCurrentButton().name,
        single: ActionLabels.EDIT,
        empty: ActionLabels.CREATE
      };
    });

    describe('with all', () => {
      beforeEach(() => {
        tableActions = permissionHelper.setPermissionsAndGetActions(1, 1, 1);
      });

      it(`shows 'Edit' for single selection else 'Add' as main action`, () =>
        permissionHelper.testScenarios(scenario));

      it('shows all actions', () => {
        expect(tableActions.tableActions.length).toBe(3);
        expect(tableActions.tableActions).toEqual(component.tableActions);
      });
    });

    describe('with read, create and update', () => {
      beforeEach(() => {
        tableActions = permissionHelper.setPermissionsAndGetActions(1, 1, 0);
      });

      it(`shows 'Edit' for single selection else 'Add' as main action`, () =>
        permissionHelper.testScenarios(scenario));

      it(`shows 'Add' and 'Edit' action`, () => {
        expect(tableActions.tableActions.length).toBe(2);
        component.tableActions.pop();
        expect(tableActions.tableActions).toEqual(component.tableActions);
      });
    });

    describe('with read, create and delete', () => {
      beforeEach(() => {
        tableActions = permissionHelper.setPermissionsAndGetActions(1, 0, 1);
      });

      it(`shows 'Delete' for single selection else 'Add' as main action`, () => {
        scenario.single = 'Delete';
        permissionHelper.testScenarios(scenario);
      });

      it(`shows 'Add' and 'Delete' action`, () => {
        expect(tableActions.tableActions.length).toBe(2);
        expect(tableActions.tableActions).toEqual([
          component.tableActions[0],
          component.tableActions[2]
        ]);
      });
    });

    describe('with read, edit and delete', () => {
      beforeEach(() => {
        tableActions = permissionHelper.setPermissionsAndGetActions(0, 1, 1);
      });

      it(`shows always 'Edit' as main action`, () => {
        scenario.empty = ActionLabels.EDIT;
        permissionHelper.testScenarios(scenario);
      });

      it(`shows 'Edit' and 'Delete' action`, () => {
        expect(tableActions.tableActions.length).toBe(2);
        expect(tableActions.tableActions).toEqual([
          component.tableActions[1],
          component.tableActions[2]
        ]);
      });
    });

    describe('with read and create', () => {
      beforeEach(() => {
        tableActions = permissionHelper.setPermissionsAndGetActions(1, 0, 0);
      });

      it(`shows always 'Add' as main action`, () => {
        scenario.single = ActionLabels.CREATE;
        permissionHelper.testScenarios(scenario);
      });

      it(`shows only 'Add' action`, () => {
        expect(tableActions.tableActions.length).toBe(1);
        expect(tableActions.tableActions).toEqual([component.tableActions[0]]);
      });
    });

    describe('with read and update', () => {
      beforeEach(() => {
        tableActions = permissionHelper.setPermissionsAndGetActions(0, 1, 0);
      });

      it(`shows always 'Edit' as main action`, () => {
        scenario.empty = ActionLabels.EDIT;
        permissionHelper.testScenarios(scenario);
      });

      it(`shows only 'Edit' action`, () => {
        expect(tableActions.tableActions.length).toBe(1);
        expect(tableActions.tableActions).toEqual([component.tableActions[1]]);
      });
    });

    describe('with read and delete', () => {
      beforeEach(() => {
        tableActions = permissionHelper.setPermissionsAndGetActions(0, 0, 1);
      });

      it(`shows always 'Delete' as main action`, () => {
        scenario.single = 'Delete';
        scenario.empty = 'Delete';
        permissionHelper.testScenarios(scenario);
      });

      it(`shows only 'Delete' action`, () => {
        expect(tableActions.tableActions.length).toBe(1);
        expect(tableActions.tableActions).toEqual([component.tableActions[2]]);
      });
    });

    describe('with only read', () => {
      beforeEach(() => {
        tableActions = permissionHelper.setPermissionsAndGetActions(0, 0, 0);
      });

      it('shows no main action', () => {
        permissionHelper.testScenarios({
          fn: () => tableActions.getCurrentButton(),
          single: undefined,
          empty: undefined
        });
      });

      it('shows no actions', () => {
        expect(tableActions.tableActions.length).toBe(0);
        expect(tableActions.tableActions).toEqual([]);
      });
    });
  });

  it('should test if bucket data is tranformed correctly', () => {
    rgwBucketServiceListSpy.and.returnValue(
      of([
        {
          bucket: 'bucket',
          owner: 'testid',
          usage: {
            'rgw.main': {
              size_actual: 4,
              num_objects: 2
            },
            'rgw.another': {
              size_actual: 6,
              num_objects: 6
            }
          },
          bucket_quota: {
            max_size: 20,
            max_objects: 10,
            enabled: true
          }
        }
      ])
    );
    fixture.detectChanges();
    expect(component.buckets).toEqual([
      {
        bucket: 'bucket',
        owner: 'testid',
        usage: {
          'rgw.main': { size_actual: 4, num_objects: 2 },
          'rgw.another': { size_actual: 6, num_objects: 6 }
        },
        bucket_quota: {
          max_size: 20,
          max_objects: 10,
          enabled: true
        },
        bucket_size: 10,
        num_objects: 8,
        size_usage: 0.5,
        object_usage: 0.8
      }
    ]);
  });
  it('should usage bars only if quota enabled', () => {
    rgwBucketServiceListSpy.and.returnValue(
      of([
        {
          bucket: 'bucket',
          owner: 'testid',
          bucket_quota: {
            max_size: 1024,
            max_objects: 10,
            enabled: true
          }
        }
      ])
    );
    fixture.detectChanges();
    const usageBars = fixture.debugElement.nativeElement.querySelectorAll('cd-usage-bar');
    expect(usageBars.length).toBe(2);
  });
  it('should not show any usage bars if quota disabled', () => {
    rgwBucketServiceListSpy.and.returnValue(
      of([
        {
          bucket: 'bucket',
          owner: 'testid',
          bucket_quota: {
            max_size: 1024,
            max_objects: 10,
            enabled: false
          }
        }
      ])
    );
    fixture.detectChanges();
    const usageBars = fixture.debugElement.nativeElement.querySelectorAll('cd-usage-bar');
    expect(usageBars.length).toBe(0);
  });
});
