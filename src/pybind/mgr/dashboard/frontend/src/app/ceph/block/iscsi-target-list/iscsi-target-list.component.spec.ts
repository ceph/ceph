import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { TreeModule } from 'ng2-tree';
import { TabsModule } from 'ngx-bootstrap/tabs';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { TaskListService } from '../../../shared/services/task-list.service';
import { SharedModule } from '../../../shared/shared.module';
import { IscsiTabsComponent } from '../iscsi-tabs/iscsi-tabs.component';
import { IscsiTargetDetailsComponent } from '../iscsi-target-details/iscsi-target-details.component';
import { IscsiTargetListComponent } from './iscsi-target-list.component';

describe('IscsiTargetListComponent', () => {
  let component: IscsiTargetListComponent;
  let fixture: ComponentFixture<IscsiTargetListComponent>;

  configureTestBed({
    imports: [
      HttpClientTestingModule,
      RouterTestingModule,
      SharedModule,
      TabsModule.forRoot(),
      TreeModule
    ],
    declarations: [IscsiTargetListComponent, IscsiTabsComponent, IscsiTargetDetailsComponent],
    providers: [TaskListService, i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(IscsiTargetListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
