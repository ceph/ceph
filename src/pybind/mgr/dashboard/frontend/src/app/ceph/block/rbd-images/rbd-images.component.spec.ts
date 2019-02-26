import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastModule } from 'ng2-toastr';
import { TabsModule } from 'ngx-bootstrap/tabs';
import { TooltipModule } from 'ngx-bootstrap/tooltip';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { TaskListService } from '../../../shared/services/task-list.service';
import { SharedModule } from '../../../shared/shared.module';
import { RbdConfigurationListComponent } from '../rbd-configuration-list/rbd-configuration-list.component';
import { RbdDetailsComponent } from '../rbd-details/rbd-details.component';
import { RbdListComponent } from '../rbd-list/rbd-list.component';
import { RbdSnapshotListComponent } from '../rbd-snapshot-list/rbd-snapshot-list.component';
import { RbdTrashListComponent } from '../rbd-trash-list/rbd-trash-list.component';
import { RbdImagesComponent } from './rbd-images.component';

describe('RbdImagesComponent', () => {
  let component: RbdImagesComponent;
  let fixture: ComponentFixture<RbdImagesComponent>;

  configureTestBed({
    declarations: [
      RbdDetailsComponent,
      RbdImagesComponent,
      RbdListComponent,
      RbdSnapshotListComponent,
      RbdTrashListComponent,
      RbdConfigurationListComponent
    ],
    imports: [
      HttpClientTestingModule,
      RouterTestingModule,
      SharedModule,
      TabsModule.forRoot(),
      ToastModule.forRoot(),
      TooltipModule.forRoot()
    ],
    providers: [TaskListService, i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RbdImagesComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
