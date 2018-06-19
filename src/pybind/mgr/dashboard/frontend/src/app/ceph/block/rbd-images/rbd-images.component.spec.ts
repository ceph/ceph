import { HttpClientTestingModule } from '@angular/common/http/testing';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastModule } from 'ng2-toastr';
import { TabsModule, TooltipModule } from 'ngx-bootstrap';

import { TaskListService } from '../../../shared/services/task-list.service';
import { SharedModule } from '../../../shared/shared.module';
import { RbdDetailsComponent } from '../rbd-details/rbd-details.component';
import { RbdListComponent } from '../rbd-list/rbd-list.component';
import { RbdSnapshotListComponent } from '../rbd-snapshot-list/rbd-snapshot-list.component';
import { RbdTrashListComponent } from '../rbd-trash-list/rbd-trash-list.component';
import { RbdImagesComponent } from './rbd-images.component';

describe('RbdImagesComponent', () => {
  let component: RbdImagesComponent;
  let fixture: ComponentFixture<RbdImagesComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [
        RbdDetailsComponent,
        RbdImagesComponent,
        RbdListComponent,
        RbdSnapshotListComponent,
        RbdTrashListComponent
      ],
      imports: [
        HttpClientTestingModule,
        RouterTestingModule,
        SharedModule,
        TabsModule.forRoot(),
        ToastModule.forRoot(),
        TooltipModule.forRoot()
      ],
      providers: [TaskListService]
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(RbdImagesComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
