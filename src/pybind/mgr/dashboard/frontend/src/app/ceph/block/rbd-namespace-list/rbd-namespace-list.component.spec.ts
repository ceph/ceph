import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';
import { ToastrModule } from 'ngx-toastr';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { TaskListService } from '../../../shared/services/task-list.service';
import { SharedModule } from '../../../shared/shared.module';
import { RbdTabsComponent } from '../rbd-tabs/rbd-tabs.component';
import { RbdNamespaceListComponent } from './rbd-namespace-list.component';

describe('RbdNamespaceListComponent', () => {
  let component: RbdNamespaceListComponent;
  let fixture: ComponentFixture<RbdNamespaceListComponent>;

  configureTestBed({
    declarations: [RbdNamespaceListComponent, RbdTabsComponent],
    imports: [
      BrowserAnimationsModule,
      SharedModule,
      HttpClientTestingModule,
      RouterTestingModule,
      ToastrModule.forRoot(),
      NgbNavModule
    ],
    providers: [TaskListService]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RbdNamespaceListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
