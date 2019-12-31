import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastrModule } from 'ngx-toastr';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { TaskListService } from '../../../shared/services/task-list.service';
import { SharedModule } from '../../../shared/shared.module';
import { RbdNamespaceListComponent } from './rbd-namespace-list.component';

describe('RbdNamespaceListComponent', () => {
  let component: RbdNamespaceListComponent;
  let fixture: ComponentFixture<RbdNamespaceListComponent>;

  configureTestBed({
    declarations: [RbdNamespaceListComponent],
    imports: [SharedModule, HttpClientTestingModule, RouterTestingModule, ToastrModule.forRoot()],
    providers: [TaskListService, i18nProviders]
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
