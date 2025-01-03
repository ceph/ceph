import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';
import moment from 'moment';
import { NgxPipeFunctionModule } from 'ngx-pipe-function';
import { ToastrModule } from 'ngx-toastr';
import { of } from 'rxjs';

import { RbdService } from '~/app/shared/api/rbd.service';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { ExecutingTask } from '~/app/shared/models/executing-task';
import { Summary } from '~/app/shared/models/summary.model';
import { SummaryService } from '~/app/shared/services/summary.service';
import { TaskListService } from '~/app/shared/services/task-list.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed, expectItemTasks } from '~/testing/unit-test-helper';
import { RbdTabsComponent } from '../rbd-tabs/rbd-tabs.component';
import { RbdTrashListComponent } from './rbd-trash-list.component';

describe('RbdTrashListComponent', () => {
  let component: RbdTrashListComponent;
  let fixture: ComponentFixture<RbdTrashListComponent>;
  let summaryService: SummaryService;
  let rbdService: RbdService;

  configureTestBed({
    declarations: [RbdTrashListComponent, RbdTabsComponent],
    imports: [
      BrowserAnimationsModule,
      HttpClientTestingModule,
      RouterTestingModule,
      SharedModule,
      NgbNavModule,
      NgxPipeFunctionModule,
      ToastrModule.forRoot()
    ],
    providers: [TaskListService]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RbdTrashListComponent);
    component = fixture.componentInstance;
    summaryService = TestBed.inject(SummaryService);
    rbdService = TestBed.inject(RbdService);
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should load trash images when summary is trigged', () => {
    spyOn(rbdService, 'listTrash').and.callThrough();

    summaryService['summaryDataSource'].next(new Summary());
    expect(rbdService.listTrash).toHaveBeenCalled();
  });

  it('should call updateSelection', () => {
    expect(component.selection.hasSelection).toBeFalsy();
    component.updateSelection(new CdTableSelection(['foo']));
    expect(component.selection.hasSelection).toBeTruthy();
  });

  describe('handling of executing tasks', () => {
    let images: any[];

    const addImage = (id: string) => {
      images.push({
        id: id,
        pool_name: 'pl'
      });
    };

    const addTask = (name: string, image_id: string) => {
      const task = new ExecutingTask();
      task.name = name;
      task.metadata = {
        image_id_spec: `pl/${image_id}`
      };
      summaryService.addRunningTask(task);
    };

    beforeEach(() => {
      images = [];
      addImage('1');
      addImage('2');
      component.images = images;
      summaryService['summaryDataSource'].next(new Summary());
      spyOn(rbdService, 'listTrash').and.callFake(() =>
        of([{ pool_name: 'rbd', status: 1, value: images }])
      );
      fixture.detectChanges();
    });

    it('should gets all images without tasks', () => {
      expect(component.images.length).toBe(2);
      expect(
        component.images.every((image: Record<string, any>) => !image.cdExecuting)
      ).toBeTruthy();
    });

    it('should show when an existing image is being modified', () => {
      addTask('rbd/trash/remove', '1');
      addTask('rbd/trash/restore', '2');
      expect(component.images.length).toBe(2);
      expectItemTasks(component.images[0], 'Deleting');
      expectItemTasks(component.images[1], 'Restoring');
    });
  });

  describe('display purge button', () => {
    let images: any[];
    const addImage = (id: string) => {
      images.push({
        id: id,
        pool_name: 'pl',
        deferment_end_time: moment()
      });
    };

    beforeEach(() => {
      summaryService['summaryDataSource'].next(new Summary());
      spyOn(rbdService, 'listTrash').and.callFake(() => {
        of([{ pool_name: 'rbd', status: 1, value: images }]);
      });
      fixture.detectChanges();
    });

    it('should show button disabled when no image is in trash', () => {
      expect(component.disablePurgeBtn).toBeTruthy();
    });

    it('should show button enabled when an existing image is in trash', () => {
      images = [];
      addImage('1');
      const payload = [{ pool_name: 'rbd', status: 1, value: images }];
      component.prepareResponse(payload);
      expect(component.disablePurgeBtn).toBeFalsy();
    });

    it('should show button with delete permission', () => {
      component.permission = {
        read: true,
        create: true,
        delete: true,
        update: true
      };
      fixture.detectChanges();

      const purge = fixture.debugElement.query(By.css('.table-actions button'));
      expect(purge).not.toBeNull();
    });

    it('should remove button without delete permission', () => {
      component.permission = {
        read: true,
        create: true,
        delete: false,
        update: true
      };
      fixture.detectChanges();

      const purge = fixture.debugElement.query(By.css('.table-actions button'));
      expect(purge).toBeNull();
    });
  });
});
