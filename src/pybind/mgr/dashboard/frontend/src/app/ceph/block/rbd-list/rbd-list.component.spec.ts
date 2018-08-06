import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastModule } from 'ng2-toastr';
import {
  AlertModule,
  BsDropdownModule,
  ModalModule,
  TabsModule,
  TooltipModule
} from 'ngx-bootstrap';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { ComponentsModule } from '../../../shared/components/components.module';
import { ViewCacheStatus } from '../../../shared/enum/view-cache-status.enum';
import { SummaryService } from '../../../shared/services/summary.service';
import { SharedModule } from '../../../shared/shared.module';
import { RbdDetailsComponent } from '../rbd-details/rbd-details.component';
import { RbdSnapshotListComponent } from '../rbd-snapshot-list/rbd-snapshot-list.component';
import { RbdListComponent } from './rbd-list.component';

describe('RbdListComponent', () => {
  let component: RbdListComponent;
  let fixture: ComponentFixture<RbdListComponent>;

  class SummaryServiceMock extends SummaryService {
    data: any;

    raiseError() {
      this.summaryDataSource.error(undefined);
    }

    refresh() {
      this.summaryDataSource.next(this.data);
    }
  }

  configureTestBed({
    imports: [
      SharedModule,
      BsDropdownModule.forRoot(),
      TabsModule.forRoot(),
      ModalModule.forRoot(),
      TooltipModule.forRoot(),
      ToastModule.forRoot(),
      AlertModule.forRoot(),
      ComponentsModule,
      RouterTestingModule,
      HttpClientTestingModule
    ],
    declarations: [RbdListComponent, RbdDetailsComponent, RbdSnapshotListComponent],
    providers: [{ provide: SummaryService, useClass: SummaryServiceMock }]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RbdListComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('after ngOnInit', () => {
    let summaryService: SummaryServiceMock;

    beforeEach(() => {
      summaryService = TestBed.get(SummaryService);
      summaryService.data = undefined;
      fixture.detectChanges();
    });

    it('should load images on init', () => {
      spyOn(component, 'loadImages');
      summaryService.data = {};
      summaryService.refresh();
      expect(component.loadImages).toHaveBeenCalled();
    });

    it('should not load images on init because no data', () => {
      spyOn(component, 'loadImages');
      summaryService.refresh();
      expect(component.loadImages).not.toHaveBeenCalled();
    });

    it('should call error function on init when summary service fails', () => {
      spyOn(component.table, 'reset');
      summaryService.raiseError();
      expect(component.table.reset).toHaveBeenCalled();
      expect(component.viewCacheStatusList).toEqual([{ status: ViewCacheStatus.ValueException }]);
    });
  });
});
