import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { TabsModule } from 'ngx-bootstrap/tabs';
import { TooltipModule } from 'ngx-bootstrap/tooltip';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { SharedModule } from '../../../shared/shared.module';
import { RbdConfigurationListComponent } from '../rbd-configuration-list/rbd-configuration-list.component';
import { RbdSnapshotListComponent } from '../rbd-snapshot-list/rbd-snapshot-list.component';
import { RbdDetailsComponent } from './rbd-details.component';

describe('RbdDetailsComponent', () => {
  let component: RbdDetailsComponent;
  let fixture: ComponentFixture<RbdDetailsComponent>;

  configureTestBed({
    declarations: [RbdDetailsComponent, RbdSnapshotListComponent, RbdConfigurationListComponent],
    imports: [SharedModule, TabsModule.forRoot(), TooltipModule.forRoot(), RouterTestingModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RbdDetailsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
