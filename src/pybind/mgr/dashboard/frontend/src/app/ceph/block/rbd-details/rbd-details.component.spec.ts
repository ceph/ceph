import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { TabsModule, TooltipModule } from 'ngx-bootstrap';

import { SharedModule } from '../../../shared/shared.module';
import { RbdSnapshotListComponent } from '../rbd-snapshot-list/rbd-snapshot-list.component';
import { RbdDetailsComponent } from './rbd-details.component';

describe('RbdDetailsComponent', () => {
  let component: RbdDetailsComponent;
  let fixture: ComponentFixture<RbdDetailsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ RbdDetailsComponent, RbdSnapshotListComponent ],
      imports: [ SharedModule, TabsModule.forRoot(), TooltipModule.forRoot(), RouterTestingModule]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(RbdDetailsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
