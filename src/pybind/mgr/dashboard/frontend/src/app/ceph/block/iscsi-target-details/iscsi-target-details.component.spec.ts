import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { TreeModule } from 'ng2-tree';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { SharedModule } from '../../../shared/shared.module';
import { IscsiTargetDetailsComponent } from './iscsi-target-details.component';

describe('IscsiTargetDetailsComponent', () => {
  let component: IscsiTargetDetailsComponent;
  let fixture: ComponentFixture<IscsiTargetDetailsComponent>;

  configureTestBed({
    declarations: [IscsiTargetDetailsComponent],
    imports: [TreeModule, SharedModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(IscsiTargetDetailsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
