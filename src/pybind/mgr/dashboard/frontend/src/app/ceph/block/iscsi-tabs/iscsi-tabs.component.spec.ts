import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { TabsModule } from 'ngx-bootstrap/tabs';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { SharedModule } from '../../../shared/shared.module';
import { IscsiTabsComponent } from './iscsi-tabs.component';

describe('IscsiTabsComponent', () => {
  let component: IscsiTabsComponent;
  let fixture: ComponentFixture<IscsiTabsComponent>;

  configureTestBed({
    imports: [SharedModule, TabsModule.forRoot(), RouterTestingModule],
    declarations: [IscsiTabsComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(IscsiTabsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
