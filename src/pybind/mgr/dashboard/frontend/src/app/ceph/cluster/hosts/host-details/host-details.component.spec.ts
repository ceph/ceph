import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { BsDropdownModule } from 'ngx-bootstrap/dropdown';
import { TabsModule } from 'ngx-bootstrap/tabs';

import { configureTestBed } from '../../../../../testing/unit-test-helper';
import { CdTableSelection } from '../../../../shared/models/cd-table-selection';
import { SharedModule } from '../../../../shared/shared.module';
import { InventoryComponent } from '../../inventory/inventory.component';
import { ServicesComponent } from '../../services/services.component';
import { HostDetailsComponent } from './host-details.component';

describe('HostDetailsComponent', () => {
  let component: HostDetailsComponent;
  let fixture: ComponentFixture<HostDetailsComponent>;

  configureTestBed({
    imports: [
      HttpClientTestingModule,
      TabsModule.forRoot(),
      BsDropdownModule.forRoot(),
      SharedModule
    ],
    declarations: [HostDetailsComponent, InventoryComponent, ServicesComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(HostDetailsComponent);
    component = fixture.componentInstance;

    component.selection = new CdTableSelection();

    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
