import { HttpClientModule } from '@angular/common/http';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { BsModalService } from 'ngx-bootstrap/modal';
import { TabsModule } from 'ngx-bootstrap/tabs';

import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { SharedModule } from '../../../shared/shared.module';
import { configureTestBed } from '../../../shared/unit-test-helper';
import { RgwUserDetailsComponent } from './rgw-user-details.component';

describe('RgwUserDetailsComponent', () => {
  let component: RgwUserDetailsComponent;
  let fixture: ComponentFixture<RgwUserDetailsComponent>;

  configureTestBed({
    declarations: [RgwUserDetailsComponent],
    imports: [HttpClientTestingModule, HttpClientModule, SharedModule, TabsModule.forRoot()],
    providers: [BsModalService]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwUserDetailsComponent);
    component = fixture.componentInstance;
    component.selection = new CdTableSelection();
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
