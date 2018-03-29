import { HttpClientModule } from '@angular/common/http';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { TabsModule } from 'ngx-bootstrap/tabs';

import { DataTableModule } from '../../../shared/datatable/datatable.module';
import { SharedModule } from '../../../shared/shared.module';
import { PerformanceCounterModule } from '../../performance-counter/performance-counter.module';
import { RgwDaemonDetailsComponent } from '../rgw-daemon-details/rgw-daemon-details.component';
import { RgwDaemonListComponent } from './rgw-daemon-list.component';

describe('RgwDaemonListComponent', () => {
  let component: RgwDaemonListComponent;
  let fixture: ComponentFixture<RgwDaemonListComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ RgwDaemonListComponent, RgwDaemonDetailsComponent ],
      imports: [
        DataTableModule,
        HttpClientTestingModule,
        HttpClientModule,
        TabsModule.forRoot(),
        PerformanceCounterModule,
        SharedModule
      ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwDaemonListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
