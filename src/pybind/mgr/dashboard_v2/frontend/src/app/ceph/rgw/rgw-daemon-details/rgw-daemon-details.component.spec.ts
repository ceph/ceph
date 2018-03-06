import { HttpClientModule } from '@angular/common/http';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { TabsModule } from 'ngx-bootstrap/tabs';

import { SharedModule } from '../../../shared/shared.module';
import { PerformanceCounterModule } from '../../performance-counter/performance-counter.module';
import { RgwDaemonService } from '../services/rgw-daemon.service';
import { RgwDaemonDetailsComponent } from './rgw-daemon-details.component';

describe('RgwDaemonDetailsComponent', () => {
  let component: RgwDaemonDetailsComponent;
  let fixture: ComponentFixture<RgwDaemonDetailsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ RgwDaemonDetailsComponent ],
      imports: [
        SharedModule,
        PerformanceCounterModule,
        HttpClientTestingModule,
        HttpClientModule,
        TabsModule.forRoot()
      ],
      providers: [ RgwDaemonService ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwDaemonDetailsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
