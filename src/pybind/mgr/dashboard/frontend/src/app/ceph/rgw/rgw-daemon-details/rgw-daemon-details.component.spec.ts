import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { TabsModule } from 'ngx-bootstrap/tabs';

import { RgwDaemonService } from '../../../shared/api/rgw-daemon.service';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { SharedModule } from '../../../shared/shared.module';
import { PerformanceCounterModule } from '../../performance-counter/performance-counter.module';
import { RgwDaemonDetailsComponent } from './rgw-daemon-details.component';

describe('RgwDaemonDetailsComponent', () => {
  let component: RgwDaemonDetailsComponent;
  let fixture: ComponentFixture<RgwDaemonDetailsComponent>;

  const fakeRgwDaemonService = {
    get: (id: string) => {
      return new Promise(function(resolve) {
        resolve([]);
      });
    }
  };

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ RgwDaemonDetailsComponent ],
      imports: [
        SharedModule,
        PerformanceCounterModule,
        TabsModule.forRoot()
      ],
      providers: [{ provide: RgwDaemonService, useValue: fakeRgwDaemonService }]
    });
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwDaemonDetailsComponent);
    component = fixture.componentInstance;
    component.selection = new CdTableSelection();
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
