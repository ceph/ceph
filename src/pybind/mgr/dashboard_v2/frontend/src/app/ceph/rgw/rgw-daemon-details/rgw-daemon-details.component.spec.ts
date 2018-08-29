import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { TabsModule } from 'ngx-bootstrap/tabs';

import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { SharedModule } from '../../../shared/shared.module';
import { PerformanceCounterModule } from '../../performance-counter/performance-counter.module';
import { RgwDaemonService } from '../services/rgw-daemon.service';
import { RgwDaemonDetailsComponent } from './rgw-daemon-details.component';

describe('RgwDaemonDetailsComponent', () => {
  let component: RgwDaemonDetailsComponent;
  let fixture: ComponentFixture<RgwDaemonDetailsComponent>;

  const fakeService = {
    get: (id: string) => {
      return new Promise(function(resolve, reject) {
        return [];
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
      providers: [{ provide: RgwDaemonService, useValue: fakeService }]
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
