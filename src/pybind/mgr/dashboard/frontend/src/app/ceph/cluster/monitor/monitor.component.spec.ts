import { ComponentFixture, TestBed } from '@angular/core/testing';

import { AppModule } from '../../../app.module';
import { configureTestBed } from '../../../shared/unit-test-helper';
import { MonitorComponent } from './monitor.component';

describe('MonitorComponent', () => {
  let component: MonitorComponent;
  let fixture: ComponentFixture<MonitorComponent>;

  configureTestBed({
    imports: [AppModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(MonitorComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });
});
