import { HttpClientModule } from '@angular/common/http';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { TabsModule } from 'ngx-bootstrap/tabs';

import { SharedModule } from '../../../shared/shared.module';
import { DashboardService } from '../dashboard.service';
import { HealthComponent } from './health.component';

describe('HealthComponent', () => {
  let component: HealthComponent;
  let fixture: ComponentFixture<HealthComponent>;

  const fakeService = {
    getHealth() {
      return {};
    }
  };

  beforeEach(
    async(() => {
      TestBed.configureTestingModule({
        providers: [{ provide: DashboardService, useValue: fakeService }],
        imports: [SharedModule],
        declarations: [HealthComponent]
      }).compileComponents();
    })
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(HealthComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });
});
