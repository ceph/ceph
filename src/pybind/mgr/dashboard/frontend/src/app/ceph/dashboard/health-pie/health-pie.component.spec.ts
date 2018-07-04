import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ChartsModule } from 'ng2-charts/ng2-charts';

import { SharedModule } from '../../../shared/shared.module';
import { configureTestBed } from '../../../../testing/unit-test-helper';
import { DimlessBinaryPipe } from '../../../shared/pipes/dimless-binary.pipe';
import { FormatterService } from '../../../shared/services/formatter.service';
import { HealthPieComponent } from './health-pie.component';

describe('HealthPieComponent', () => {
  let component: HealthPieComponent;
  let fixture: ComponentFixture<HealthPieComponent>;

  beforeEach(
    async(() => {
      TestBed.configureTestingModule({
        imports: [ChartsModule, SharedModule],
        declarations: [HealthPieComponent]
      }).compileComponents();
    })
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(HealthPieComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
