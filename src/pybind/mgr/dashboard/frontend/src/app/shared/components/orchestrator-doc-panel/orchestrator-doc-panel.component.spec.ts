import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { CephReleaseNamePipe } from '../../pipes/ceph-release-name.pipe';
import { SummaryService } from '../../services/summary.service';
import { ComponentsModule } from '../components.module';
import { OrchestratorDocPanelComponent } from './orchestrator-doc-panel.component';

describe('OrchestratorDocPanelComponent', () => {
  let component: OrchestratorDocPanelComponent;
  let fixture: ComponentFixture<OrchestratorDocPanelComponent>;

  configureTestBed({
    imports: [ComponentsModule, HttpClientTestingModule, RouterTestingModule],
    providers: [CephReleaseNamePipe, SummaryService]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(OrchestratorDocPanelComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
