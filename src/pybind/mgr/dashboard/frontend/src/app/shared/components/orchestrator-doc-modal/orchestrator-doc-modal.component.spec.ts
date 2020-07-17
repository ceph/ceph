import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { ComponentsModule } from '../components.module';
import { OrchestratorDocModalComponent } from './orchestrator-doc-modal.component';

describe('OrchestratorDocModalComponent', () => {
  let component: OrchestratorDocModalComponent;
  let fixture: ComponentFixture<OrchestratorDocModalComponent>;

  configureTestBed({
    imports: [ComponentsModule, HttpClientTestingModule, RouterTestingModule],
    providers: [NgbActiveModal]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(OrchestratorDocModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
