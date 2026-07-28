import { ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { MultiClusterComponent } from './multi-cluster.component';
import { SharedModule } from '~/app/shared/shared.module';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('MultiClusterComponent', () => {
  let component: MultiClusterComponent;
  let fixture: ComponentFixture<MultiClusterComponent>;

  configureTestBed({
    imports: [HttpClientTestingModule, SharedModule],
    declarations: [MultiClusterComponent],
    providers: [NgbActiveModal, DimlessBinaryPipe]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(MultiClusterComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
