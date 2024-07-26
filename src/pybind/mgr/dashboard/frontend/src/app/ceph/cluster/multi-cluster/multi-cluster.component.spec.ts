import { ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { MultiClusterComponent } from './multi-cluster.component';
import { SharedModule } from '~/app/shared/shared.module';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';

describe('MultiClusterComponent', () => {
  let component: MultiClusterComponent;
  let fixture: ComponentFixture<MultiClusterComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [HttpClientTestingModule, SharedModule],
      declarations: [MultiClusterComponent],
      providers: [NgbActiveModal, DimlessBinaryPipe]
    }).compileComponents();

    fixture = TestBed.createComponent(MultiClusterComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
