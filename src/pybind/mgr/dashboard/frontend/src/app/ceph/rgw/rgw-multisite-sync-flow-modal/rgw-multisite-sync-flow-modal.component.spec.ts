import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RgwMultisiteSyncFlowModalComponent } from './rgw-multisite-sync-flow-modal.component';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { ToastrModule } from 'ngx-toastr';
import { PipesModule } from '~/app/shared/pipes/pipes.module';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { CommonModule } from '@angular/common';

enum FlowType {
  symmetrical = 'symmetrical',
  directional = 'directional'
}
describe('RgwMultisiteSyncFlowModalComponent', () => {
  let component: RgwMultisiteSyncFlowModalComponent;
  let fixture: ComponentFixture<RgwMultisiteSyncFlowModalComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [RgwMultisiteSyncFlowModalComponent],
      imports: [
        HttpClientTestingModule,
        ToastrModule.forRoot(),
        PipesModule,
        ReactiveFormsModule,
        CommonModule
      ],
      providers: [NgbActiveModal]
    }).compileComponents();

    fixture = TestBed.createComponent(RgwMultisiteSyncFlowModalComponent);
    component = fixture.componentInstance;
    component.groupType = FlowType.symmetrical;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
