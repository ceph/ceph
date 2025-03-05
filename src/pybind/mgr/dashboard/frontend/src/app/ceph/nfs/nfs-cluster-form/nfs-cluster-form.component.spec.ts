import { ComponentFixture, TestBed } from '@angular/core/testing';
import { NfsClusterFormComponent } from './nfs-cluster-form.component';
import { NotificationService } from '~/app/shared/services/notification.service';
import { ReactiveFormsModule } from '@angular/forms';
import { NfsRateLimitComponent } from '../nfs-rate-limit/nfs-rate-limit.component';
import { RouterTestingModule } from '@angular/router/testing';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ToastrModule } from 'ngx-toastr';

describe('NfsClusterFormComponent', () => {
  let component: NfsClusterFormComponent;
  let fixture: ComponentFixture<NfsClusterFormComponent>;
  let notificationService: NotificationService;

  configureTestBed({
    imports: [
      ReactiveFormsModule,
      HttpClientTestingModule,
      RouterTestingModule,
      SharedModule,
      ToastrModule.forRoot()
    ],
    declarations: [NfsClusterFormComponent, NfsRateLimitComponent]
  });

  beforeEach(async () => {
    notificationService = TestBed.inject(NotificationService);
    spyOn(notificationService, 'show').and.stub();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(NfsClusterFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should initialize form on ngOnInit', () => {
    component.ngOnInit();
    expect(component.nfsForm).toBeDefined();
    expect(component.nfsForm.get('cluster_id')).toBeDefined();
  });
});
