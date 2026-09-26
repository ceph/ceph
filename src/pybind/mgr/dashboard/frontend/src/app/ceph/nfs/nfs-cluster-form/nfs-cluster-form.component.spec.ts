import { ComponentFixture, TestBed } from '@angular/core/testing';
import { NfsClusterFormComponent } from './nfs-cluster-form.component';
import { NotificationService } from '~/app/shared/services/notification.service';
import { RouterTestingModule } from '@angular/router/testing';
import { configureTestBed } from '~/testing/unit-test-helper';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { CdDatePipe } from '~/app/shared/pipes/cd-date.pipe';
import { FormatterService } from '~/app/shared/services/formatter.service';

describe('NfsClusterFormComponent', () => {
  let component: NfsClusterFormComponent;
  let fixture: ComponentFixture<NfsClusterFormComponent>;
  let notificationService: NotificationService;

  configureTestBed({
    imports: [HttpClientTestingModule, RouterTestingModule, NfsClusterFormComponent],
    providers: [FormatterService, { provide: CdDatePipe, useValue: { transform: (d: any) => d } }]
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
