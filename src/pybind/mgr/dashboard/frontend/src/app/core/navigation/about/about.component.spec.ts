import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { BehaviorSubject } from 'rxjs';

import { SummaryService } from '~/app/shared/services/summary.service';
import { SharedModule } from '~/app/shared/shared.module';
import { environment } from '~/environments/environment';
import { configureTestBed } from '~/testing/unit-test-helper';
import { AboutComponent } from './about.component';

export class SummaryServiceMock {
  summaryDataSource = new BehaviorSubject({
    version:
      'ceph version 14.0.0-855-gb8193bb4cd ' +
      '(b8193bb4cda16ccc5b028c3e1df62bc72350a15d) nautilus (dev)',
    mgr_host: 'http://localhost:11000/'
  });
  summaryData$ = this.summaryDataSource.asObservable();

  subscribe(call: any) {
    return this.summaryData$.subscribe(call);
  }
}

describe('AboutComponent', () => {
  let component: AboutComponent;
  let fixture: ComponentFixture<AboutComponent>;

  configureTestBed({
    imports: [SharedModule, HttpClientTestingModule],
    declarations: [AboutComponent],
    providers: [NgbActiveModal, { provide: SummaryService, useClass: SummaryServiceMock }]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(AboutComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should parse version', () => {
    expect(component.versionNumber).toBe('14.0.0-855-gb8193bb4cd');
    expect(component.versionHash).toBe('(b8193bb4cda16ccc5b028c3e1df62bc72350a15d)');
    expect(component.versionName).toBe('nautilus (dev)');
  });

  it('should get host', () => {
    expect(component.hostAddr).toBe('localhost:11000');
  });

  it('should display copyright', () => {
    expect(component.projectConstants.copyright).toContain(environment.year);
  });
});
