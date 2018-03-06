import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { ChartsModule } from 'ng2-charts/ng2-charts';
import { ProgressbarModule } from 'ngx-bootstrap/progressbar';
import { Observable } from 'rxjs/Observable';

import { SharedModule } from '../../../shared/shared.module';
import { CephfsService } from '../cephfs.service';
import { CephfsComponent } from './cephfs.component';

describe('CephfsComponent', () => {
  let component: CephfsComponent;
  let fixture: ComponentFixture<CephfsComponent>;

  const fakeFilesystemService = {
    getCephfs: id => {
      return Observable.create(observer => {
        return () => console.log('disposed');
      });
    },
    getMdsCounters: id => {
      return Observable.create(observer => {
        return () => console.log('disposed');
      });
    }
  };

  beforeEach(
    async(() => {
      TestBed.configureTestingModule({
        imports: [SharedModule, ChartsModule, RouterTestingModule, ProgressbarModule.forRoot()],
        declarations: [CephfsComponent],
        providers: [
          { provide: CephfsService, useValue: fakeFilesystemService }
        ]
      }).compileComponents();
    })
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(CephfsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
