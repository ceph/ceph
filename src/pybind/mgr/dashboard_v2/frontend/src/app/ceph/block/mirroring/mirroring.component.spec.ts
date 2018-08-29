import { HttpClientTestingModule } from '@angular/common/http/testing';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { BsDropdownModule, TabsModule } from 'ngx-bootstrap';
import { ProgressbarModule } from 'ngx-bootstrap/progressbar';
import { Observable } from 'rxjs/Observable';

import { RbdMirroringService } from '../../../shared/services/rbd-mirroring.service';
import { SharedModule } from '../../../shared/shared.module';
import { MirrorHealthColorPipe } from '../mirror-health-color.pipe';
import { MirroringComponent } from './mirroring.component';

describe('MirroringComponent', () => {
  let component: MirroringComponent;
  let fixture: ComponentFixture<MirroringComponent>;

  const fakeService = {
    get: (service_type: string, service_id: string) => {
      return Observable.create(observer => {
        return () => console.log('disposed');
      });
    }
  };

  beforeEach(
    async(() => {
      TestBed.configureTestingModule({
        declarations: [MirroringComponent, MirrorHealthColorPipe],
        imports: [
          SharedModule,
          BsDropdownModule.forRoot(),
          TabsModule.forRoot(),
          ProgressbarModule.forRoot(),
          HttpClientTestingModule
        ],
        providers: [{ provide: RbdMirroringService, useValue: fakeService }]
      }).compileComponents();
    })
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(MirroringComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
