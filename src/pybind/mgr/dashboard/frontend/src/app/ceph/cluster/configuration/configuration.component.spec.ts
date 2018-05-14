import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';

import { TabsModule } from 'ngx-bootstrap/tabs/tabs.module';
import { Observable } from 'rxjs/Observable';

import { ConfigurationService } from '../../../shared/api/configuration.service';
import { SharedModule } from '../../../shared/shared.module';
import { ConfigurationComponent } from './configuration.component';

describe('ConfigurationComponent', () => {
  let component: ConfigurationComponent;
  let fixture: ComponentFixture<ConfigurationComponent>;

  const fakeService = {
    getConfigData: () => {
      return Observable.create(observer => {
        return () => console.log('disposed');
      });
    }
  };

  beforeEach(
    async(() => {
      TestBed.configureTestingModule({
        declarations: [ConfigurationComponent],
        providers: [{ provide: ConfigurationService, useValue: fakeService }],
        imports: [SharedModule, FormsModule, TabsModule.forRoot()]
      }).compileComponents();
    })
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(ConfigurationComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
