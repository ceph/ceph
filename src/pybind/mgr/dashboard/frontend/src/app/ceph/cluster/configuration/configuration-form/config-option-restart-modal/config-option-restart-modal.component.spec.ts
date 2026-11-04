import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { ConfigOptionRestartModalComponent } from './config-option-restart-modal.component';

describe('ConfigOptionRestartModalComponent', () => {
  let component: ConfigOptionRestartModalComponent;
  let fixture: ComponentFixture<ConfigOptionRestartModalComponent>;

  configureTestBed({
    imports: [HttpClientTestingModule, RouterTestingModule, SharedModule],
    declarations: [ConfigOptionRestartModalComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ConfigOptionRestartModalComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should initialize service items correctly', () => {
    component.services = ['osd', 'mon'];
    component.ngOnInit();
    expect(component.serviceItems.length).toBe(2);
    expect(component.serviceItems[0].name).toBe('osd');
    expect(component.serviceItems[1].name).toBe('mon');
  });
});
