import { ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { SmbClusterFormComponent } from './smb-cluster-form.component';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { SharedModule } from '~/app/shared/shared.module';
import { RouterTestingModule } from '@angular/router/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { ToastrModule } from 'ngx-toastr';
import { ComboBoxModule, GridModule, InputModule, SelectModule } from 'carbon-components-angular';

describe('SmbClusterFormComponent', () => {
  let component: SmbClusterFormComponent;
  let fixture: ComponentFixture<SmbClusterFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [
        BrowserAnimationsModule,
        SharedModule,
        HttpClientTestingModule,
        RouterTestingModule,
        ReactiveFormsModule,
        ToastrModule.forRoot(),
        GridModule,
        InputModule,
        SelectModule,
        ComboBoxModule
      ],
      declarations: [SmbClusterFormComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(SmbClusterFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
