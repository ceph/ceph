import { ComponentFixture, TestBed, fakeAsync, tick } from '@angular/core/testing';
import { of, throwError } from 'rxjs';

import { CephfsImportTokenComponent } from './cephfs-import-token.component';
import { CephfsService } from '~/app/shared/api/cephfs.service';

describe('CephfsImportTokenComponent (Jest)', () => {
  let component: CephfsImportTokenComponent;
  let fixture: ComponentFixture<CephfsImportTokenComponent>;
  let cephfsServiceMock: any;

  beforeEach(async () => {
    cephfsServiceMock = {
      createBootstrapPeer: jest.fn()
    };

    await TestBed.configureTestingModule({
      declarations: [CephfsImportTokenComponent],
      providers: [{ provide: CephfsService, useValue: cephfsServiceMock }]
    })
      .overrideComponent(CephfsImportTokenComponent, {
        set: { template: '' } // prevent template-related errors
      })
      .compileComponents();

    fixture = TestBed.createComponent(CephfsImportTokenComponent);
    component = fixture.componentInstance;

    component.selectedFilesystem = { name: 'fs1' } as any;

    fixture.detectChanges();
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('should create component', () => {
    expect(component).toBeTruthy();
  });

  it('should not call service if token is empty', () => {
    component.token = '';
    component.importToken();

    expect(cephfsServiceMock.createBootstrapPeer).not.toHaveBeenCalled();
  });

  it('should not call service if filesystem missing', () => {
    component.selectedFilesystem = null;
    component.token = 'abc';
    component.importToken();
    expect(cephfsServiceMock.createBootstrapPeer).not.toHaveBeenCalled();
  });

  it('should not call service if already submitting', () => {
    component.token = 'abcnjhgfyfyhj54f8=';
    component.isSubmitting = true;

    component.importToken();

    expect(cephfsServiceMock.createBootstrapPeer).not.toHaveBeenCalled();
  });

  it('should call service and format object response', fakeAsync(() => {
    component.token = '  my-token  ';

    cephfsServiceMock.createBootstrapPeer.mockReturnValue(
      of({ success: true })
    );
    component.importToken();
    tick();
    expect(cephfsServiceMock.createBootstrapPeer).toHaveBeenCalledWith(
      'fs1',
      'my-token'
    );

    expect(component.token).toBe(JSON.stringify({ success: true }, null, 2));
    expect(component.isSubmitting).toBeFalsy();
  }));

  it('should reset isSubmitting on error', fakeAsync(() => {
    component.token = 'my-token';

    cephfsServiceMock.createBootstrapPeer.mockReturnValue(
      throwError(() => new Error('fail'))
    );

    component.importToken();
    tick();

    expect(component.isSubmitting).toBeFalsy();
  }));
});