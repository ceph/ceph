import { ComponentFixture, TestBed } from '@angular/core/testing';

import { TextToDownloadService } from '~/app/shared/services/text-to-download.service';
import { configureTestBed } from '~/testing/unit-test-helper';
import { DownloadButtonComponent } from './download-button.component';

describe('DownloadButtonComponent', () => {
  let component: DownloadButtonComponent;
  let fixture: ComponentFixture<DownloadButtonComponent>;

  configureTestBed({
    declarations: [DownloadButtonComponent],
    providers: [TextToDownloadService]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(DownloadButtonComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should call download function', () => {
    component.objectItem = {
      testA: 'testA',
      testB: 'testB'
    };
    const downloadSpy = spyOn(TestBed.inject(TextToDownloadService), 'download');
    component.fileName = `${'reportText.json'}_${new Date().toLocaleDateString()}`;
    component.download('json');
    expect(downloadSpy).toHaveBeenCalledWith(
      JSON.stringify(component.objectItem, null, 2),
      `${component.fileName}.json`
    );
  });
});
