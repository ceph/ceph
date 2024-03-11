import { ElementRef, Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class TextAreaJsonFormatterService {
  constructor() {}

  format(textArea: ElementRef<any>): void {
    const value = textArea.nativeElement.value;
    try {
      const formatted = JSON.stringify(JSON.parse(value), null, 2);
      textArea.nativeElement.value = formatted;
      textArea.nativeElement.style.height = 'auto';
      const lineNumber = formatted.split('\n').length;
      const pixelPerLine = 20;
      const pixels = lineNumber * pixelPerLine;
      textArea.nativeElement.style.height = pixels + 'px';
    } catch (e) {}
  }
}
