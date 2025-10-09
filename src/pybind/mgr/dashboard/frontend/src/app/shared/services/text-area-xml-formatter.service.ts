import { ElementRef, Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class TextAreaXmlFormatterService {
  constructor() {}

  format(textArea: ElementRef<any>): void {
    if (!textArea.nativeElement?.value) return;
    const value = textArea.nativeElement.value;
    const parser = new DOMParser();
    const formatted = parser.parseFromString(value, 'application/xml');
    const lineNumber = formatted.getElementsByTagName('*').length;
    const pixelPerLine = 20;
    const pixels = lineNumber * pixelPerLine;
    textArea.nativeElement.style.height = pixels + 'px';
    const errorNode = formatted.querySelector('parsererror');
    if (errorNode) {
      return;
    }
  }
}
