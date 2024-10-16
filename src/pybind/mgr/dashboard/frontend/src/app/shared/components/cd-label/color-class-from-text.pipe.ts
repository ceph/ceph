import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'colorClassFromText'
})
export class ColorClassFromTextPipe implements PipeTransform {
  readonly cssClasses: string[] = [
    'tag-cd-label-green',
    'tag-cd-label-cyan',
    'tag-cd-label-purple',
    'tag-cd-label-light-blue',
    'tag-cd-label-gold',
    'tag-cd-label-light-green'
  ];

  transform(text: string): string {
    let hash = 0;
    let charCode = 0;
    if (text) {
      for (let i = 0; i < text.length; i++) {
        charCode = text.charCodeAt(i);
        // eslint-disable-next-line no-bitwise
        hash = Math.abs((hash << 5) - hash + charCode);
      }
    }
    return this.cssClasses[hash % this.cssClasses.length];
  }
}
