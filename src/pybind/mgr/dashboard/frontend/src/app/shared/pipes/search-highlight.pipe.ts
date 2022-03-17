import { Pipe, PipeTransform } from '@angular/core';
import { DomSanitizer } from '@angular/platform-browser';

@Pipe({
  name: 'searchHighlight'
})
export class SearchHighlightPipe implements PipeTransform {
  constructor(private sanitizer: DomSanitizer) {}

  transform(value: string, args: string): any {
    if (!args) {
      return value;
    }
    args = this.escapeRegExp(args);
    const regex = new RegExp(args, 'gi');
    const match = value.match(regex);

    if (!match) {
      return value;
    }
    value = value.replace(regex, '<span style="background-color:yellow;">$&</span>');
    return this.sanitizer.bypassSecurityTrustHtml(value);
  }

  private escapeRegExp(str: string) {
    // $& means the whole matched string
    return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  }
}
