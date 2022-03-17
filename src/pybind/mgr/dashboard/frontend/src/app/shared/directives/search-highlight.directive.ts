import { Directive, ElementRef, Input, OnChanges, SimpleChanges } from '@angular/core';

@Directive({
  selector: '[cdHighlight]'
})
export class SearchHighlightDirective implements OnChanges {

  @Input('cdHighlight')
  highlight: string;

  @Input()
  cdSearchTerm: string;

  @Input()
  cdCaseSensitive = false;

  constructor(private el: ElementRef) {}

  ngOnChanges(changes: SimpleChanges) {
    if (changes.highlight?.isFirstChange()) {
      (this.el.nativeElement as HTMLElement).innerHTML = changes.highlight.currentValue;
      return;
    }
    if (this.el?.nativeElement) {
      if ('cdSearchTerm' in changes || 'cdCaseSensitive' in changes) {
        let text = (this.el.nativeElement as HTMLElement).textContent;
        if (this.cdSearchTerm !== '') {
          const regex = new RegExp(
            this.escapeRegExp(this.cdSearchTerm),
            this.cdCaseSensitive ? 'g' : 'gi'
          );
          text = text.replace(regex, (match: string) => {
            return `<span style="background-color:yellow;">${match}</span>`;
          });
        }
        (this.el.nativeElement as HTMLElement).innerHTML = text;
      }
    }
  }
  private escapeRegExp(str: string) {
    // $& means the whole matched string
    return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  }
}
