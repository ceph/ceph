import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'redirectLinkResolver',
  standalone: false
})
export class RedirectLinkResolverPipe implements PipeTransform {
  transform(redirectLink: string[], value: string): string[] {
    return redirectLink.map((seg) => (seg === '::prop' ? value : seg));
  }
}
