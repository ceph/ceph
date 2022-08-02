import { fromEvent, Observable, partition } from 'rxjs';
import { repeatWhen, shareReplay, takeUntil } from 'rxjs/operators';

export function whenPageVisible() {
  const visibilitychange$ = fromEvent(document, 'visibilitychange').pipe(
    shareReplay({ refCount: true, bufferSize: 1 })
  );

  const [pageVisible$, pageHidden$] = partition(
    visibilitychange$,
    () => document.visibilityState === 'visible'
  );

  return function <T>(source: Observable<T>) {
    return source.pipe(
      takeUntil(pageHidden$),
      repeatWhen(() => pageVisible$)
    );
  };
}
