import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';

export class PaginateObservable<Type> {
  observable: Observable<Type>;
  count: number;

  subscribe: any;
  constructor(obs: Observable<Type>) {
    this.observable = obs.pipe(
      map((response: any) => {
        this.count = Number(response.headers?.get('X-Total-Count'));
        return response['body'];
      })
    );
  }
}
