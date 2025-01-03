import { Injectable } from '@angular/core';
import { CephfsService } from './cephfs.service';
import { BehaviorSubject, Observable, Subject, timer } from 'rxjs';
import { CephfsDir } from '../models/cephfs-directory-models';
import { filter, map, retry, share, switchMap, takeUntil, tap } from 'rxjs/operators';

type DirectoryStore = Record<number, CephfsDir[]>;

const POLLING_INTERVAL = 600 * 1000;

@Injectable({
  providedIn: 'root'
})
export class DirectoryStoreService {
  private _directoryStoreSubject = new BehaviorSubject<DirectoryStore>({});

  readonly directoryStore$: Observable<DirectoryStore> = this._directoryStoreSubject.asObservable();

  stopDirectoryPolling = new Subject();

  isLoading = true;

  constructor(private cephFsService: CephfsService) {}

  loadDirectories(id: number, path = '/', depth = 3) {
    this.directoryStore$
      .pipe(
        filter((store: DirectoryStore) => !Boolean(store[id])),
        switchMap(() =>
          timer(0, POLLING_INTERVAL).pipe(
            switchMap(() =>
              this.cephFsService.lsDir(id, path, depth).pipe(
                tap((response) => {
                  this.isLoading = false;
                  this._directoryStoreSubject.next({ [id]: response });
                })
              )
            ),
            retry(),
            share(),
            takeUntil(this.stopDirectoryPolling)
          )
        )
      )
      .subscribe();
  }

  search(term: string, id: number, limit = 5) {
    return this.directoryStore$.pipe(
      map((store: DirectoryStore) => {
        const regEx = new RegExp(term, 'gi');
        const results = store[id]
          .filter((x) => regEx.test(x.path))
          .map((x) => x.path)
          .slice(0, limit);
        return results;
      })
    );
  }

  stopPollingDictories() {
    this.stopDirectoryPolling.next();
  }
}
