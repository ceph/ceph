import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';

export interface FileReadOptions {
  accept?: string[];
  maxBytes?: number;
}

export interface FileReadResult {
  content: string;
  file: File;
}

@Injectable({ providedIn: 'root' })
export class FileUploaderService {
  readAsText(files: Set<any>, options: FileReadOptions = {}): Observable<FileReadResult> {
    const { accept = [], maxBytes = 1_000_000 } = options;
    return new Observable((observer) => {
      const file: File = files?.values()?.next()?.value?.file;
      if (!file) {
        observer.error(new Error($localize`No file selected.`));
        return undefined;
      }

      if (maxBytes && file.size > maxBytes) {
        observer.error(new Error($localize`File exceeds the maximum size of ${maxBytes} bytes.`));
        return undefined;
      }

      if (accept.length) {
        const name = file.name.toLowerCase();
        const valid = accept.some((ext) =>
          ext.startsWith('.') ? name.endsWith(ext) : file.type === ext
        );
        if (!valid) {
          observer.error(new Error($localize`Invalid file type. Accepted: ${accept.join(', ')}.`));
          return undefined;
        }
      }

      const reader = new FileReader();
      reader.onload = (event: ProgressEvent<FileReader>) => {
        const result = event.target?.result;
        if (typeof result !== 'string') {
          observer.error(new Error($localize`Unable to read the file.`));
        } else {
          observer.next({ content: result, file });
          observer.complete();
        }
      };
      reader.onerror = () => observer.error(new Error($localize`Unable to read the file.`));
      reader.readAsText(file);
      return () => reader.abort();
    });
  }
}
