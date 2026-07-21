import { Injectable } from '@angular/core';
import { ReplaySubject } from 'rxjs';

import { RbdService } from '~/app/shared/api/rbd.service';
import { ImageSpec } from '~/app/shared/models/image-spec';
import { RbdFormModel } from '../../ceph/block/rbd-form/rbd-form.model';

@Injectable()
export class RbdImageResourceStateService {
  private imageSource = new ReplaySubject<RbdFormModel | null>(1);

  readonly image$ = this.imageSource.asObservable();

  constructor(private rbdService: RbdService) {}

  load(imageSpecRoute: string): void {
    if (!imageSpecRoute) {
      this.imageSource.next(null);
      return;
    }

    try {
      const imageSpec = ImageSpec.fromString(decodeURIComponent(imageSpecRoute));
      this.rbdService.get(imageSpec).subscribe({
        next: (image: RbdFormModel) => this.imageSource.next(image),
        error: () => this.imageSource.next(null)
      });
    } catch {
      this.imageSource.next(null);
    }
  }
}
