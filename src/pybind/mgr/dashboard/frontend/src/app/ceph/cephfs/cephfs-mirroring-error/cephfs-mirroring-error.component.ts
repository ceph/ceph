import { Component } from '@angular/core';
import { IconSize } from '~/app/shared/enum/icons.enum';

@Component({
  selector: 'cd-cephfs-mirroring-error',
  templateUrl: './cephfs-mirroring-error.component.html',
  styleUrls: ['./cephfs-mirroring-error.component.scss'],
  standalone: false
})
export class CephfsMirroringErrorComponent {
  iconSize = IconSize;
}
