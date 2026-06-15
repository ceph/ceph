import { Component, OnInit, ViewEncapsulation } from '@angular/core';
import { ActivatedRoute, ParamMap } from '@angular/router';

@Component({
  selector: 'cd-cephfs-mirroring-fs-overview',
  templateUrl: './cephfs-mirroring-fs-overview.component.html',
  styleUrls: ['./cephfs-mirroring-fs-overview.component.scss'],
  standalone: false,
  encapsulation: ViewEncapsulation.None
})
export class CephfsMirroringFsOverviewComponent implements OnInit {
  fsName = '';

  constructor(private route: ActivatedRoute) {}

  ngOnInit(): void {
    this.route.parent?.paramMap.subscribe((paramMap: ParamMap) => {
      this.fsName = paramMap.get('fsName') ?? '';
    });
  }
}
