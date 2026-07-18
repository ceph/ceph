import {
  Component,
  EventEmitter,
  Input,
  OnChanges,
  OnInit,
  Output,
  TemplateRef,
  ViewChild,
  ViewEncapsulation
} from '@angular/core';

import * as xml2js from 'xml2js';
import { tap } from 'rxjs/operators';

import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { NotificationService } from '~/app/shared/services/notification.service';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { RgwBucketMfaDelete } from '../models/rgw-bucket-mfa-delete';
import { Bucket } from '../models/rgw-bucket';
import { RgwBucketVersioning } from '../models/rgw-bucket-versioning';
import {
  RgwBucketAclGrantee as Grantee,
  RgwBucketAclPermissions as AclPermission
} from '../rgw-bucket-form/rgw-bucket-acl-permissions.enum';

interface BucketTagRow {
  key: string;
  value: string;
}

@Component({
  selector: 'cd-rgw-bucket-tags-table',
  templateUrl: './rgw-bucket-tags-table.component.html',
  styleUrls: ['./rgw-bucket-tags-table.component.scss'],
  encapsulation: ViewEncapsulation.None,
  standalone: false
})
export class RgwBucketTagsTableComponent implements OnInit, OnChanges {
  @Input() bucket: Bucket;
  @Input() owner = '';
  @Output() tagsUpdated = new EventEmitter<void>();

  @ViewChild('removeTagTpl', { static: true })
  removeTagTpl: TemplateRef<any>;

  columns: CdTableColumn[] = [];
  tags: BucketTagRow[] = [];

  constructor(
    private rgwBucketService: RgwBucketService,
    private modalService: ModalCdsService,
    private notificationService: NotificationService
  ) {}

  ngOnInit(): void {
    this.columns = [
      {
        name: $localize`Key`,
        prop: 'key',
        flexGrow: 2
      },
      {
        name: $localize`Value`,
        prop: 'value',
        flexGrow: 3
      },
      {
        name: $localize`Action`,
        prop: 'key',
        sortable: false,
        flexGrow: 1,
        cellTemplate: this.removeTagTpl
      }
    ];
    this.syncTags();
  }

  ngOnChanges(): void {
    this.syncTags();
  }

  deleteTag(tag: BucketTagRow, event: Event): void {
    event.preventDefault();
    event.stopPropagation();

    this.modalService.show(DeleteConfirmationModalComponent, {
      itemDescription: $localize`Tag`,
      itemNames: [`${tag.key}: ${tag.value}`],
      actionDescription: $localize`remove`,
      submitActionObservable: () => this.submitDelete(tag)
    });
  }

  private syncTags(): void {
    const tagset = this.bucket?.tagset || {};
    this.tags = Object.keys(tagset).map((key) => ({
      key,
      value: `${tagset[key] ?? ''}`
    }));
  }

  private submitDelete(tagToRemove: BucketTagRow) {
    const remainingTags = this.tags.filter((tag) => tag.key !== tagToRemove.key);

    return this.rgwBucketService
      .update(
        this.bucket?.bid,
        this.bucket?.id,
        this.resolveOwner(),
        this.resolveVersioningState(),
        this.bucket?.encryption === 'Enabled',
        this.bucket?.encryption_type || '',
        this.bucket?.key_id || '',
        this.bucket?.mfa_delete || RgwBucketMfaDelete.DISABLED,
        '',
        '',
        this.bucket?.lock_mode || 'COMPLIANCE',
        `${this.bucket?.lock_retention_period_days ?? 0}`,
        this.tagsToXML(remainingTags),
        this.stringifyJson(this.bucket?.bucket_policy, '{}'),
        this.resolveCannedAcl(),
        this.hasReplicationRules() ? 'true' : 'false',
        this.stringifyJson(this.bucket?.lifecycle, '{}')
      )
      .pipe(
        tap(() => {
          this.notificationService.show(
            NotificationType.success,
            $localize`Bucket tag deleted successfully`
          );
          this.tagsUpdated.emit();
        })
      );
  }

  private resolveOwner(): string {
    if (this.owner && this.bucket?.owner && this.owner !== this.bucket.owner) {
      return '';
    }

    return this.owner || this.bucket?.owner || '';
  }

  private resolveVersioningState(): string {
    if (this.bucket?.versioning === RgwBucketVersioning.OFF) {
      return '';
    }

    return this.bucket?.versioning || '';
  }

  private hasReplicationRules(): boolean {
    const replicationConfig = this.bucket?.replication;

    return (
      replicationConfig?.replication_rules_configured === true &&
      replicationConfig?.policy?.Rule?.Status === 'Enabled'
    );
  }

  private stringifyJson(value: any, fallback: string): string {
    if (!value) {
      return fallback;
    }

    return typeof value === 'string' ? value : JSON.stringify(value);
  }

  private escapeXml(value: string): string {
    if (!value) return '';
    return value.replace(/[&<>"']/g, (char) => {
      switch (char) {
        case '&':
          return '&amp;';
        case '<':
          return '&lt;';
        case '>':
          return '&gt;';
        case '"':
          return '&quot;';
        case "'":
          return '&apos;';
        default:
          return char;
      }
    });
  }

  private tagsToXML(tags: BucketTagRow[]): string {
    let xml = '<Tagging><TagSet>';

    for (const tag of tags) {
      xml += `<Tag><Key>${this.escapeXml(tag.key)}</Key><Value>${this.escapeXml(tag.value)}</Value></Tag>`;
    }

    xml += '</TagSet></Tagging>';
    return xml;
  }

  private resolveCannedAcl(): string {
    const parser = new xml2js.Parser({ explicitArray: false, trim: true });
    let selectedGrantee: string = Grantee.Owner;
    let selectedAclPermission: string = AclPermission.FullControl;

    parser.parseString(this.bucket?.acl, (err, result) => {
      if (err) {
        return;
      }

      const xmlGrantees = result?.AccessControlPolicy?.AccessControlList?.Grant;
      const grants = Array.isArray(xmlGrantees) ? xmlGrantees : [xmlGrantees];

      for (const grant of grants) {
        if (grant?.Grantee?.ID === this.bucket?.owner) {
          continue;
        }

        if (grant?.Grantee?.URI?.includes('AllUsers')) {
          selectedGrantee = Grantee.Everyone;
          if (grant?.Permission === 'READ') {
            selectedAclPermission = AclPermission.Read;
          } else {
            selectedAclPermission = AclPermission.All;
          }
        } else if (grant?.Grantee?.URI?.includes('AuthenticatedUsers')) {
          selectedGrantee = Grantee.AuthenticatedUsers;
          selectedAclPermission = AclPermission.Read;
        }
      }
    });

    switch (selectedGrantee) {
      case Grantee.Everyone:
        return selectedAclPermission === AclPermission.Read ? 'public-read' : 'public-read-write';
      case Grantee.AuthenticatedUsers:
        return 'authenticated-read';
      default:
        return 'private';
    }
  }
}
