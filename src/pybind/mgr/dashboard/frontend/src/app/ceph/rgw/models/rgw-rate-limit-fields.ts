import { RgwRateLimitNumericKey } from './rgw-rate-limit';

export type RgwRateLimitKind = 'ops' | 'bytes';

/**
 * One description of a rate-limit threshold, shared by the user/bucket form,
 * the read-only details table, and the global rate-limit banner.
 */
export interface RgwRateLimitField {
  apiKey: RgwRateLimitNumericKey;
  control: string;
  unlimitedControl: string;
  kind: RgwRateLimitKind;
  label: string;
  unlimitedLabel: string;
  helpText: string;
  unlimitedHelp: string;
  bannerLabel: string;
  detailLabel: string;
  tooltip?: string;
}

interface RateLimitCopy {
  label: string;
  unlimitedLabel: string;
  helpText: string;
  unlimitedHelp: string;
  bannerLabel: string;
  detailLabel: string;
  tooltip?: string;
}

const listOpsTooltip = $localize`Limits bucket listing requests. List operations also count against the maximum read ops.`;
const deleteOpsTooltip = $localize`Limits delete operations. Delete operations also count against the maximum write ops. S3 multi-object delete is not rate limited.`;

function rateLimitField(
  apiKey: RgwRateLimitNumericKey,
  controlSuffix: string,
  kind: RgwRateLimitKind,
  copy: RateLimitCopy
): RgwRateLimitField {
  const control = `rate_limit_max_${controlSuffix}`;
  return {
    apiKey,
    kind,
    control,
    unlimitedControl: `${control}_unlimited`,
    ...copy
  };
}

/**
 * Form order groups operation limits together. List and delete sit after write
 * ops and before the byte limits.
 */
export const RGW_RATE_LIMIT_FIELDS: readonly RgwRateLimitField[] = [
  rateLimitField('max_read_ops', 'readOps', 'ops', {
    label: $localize`Maximum read ops`,
    unlimitedLabel: $localize`Unlimited read ops`,
    helpText: $localize`Limits the number of read operations per minute for a user.`,
    unlimitedHelp: $localize`Select this box to allow unlimited read operations.`,
    bannerLabel: $localize`Max. read ops`,
    detailLabel: $localize`Maximum Read Ops`
  }),
  rateLimitField('max_write_ops', 'writeOps', 'ops', {
    label: $localize`Maximum write ops`,
    unlimitedLabel: $localize`Unlimited write ops`,
    helpText: $localize`Limits the number of write operations per minute for a user.`,
    unlimitedHelp: $localize`Select this box to allow unlimited write operations.`,
    bannerLabel: $localize`Max. write ops`,
    detailLabel: $localize`Maximum Write Ops`
  }),
  rateLimitField('max_list_ops', 'listOps', 'ops', {
    label: $localize`Maximum list ops`,
    unlimitedLabel: $localize`Unlimited list ops`,
    helpText: $localize`Limits the number of list operations per minute. List operations also count against the maximum read ops.`,
    unlimitedHelp: $localize`Select this box to allow unlimited list operations. List operations also count against the maximum read ops.`,
    bannerLabel: $localize`Max. list ops`,
    detailLabel: $localize`Maximum List Ops`,
    tooltip: listOpsTooltip
  }),
  rateLimitField('max_delete_ops', 'deleteOps', 'ops', {
    label: $localize`Maximum delete ops`,
    unlimitedLabel: $localize`Unlimited delete ops`,
    helpText: $localize`Limits the number of delete operations per minute. Delete operations also count against the maximum write ops. S3 multi-object delete is not rate limited.`,
    unlimitedHelp: $localize`Select this box to allow unlimited delete operations. Delete operations also count against the maximum write ops. S3 multi-object delete is not rate limited.`,
    bannerLabel: $localize`Max. delete ops`,
    detailLabel: $localize`Maximum Delete Ops`,
    tooltip: deleteOpsTooltip
  }),
  rateLimitField('max_read_bytes', 'readBytes', 'bytes', {
    label: $localize`Maximum read bytes`,
    unlimitedLabel: $localize`Unlimited read bytes`,
    helpText: $localize`Limits the number of read bytes per minute for a user.`,
    unlimitedHelp: $localize`Select this box to allow unlimited read bytes.`,
    bannerLabel: $localize`Max. read bytes`,
    detailLabel: $localize`Maximum Read Bytes`
  }),
  rateLimitField('max_write_bytes', 'writeBytes', 'bytes', {
    label: $localize`Maximum write bytes`,
    unlimitedLabel: $localize`Unlimited write bytes`,
    helpText: $localize`Limits the number of write bytes per minute for a user.`,
    unlimitedHelp: $localize`Select this box to allow unlimited write bytes.`,
    bannerLabel: $localize`Max. write bytes`,
    detailLabel: $localize`Maximum Write Bytes`
  })
];

function fieldByKey(apiKey: RgwRateLimitNumericKey): RgwRateLimitField {
  const field = RGW_RATE_LIMIT_FIELDS.find((item) => item.apiKey === apiKey);
  if (!field) {
    throw new Error(`Missing rate limit field ${apiKey}`);
  }
  return field;
}

const bannerKeyOrder: RgwRateLimitNumericKey[] = [
  'max_read_bytes',
  'max_read_ops',
  'max_write_bytes',
  'max_write_ops',
  'max_list_ops',
  'max_delete_ops'
];

/** Banner keeps the historical read/write order and appends list and delete. */
export const RGW_RATE_LIMIT_BANNER_FIELDS: readonly RgwRateLimitField[] =
  bannerKeyOrder.map(fieldByKey);
