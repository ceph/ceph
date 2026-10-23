/**
 * Numeric RGW rate-limit thresholds. Zero means the threshold is unlimited.
 * List operations are also counted against max_read_ops, and delete operations
 * are also counted against max_write_ops.
 */
export interface RgwRateLimitThresholds {
  max_read_ops: number;
  max_write_ops: number;
  max_list_ops: number;
  max_delete_ops: number;
  max_read_bytes: number;
  max_write_bytes: number;
}

export interface RgwRateLimitConfig extends RgwRateLimitThresholds {
  enabled: boolean;
  name?: string;
}

/** Global user, bucket, and anonymous scopes share the same threshold shape. */
export type RgwRateLimitScope = Omit<RgwRateLimitConfig, 'name'>;

export interface GlobalRateLimitConfig {
  bucket_ratelimit: RgwRateLimitScope;
  user_ratelimit: RgwRateLimitScope;
  anonymous_ratelimit: RgwRateLimitScope;
}

export type RgwRateLimitNumericKey = keyof RgwRateLimitThresholds;

/** Missing, zero, and non-positive values are displayed as unlimited. */
export function isRgwRateLimitUnlimited(value: number | null | undefined): boolean {
  return !(value > 0);
}
