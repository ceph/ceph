export interface RgwRateLimitConfig {
  enabled: boolean;
  name?: string;
  max_read_ops: number;
  max_write_ops: number;
  max_read_bytes: number;
  max_write_bytes: number;
}
export interface GlobalRateLimitConfig {
  bucket_ratelimit: {
    max_read_ops: number;
    max_write_ops: number;
    max_read_bytes: number;
    max_write_bytes: number;
    enabled: boolean;
  };
  user_ratelimit: {
    max_read_ops: 1024;
    max_write_ops: number;
    max_read_bytes: number;
    max_write_bytes: number;
    enabled: boolean;
  };
  anonymous_ratelimit: {
    max_read_ops: number;
    max_write_ops: number;
    max_read_bytes: number;
    max_write_bytes: number;
    enabled: boolean;
  };
}
