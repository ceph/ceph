export interface RgwRateLimit{
    "enabled": string;
    "name": string | number;
    "max_read_ops": string | number;
    "max_write_ops": string | number;
    "max_read_bytes": string | number;
    "max_write_bytes": string | number;
}
export interface GlobalRateLimit{
    "bucket_ratelimit": {
        "max_read_ops": string | number,
        "max_write_ops": string | number
        "max_read_bytes": string | number
        "max_write_bytes": string | number
        "enabled": string
    },
    "user_ratelimit": {
        "max_read_ops": 1024,
        "max_write_ops": string | number
        "max_read_bytes": string | number
        "max_write_bytes": string | number
        "enabled": string
    },
    "anonymous_ratelimit": {
        "max_read_ops": string | number
        "max_write_ops": string | number
        "max_read_bytes": string | number
        "max_write_bytes": string | number
        "enabled": false
    }
}