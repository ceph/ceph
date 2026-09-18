use rand::Rng;

pub fn generate_random(size: usize, part_size: usize) -> Vec<Vec<u8>> {
    let mut rng = rand::thread_rng();
    let allowed: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    let chunk = 1024usize;

    let mut parts = Vec::new();
    let mut remaining = size;

    while remaining > 0 {
        let this_part_size = remaining.min(part_size);

        let pattern: Vec<u8> = (0..chunk)
            .map(|_| allowed[rng.gen_range(0..allowed.len())])
            .collect();

        let mut part = Vec::with_capacity(this_part_size);
        let full_chunks = this_part_size / chunk;
        let leftover = this_part_size % chunk;
        for _ in 0..full_chunks {
            part.extend_from_slice(&pattern);
        }
        part.extend_from_slice(&pattern[..leftover]);

        parts.push(part);
        remaining -= this_part_size;
    }
    parts
}

pub const DEFAULT_PART_SIZE: usize = 5 * 1024 * 1024;
