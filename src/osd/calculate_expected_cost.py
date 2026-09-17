#!/usr/bin/env python3

# With k=2, m=1, stripe_unit=4KB
# Data striping: round-robin across k=2 data shards

objects = [
    (4, 20),    # 4KB × 20
    (8, 20),    # 8KB × 20
    (16, 20),   # etc.
    (32, 20),
    (64, 20),
    (128, 20),
    (256, 20),
    (512, 20),
    (1024, 20),
]

stripe_unit = 4  # KB
k = 2  # data shards

print("Expected data per shard for each object size:\n")

total_shard_0 = 0
total_shard_1 = 0
total_shard_2 = 0

for size_kb, count in objects:
    # How many stripe units does this object need?
    stripe_units = (size_kb + stripe_unit - 1) // stripe_unit
    
    # Distribute across k data shards
    shard_0_units = (stripe_units + 1) // 2  # Ceiling division
    shard_1_units = stripe_units // 2         # Floor division
    
    shard_0_kb = shard_0_units * stripe_unit
    shard_1_kb = shard_1_units * stripe_unit
    # Shard 2 has parity (roughly same as max data shard)
    shard_2_kb = max(shard_0_kb, shard_1_kb)
    
    print(f"{size_kb:4d}KB × {count}: shard0={shard_0_kb:4d}KB, shard1={shard_1_kb:4d}KB, shard2={shard_2_kb:4d}KB (parity)")
    
    total_shard_0 += shard_0_kb * count
    total_shard_1 += shard_1_kb * count
    total_shard_2 += shard_2_kb * count

print(f"\nTotal per shard:")
print(f"  Shard 0: {total_shard_0:,} KB ({total_shard_0/1024:.1f} MB)")
print(f"  Shard 1: {total_shard_1:,} KB ({total_shard_1/1024:.1f} MB)")
print(f"  Shard 2: {total_shard_2:,} KB ({total_shard_2/1024:.1f} MB)")

print(f"\nWith 1000 bytes overhead per object:")
overhead_kb = (1000 * 180) / 1024
print(f"  Overhead: {overhead_kb:.1f} KB")
print(f"\nExpected message costs:")
print(f"  Shard 0: {(total_shard_0 * 1024 + 180 * 1000):,} bytes ({(total_shard_0 * 1024 + 180 * 1000)/1024/1024:.2f} MB)")
print(f"  Shard 1: {(total_shard_1 * 1024 + 180 * 1000):,} bytes ({(total_shard_1 * 1024 + 180 * 1000)/1024/1024:.2f} MB)")
print(f"  Shard 2: {(total_shard_2 * 1024 + 180 * 1000):,} bytes ({(total_shard_2 * 1024 + 180 * 1000)/1024/1024:.2f} MB)")

print(f"\nActual observed: 180,000 bytes (0.17 MB) - ALL overhead, NO data!")
