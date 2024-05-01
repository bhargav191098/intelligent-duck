#include<arm_neon.h>

long long count_bits(long long x) {
    // Split the 64-bit integer into 8-bit chunks
    uint8x8_t chunks = vcreate_u8(x);

    // Count the set bits in each chunk
    uint8x8_t counts = vcnt_u8(chunks);

    // Sum the counts
    uint16x4_t counts16 = vpaddl_u8(counts);
    uint32x2_t counts32 = vpaddl_u16(counts16);
    uint64x1_t counts64 = vpaddl_u32(counts32);

    // Extract the total count
    return vget_lane_u64(counts64, 0);
}

static __inline__ long long _mm_popcnt_u64(unsigned long long __A)
{
  return __builtin_popcountll(__A);
}

static __inline__ unsigned long long _tzcnt_u64( unsigned long long __A )
{
  return __builtin_ctzll(__A);
}

static __inline__ unsigned long long _lzcnt_u64(unsigned long long __A)
{
  return __builtin_clzll(__A);
}