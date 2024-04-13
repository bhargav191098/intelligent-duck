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
