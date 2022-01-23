//
// Created by akatsarakis on 11/12/18.
//

#ifndef HERMES_BIT_VECTOR_H
#define HERMES_BIT_VECTOR_H

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Change accordingly
#define BV_BIT_VECTOR_SIZE \
  8  // Set if you use statical bit vector (bit_vector_t)
#define BV_ENABLE_BIT_VECTOR_ASSERTS 1

// Do not change the following defines
#define BV_CEILING(x, y) (((x) + (y)-1) / (y))
#define BV_BITS_IN_A_BYTE 8

#define BV_BIT_VECTOR_SIZE_IN_BYTES \
  BV_CEILING(BV_BIT_VECTOR_SIZE, BV_BITS_IN_A_BYTE)

#define BV_BIT_SLOT(bit) (bit / BV_BITS_IN_A_BYTE)
#define BV_BIT_MOD(bit) ((uint8_t)1 << bit % BV_BITS_IN_A_BYTE)

// print binary numbers
#define BYTE_TO_BINARY_PATTERN "%c%c%c%c%c%c%c%c"
#define BYTE_TO_BINARY(byte)                                \
  (byte & 0x80 ? '1' : '0'), (byte & 0x40 ? '1' : '0'),     \
      (byte & 0x20 ? '1' : '0'), (byte & 0x10 ? '1' : '0'), \
      (byte & 0x08 ? '1' : '0'), (byte & 0x04 ? '1' : '0'), \
      (byte & 0x02 ? '1' : '0'), (byte & 0x01 ? '1' : '0')

typedef struct {
  uint8_t bit_array[BV_BIT_VECTOR_SIZE_IN_BYTES];
} bit_vector_t;

typedef struct {
  uint8_t bv_size;     // in bits
  uint8_t* bit_array;  // bit_array len == ceil(bv_size / 8)
} dbit_vector_t;

// returns the least amount of bytes that required to store x bits
static inline uint16_t
bv_bits_to_bytes(uint16_t bits)
{
  return (uint16_t)BV_CEILING(bits, BV_BITS_IN_A_BYTE);
}

/////////////////////////////////////////
/// Internal Bitvector API functions (should not be called directly)
/////////////////////////////////////////

static inline void
bv_init_internal(uint8_t* bit_array, uint16_t size_in_bits)
{
  for (int i = 0; i < bv_bits_to_bytes(size_in_bits); ++i)
    bit_array[i] = 0;
}

static inline uint8_t
bv_bit_get_internal(const uint8_t* bit_array, uint16_t size_in_bits,
                    uint8_t bit)
{
  if (BV_ENABLE_BIT_VECTOR_ASSERTS) assert(bit < size_in_bits);

  return (uint8_t)((bit_array[BV_BIT_SLOT(bit)] & BV_BIT_MOD(bit)) == 0 ? 0
                                                                        : 1);
}

static inline void
bv_bit_set_internal(uint8_t* bit_array, uint16_t size_in_bits, uint8_t bit)
{
  if (BV_ENABLE_BIT_VECTOR_ASSERTS) assert(bit < size_in_bits);

  bit_array[BV_BIT_SLOT(bit)] |= BV_BIT_MOD(bit);
}

static inline void
bv_bit_reset_internal(uint8_t* bit_array, uint16_t size_in_bits, uint8_t bit)
{
  if (BV_ENABLE_BIT_VECTOR_ASSERTS) assert(bit < size_in_bits);

  bit_array[BV_BIT_SLOT(bit)] &= ~(BV_BIT_MOD(bit));
}

static inline void
bv_set_all_internal(uint8_t* bit_array, uint16_t size_in_bits)
{
  uint8_t bytes = (uint8_t)bv_bits_to_bytes(size_in_bits);
  uint8_t unused = (uint8_t)(bytes * 8 - size_in_bits);
  uint8_t last_byte = (uint8_t)(255 >> unused);

  for (int i = 0; i < bytes - 1; ++i)
    bit_array[i] = 255;

  bit_array[bytes - 1] = last_byte;
}

static inline void
bv_reset_all_internal(uint8_t* bit_array, uint16_t size_in_bits)
{
  for (int i = 0; i < bv_bits_to_bytes(size_in_bits); ++i)
    bit_array[i] = 0;
}

static inline uint8_t
bv_are_equal_internal(uint8_t* ba1, uint16_t size_in_bits1, uint8_t* ba2,
                      uint16_t size_in_bits2)
{
  if (size_in_bits1 != size_in_bits2) return 0;

  uint16_t size_in_bytes = bv_bits_to_bytes(size_in_bits1);

  // shift the unused bits to avoid failing due to them
  // (difference only in the unused bits)
  uint8_t unused_ms_bits =
      (uint8_t)(BV_BITS_IN_A_BYTE * size_in_bytes - size_in_bits1);
  uint8_t last_byte1 = ba1[size_in_bytes - 1] << unused_ms_bits;
  uint8_t last_byte2 = ba2[size_in_bytes - 1] << unused_ms_bits;

  return (uint8_t)(memcmp(ba1, ba2, (size_t)(size_in_bytes - 1)) == 0 &&
                           last_byte1 == last_byte2
                       ? 1
                       : 0);
}

static inline void
bv_copy_internal(uint8_t* ba_dst, uint16_t size_in_bits_dst, uint8_t* ba_src,
                 uint16_t size_in_bits_src)
{
  // allow copy only if sizes match
  if (size_in_bits_dst != size_in_bits_src) assert(0);

  memcpy(ba_dst, ba_src, bv_bits_to_bytes(size_in_bits_src));
}

static inline uint8_t
bv_no_setted_bits_internal(uint8_t* bit_array, uint16_t size_in_bits)
{
  uint8_t cnt = 0;
  for (uint8_t i = 0; i < size_in_bits; ++i)
    cnt += bv_bit_get_internal(bit_array, size_in_bits, i);
  return cnt;
}

/// Bitvector Bitwise ops internal

static inline void
bv_reverse_internal(uint8_t* bit_array, uint16_t size_in_bits)
{
  for (int i = 0; i < bv_bits_to_bytes(size_in_bits); ++i)
    bit_array[i] = ~bit_array[i];
}

static inline void
bv_and_internal(uint8_t* ba_dst, uint16_t size_in_bits_dst,
                const uint8_t* ba_src, uint16_t size_in_bits_src)
{
  // allow and only if sizes match
  if (size_in_bits_dst != size_in_bits_src) assert(0);

  for (int i = 0; i < bv_bits_to_bytes(size_in_bits_dst); ++i)
    ba_dst[i] &= ba_src[i];
}

static inline void
bv_or_internal(uint8_t* ba_dst, uint16_t size_in_bits_dst,
               const uint8_t* ba_src, uint16_t size_in_bits_src)
{
  // allow or only if sizes match
  if (size_in_bits_dst != size_in_bits_src) assert(0);

  for (int i = 0; i < bv_bits_to_bytes(size_in_bits_dst); ++i)
    ba_dst[i] |= ba_src[i];
}

/// Bitvector Print functions

static inline void
bv_print_internal(const uint8_t* bit_array, uint16_t size_in_bits)
{
  for (int i = bv_bits_to_bytes(size_in_bits) - 1; i >= 0; --i)
    printf(BYTE_TO_BINARY_PATTERN, BYTE_TO_BINARY(bit_array[i]));
}

static inline void
bv_print_enhanced_internal(const uint8_t* bit_array, uint16_t size_in_bits)
{
  printf("Bit vector: ");
  bv_print_internal(bit_array, size_in_bits);
  printf("\n");
}

/////////////////////////////////////////
/// Dynamic Bitvector API functions
/////////////////////////////////////////
static inline void
dbv_init(dbit_vector_t** bv, uint8_t size)
{
  uint16_t bv_size_in_bytes = bv_bits_to_bytes(size);
  *bv = malloc(sizeof(dbit_vector_t));
  (*bv)->bit_array = malloc(bv_size_in_bytes * sizeof(uint8_t));
  (*bv)->bv_size = size;
  bv_init_internal((*bv)->bit_array, size);
}

static inline void
dbv_destroy(dbit_vector_t* bv)
{
  free(bv->bit_array);
  free(bv);
}

static inline uint8_t
dbv_bit_get(dbit_vector_t bv, int bit)
{
  return bv_bit_get_internal(bv.bit_array, bv.bv_size, bit);
}

static inline void
dbv_bit_set(dbit_vector_t* bv, uint8_t bit)
{
  bv_bit_set_internal(bv->bit_array, bv->bv_size, bit);
}

static inline void
dbv_bit_reset(dbit_vector_t* bv, uint8_t bit)
{
  bv_bit_reset_internal(bv->bit_array, bv->bv_size, bit);
}

static inline void
dbv_set_all(dbit_vector_t* bv)
{
  bv_set_all_internal(bv->bit_array, bv->bv_size);
}

static inline void
dbv_reset_all(dbit_vector_t* bv)
{
  bv_reset_all_internal(bv->bit_array, bv->bv_size);
}

static inline uint8_t
dbv_no_setted_bits(dbit_vector_t bv)
{
  return bv_no_setted_bits_internal(bv.bit_array, bv.bv_size);
}

static inline uint8_t
dbv_are_equal(dbit_vector_t bv1, dbit_vector_t bv2)
{
  return bv_are_equal_internal(bv1.bit_array, bv1.bv_size, bv2.bit_array,
                               bv2.bv_size);
}

static inline void
dbv_copy(dbit_vector_t* bv_dst, dbit_vector_t bv_src)
{
  bv_copy_internal(bv_dst->bit_array, bv_dst->bv_size, bv_src.bit_array,
                   bv_src.bv_size);
}

static inline uint8_t
dbv_is_all_set(dbit_vector_t bv)
{
  dbit_vector_t* bv_tmp;
  dbv_init(&bv_tmp, bv.bv_size);
  dbv_set_all(bv_tmp);
  return dbv_are_equal(bv, *bv_tmp);
}

/// Bitvector bitwise ops
static inline void
dbv_reverse(dbit_vector_t* bv)
{
  bv_reverse_internal(bv->bit_array, bv->bv_size);
}

static inline void
dbv_and(dbit_vector_t* bv_dst, dbit_vector_t bv_src)
{
  bv_and_internal(bv_dst->bit_array, bv_dst->bv_size, bv_src.bit_array,
                  bv_src.bv_size);
}

static inline void
dbv_or(dbit_vector_t* bv_dst, dbit_vector_t bv_src)
{
  bv_or_internal(bv_dst->bit_array, bv_dst->bv_size, bv_src.bit_array,
                 bv_src.bv_size);
}

/// Bitvector Print functions

static inline void
dbv_print(dbit_vector_t bv)
{
  bv_print_internal(bv.bit_array, bv.bv_size);
}

static inline void
dbv_print_enhanced(dbit_vector_t bv)
{
  bv_print_enhanced_internal(bv.bit_array, bv.bv_size);
}

static inline void
dbv_unit_test(void)
{
  dbit_vector_t* bv;
  dbit_vector_t* bv_set__all;
  dbv_init(&bv, 22);
  dbv_init(&bv_set__all, 22);
  dbv_set_all(bv_set__all);

  for (uint8_t i = 0; i < bv->bv_size; ++i)
    dbv_bit_set(bv, i);
  assert(dbv_are_equal(*bv, *bv_set__all) == 1);

  for (uint8_t i = 0; i < bv->bv_size; ++i)
    dbv_bit_reset(bv, i);
  dbv_reverse(bv);
  assert(dbv_are_equal(*bv, *bv_set__all) == 1);

  for (uint8_t i = 0; i < bv->bv_size; ++i)
    if (i % 2 == 0) {
      dbv_bit_reset(bv, i);
      assert(dbv_bit_get(*bv, i) == 0);
    } else {
      dbv_bit_set(bv, i);
      assert(dbv_bit_get(*bv, i) == 1);
    }

  dbv_reset_all(bv);
  assert(dbv_are_equal(*bv, *bv_set__all) == 0);

  dbv_set_all(bv);
  dbv_and(bv, *bv_set__all);
  assert(dbv_are_equal(*bv, *bv_set__all) == 1);

  dbv_copy(bv, *bv_set__all);
  assert(dbv_are_equal(*bv, *bv_set__all) == 1);

  dbv_reset_all(bv);
  dbv_or(bv, *bv_set__all);
  assert(dbv_are_equal(*bv, *bv_set__all) == 1);
  printf("Dynamic Bit Vector Unit Test was Successful!\n");
}

/////////////////////////////////////////
/// Static Bitvector API functions
/////////////////////////////////////////

static inline void
bv_init(bit_vector_t* bv)
{
  bv_init_internal(bv->bit_array, BV_BIT_VECTOR_SIZE);
}

static inline uint8_t
bv_bit_get(bit_vector_t bv, int bit)
{
  return bv_bit_get_internal(bv.bit_array, BV_BIT_VECTOR_SIZE, bit);
}

static inline void
bv_bit_set(bit_vector_t* bv, uint8_t bit)
{
  bv_bit_set_internal(bv->bit_array, BV_BIT_VECTOR_SIZE, bit);
}

static inline void
bv_bit_reset(bit_vector_t* bv, uint8_t bit)
{
  bv_bit_reset_internal(bv->bit_array, BV_BIT_VECTOR_SIZE, bit);
}

static inline void
bv_set_all(bit_vector_t* bv)
{
  bv_set_all_internal(bv->bit_array, BV_BIT_VECTOR_SIZE);
}

static inline void
bv_reset_all(bit_vector_t* bv)
{
  bv_reset_all_internal(bv->bit_array, BV_BIT_VECTOR_SIZE);
}

static inline uint8_t
bv_no_setted_bits(bit_vector_t bv)
{
  return bv_no_setted_bits_internal(bv.bit_array, BV_BIT_VECTOR_SIZE);
}

static inline uint8_t
bv_are_equal(bit_vector_t bv1, bit_vector_t bv2)
{
  return bv_are_equal_internal(bv1.bit_array, BV_BIT_VECTOR_SIZE, bv2.bit_array,
                               BV_BIT_VECTOR_SIZE);
}

static inline void
bv_copy(bit_vector_t* bv_dst, bit_vector_t bv_src)
{
  bv_copy_internal(bv_dst->bit_array, BV_BIT_VECTOR_SIZE, bv_src.bit_array,
                   BV_BIT_VECTOR_SIZE);
}

/// Bitvector bitwise ops
static inline void
bv_reverse(bit_vector_t* bv)
{
  bv_reverse_internal(bv->bit_array, BV_BIT_VECTOR_SIZE);
}

static inline void
bv_and(bit_vector_t* bv_dst, bit_vector_t bv_src)
{
  bv_and_internal(bv_dst->bit_array, BV_BIT_VECTOR_SIZE, bv_src.bit_array,
                  BV_BIT_VECTOR_SIZE);
}

static inline void
bv_or(bit_vector_t* bv_dst, bit_vector_t bv_src)
{
  bv_or_internal(bv_dst->bit_array, BV_BIT_VECTOR_SIZE, bv_src.bit_array,
                 BV_BIT_VECTOR_SIZE);
}

/// Bitvector Print functions

static inline void
bv_print(bit_vector_t bv)
{
  bv_print_internal(bv.bit_array, BV_BIT_VECTOR_SIZE);
}

static inline void
bv_print_enhanced(bit_vector_t bv)
{
  bv_print_enhanced_internal(bv.bit_array, BV_BIT_VECTOR_SIZE);
}

/////////////////////////////////////////
/// Bitvector unit test functions
/////////////////////////////////////////
static inline void
bv_unit_test(void)
{
  bit_vector_t bv;
  bit_vector_t bv_set__all;
  bv_init(&bv);
  bv_set_all(&bv_set__all);

  dbv_unit_test();

  for (uint8_t i = 0; i < BV_BIT_VECTOR_SIZE; ++i)
    bv_bit_set(&bv, i);
  assert(bv_are_equal(bv, bv_set__all) == 1);

  for (uint8_t i = 0; i < BV_BIT_VECTOR_SIZE; ++i)
    bv_bit_reset(&bv, i);
  bv_reverse(&bv);
  assert(bv_are_equal(bv, bv_set__all) == 1);

  for (uint8_t i = 0; i < BV_BIT_VECTOR_SIZE; ++i)
    if (i % 2 == 0) {
      bv_bit_reset(&bv, i);
      assert(bv_bit_get(bv, i) == 0);
    } else {
      bv_bit_set(&bv, i);
      assert(bv_bit_get(bv, i) == 1);
    }

  bv_reset_all(&bv);
  assert(bv_are_equal(bv, bv_set__all) == 0);

  bv_set_all(&bv);
  bv_and(&bv, bv_set__all);
  assert(bv_are_equal(bv, bv_set__all) == 1);

  bv_copy(&bv, bv_set__all);
  assert(bv_are_equal(bv, bv_set__all) == 1);

  bv_reset_all(&bv);
  bv_or(&bv, bv_set__all);
  assert(bv_are_equal(bv, bv_set__all) == 1);
  printf("Static  Bit Vector Unit Test was Successful!\n");
}

#endif  // HERMES_BIT_VECTOR_H
