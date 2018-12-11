//
// Created by akatsarakis on 11/12/18.
//

#ifndef HERMES_BIT_VECTOR_H
#define HERMES_BIT_VECTOR_H

#include <stdint.h>
#include <string.h>
#include <assert.h>

// Change accordingly
#define BV_BIT_VECTOR_SIZE 8
#define BV_ENABLE_BIT_VECTOR_ASSERTS 1

// Do not change
#define BV_CEILING(x,y) (((x) + (y) - 1) / (y))
#define BV_BITS_IN_A_BYTE 8
#define BV_BIT_VECTOR_SIZE_IN_BYTES BV_CEILING(BV_BIT_VECTOR_SIZE, BV_BITS_IN_A_BYTE)

#define BIT_SLOT(bit) (bit / BV_BITS_IN_A_BYTE)
#define BIT_MOD(bit)  ((uint8_t) 1 << bit % BV_BITS_IN_A_BYTE)


typedef struct
{
    uint8_t bit_array[BV_BIT_VECTOR_SIZE_IN_BYTES];
}
bit_vector_t;

static inline void
bv_reset_all(bit_vector_t *bv)
{
    for(int i = 0; i < BV_BIT_VECTOR_SIZE_IN_BYTES; ++i)
        bv->bit_array[i] = 0;
}

static inline void
bv_init(bit_vector_t* bv)
{
    return bv_reset_all(bv);
}


static inline void
bv_bit_set(bit_vector_t *bv, uint8_t bit)
{
    if(BV_ENABLE_BIT_VECTOR_ASSERTS)
        assert(bit < BV_BIT_VECTOR_SIZE);
    bv->bit_array[BIT_SLOT(bit)] |= BIT_MOD(bit);
}

static inline void
bv_bit_reset(bit_vector_t *bv, uint8_t bit)
{
    if(BV_ENABLE_BIT_VECTOR_ASSERTS)
        assert(bit < BV_BIT_VECTOR_SIZE);

    bv->bit_array[BIT_SLOT(bit)] &= ~(BIT_MOD(bit));
}

static inline uint8_t
bv_bit_get(bit_vector_t bv, uint8_t bit)
{
    if(BV_ENABLE_BIT_VECTOR_ASSERTS)
        assert(bit < BV_BIT_VECTOR_SIZE);

    return (uint8_t) ((bv.bit_array[BIT_SLOT(bit)] & BIT_MOD(bit)) == 0 ? 0 : 1);
}

static inline void
bv_set_all(bit_vector_t* bv)
{
    for(int i = 0; i < BV_BIT_VECTOR_SIZE_IN_BYTES; ++i)
        bv->bit_array[i] = 255;
}

static inline void
bv_reverse(bit_vector_t* bv)
{
    for(int i = 0; i < BV_BIT_VECTOR_SIZE_IN_BYTES; ++i)
        bv->bit_array[i] = ~bv->bit_array[i];
}

static inline uint8_t
bv_are_equal(bit_vector_t bv1, bit_vector_t bv2)
{
    // shift the unused bits to avoid failing due to them
    // (difference only in the unused bits)
    uint8_t unused_ms_bits = BV_BITS_IN_A_BYTE * BV_BIT_VECTOR_SIZE_IN_BYTES - BV_BIT_VECTOR_SIZE;
    bv1.bit_array[BV_BIT_VECTOR_SIZE_IN_BYTES - 1] <<= unused_ms_bits;
    bv2.bit_array[BV_BIT_VECTOR_SIZE_IN_BYTES - 1] <<= unused_ms_bits;

    return (uint8_t) (memcmp(bv1.bit_array, bv2.bit_array, BV_BIT_VECTOR_SIZE_IN_BYTES) == 0 ? 1 : 0);
}

static inline void
bv_copy(bit_vector_t *bv_dst, bit_vector_t bv_src)
{
    memcpy(bv_dst->bit_array, bv_src.bit_array, BV_BIT_VECTOR_SIZE_IN_BYTES);
}

static inline void
bv_and(bit_vector_t *bv_dst, bit_vector_t bv_src)
{
    for(int i = 0; i < BV_BIT_VECTOR_SIZE_IN_BYTES; ++i)
        bv_dst->bit_array[i] &= bv_src.bit_array[i];
}

static inline void
bv_or(bit_vector_t *bv_dst, bit_vector_t bv_src)
{
    for(int i = 0; i < BV_BIT_VECTOR_SIZE_IN_BYTES; ++i)
        bv_dst->bit_array[i] |= bv_src.bit_array[i];
}

////////// Prints
#include <stdio.h>
//print binary numbers
#define BYTE_TO_BINARY_PATTERN "%c%c%c%c%c%c%c%c"
#define BYTE_TO_BINARY(byte)  \
  (byte & 0x80 ? '1' : '0'), \
  (byte & 0x40 ? '1' : '0'), \
  (byte & 0x20 ? '1' : '0'), \
  (byte & 0x10 ? '1' : '0'), \
  (byte & 0x08 ? '1' : '0'), \
  (byte & 0x04 ? '1' : '0'), \
  (byte & 0x02 ? '1' : '0'), \
  (byte & 0x01 ? '1' : '0')


static inline void
bv_print(bit_vector_t bv)
{
    for(int i = BV_BIT_VECTOR_SIZE_IN_BYTES - 1; i >= 0; --i)
        printf(BYTE_TO_BINARY_PATTERN, BYTE_TO_BINARY(bv.bit_array[i]));
}

static inline void
bv_print_enhanced(bit_vector_t bv)
{
    printf("Bit vector: ");
    bv_print(bv);
    printf("\n");
}

/////
static inline void
bv_unit_test()
{
    bit_vector_t bv;
    bit_vector_t bv_set__all;
    bv_init(&bv);
    bv_set_all(&bv_set__all);

    for(uint8_t i = 0; i < BV_BIT_VECTOR_SIZE; ++i){
        bv_bit_set(&bv, i);
//        bv_print_enhanced(bv);
    }
    assert(bv_are_equal(bv, bv_set__all) == 1);


    for(uint8_t i = 0; i < BV_BIT_VECTOR_SIZE; ++i){
        bv_bit_reset(&bv, i);
//        bv_print_enhanced(bv);
    }
    bv_reverse(&bv);
    assert(bv_are_equal(bv, bv_set__all) == 1);

    for(uint8_t i = 0; i < BV_BIT_VECTOR_SIZE; ++i)
        if(i % 2 == 0){
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
}


#endif //HERMES_BIT_VECTOR_H
