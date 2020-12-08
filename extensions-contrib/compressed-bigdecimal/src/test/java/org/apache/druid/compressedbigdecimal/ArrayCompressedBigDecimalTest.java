/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.compressedbigdecimal;

import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

/**
 * Unit tests for CompressedBigDecimal.
 */
public class ArrayCompressedBigDecimalTest
{

  /**
   * Test method for {@link ArrayCompressedBigDecimal#ArrayCompressedBigDecimal(long, int)}.
   */
  @Test
  public void testLongConstructorZero()
  {
    // Validate simple 0 case with longs.
    ArrayCompressedBigDecimal d = new ArrayCompressedBigDecimal(0, 0);
    assertEquals(0, d.getScale());
    int[] array = d.getArray();
    assertEquals(2, array.length);
    assertEquals(0, array[0]);
    assertEquals(0, array[1]);
  }

  /**
   * Test method for {@link ArrayCompressedBigDecimal#ArrayCompressedBigDecimal}.
   */
  @Test
  public void testLongConstructorPositive()
  {
    // Validate positive number that doesn't flow into the next int
    ArrayCompressedBigDecimal d = new ArrayCompressedBigDecimal(Integer.MAX_VALUE, 9);
    assertEquals(9, d.getScale());
    int[] array = d.getArray();
    assertEquals(2, array.length);
    assertEquals(Integer.MAX_VALUE, array[0]);
    assertEquals(0, array[1]);
  }

  /**
   * Test method for {@link ArrayCompressedBigDecimal#ArrayCompressedBigDecimal}.
   */
  @Test
  public void testLongConstructorNegative()
  {
    // validate negative number correctly fills in upper bits.
    ArrayCompressedBigDecimal d = new ArrayCompressedBigDecimal(Integer.MIN_VALUE, 5);
    assertEquals(5, d.getScale());
    int[] array = d.getArray();
    assertEquals(2, array.length);
    assertEquals(Integer.MIN_VALUE, array[0]);
    assertEquals(-1, array[1]);
  }

  /**
   * Test method for {@link ArrayCompressedBigDecimal#ArrayCompressedBigDecimal(java.math.BigDecimal)}.
   */
  @Test
  public void testBigDecimalConstructorZero()
  {
    // simple zero case to test short circuiting
    BigDecimal bd = new BigDecimal(0);
    ArrayCompressedBigDecimal d = new ArrayCompressedBigDecimal(bd);
    assertEquals(0, d.getScale());
    int[] array = d.getArray();
    assertEquals(1, array.length);
    assertEquals(0, array[0]);
  }

  /**
   * Test method for {@link ArrayCompressedBigDecimal#ArrayCompressedBigDecimal}.
   */
  @Test
  public void testBigDecimalConstructorSmallPositive()
  {
    // simple one int positive example
    BigDecimal bd = new BigDecimal(Integer.MAX_VALUE).scaleByPowerOfTen(-9);
    ArrayCompressedBigDecimal d = new ArrayCompressedBigDecimal(bd);
    assertEquals(9, d.getScale());
    int[] array = d.getArray();
    assertEquals(1, array.length);
    assertEquals(Integer.MAX_VALUE, array[0]);
  }

  /**
   * Test method for {@link ArrayCompressedBigDecimal#ArrayCompressedBigDecimal}.
   */
  @Test
  public void testBigDecimalConstructorSmallNegative()
  {
    // simple one int negative example
    BigDecimal bd = new BigDecimal(Integer.MIN_VALUE).scaleByPowerOfTen(-5);
    ArrayCompressedBigDecimal d = new ArrayCompressedBigDecimal(bd);
    assertEquals(5, d.getScale());
    int[] array = d.getArray();
    assertEquals(1, array.length);
    assertEquals(Integer.MIN_VALUE, array[0]);
  }

  /**
   * Test method for {@link ArrayCompressedBigDecimal#ArrayCompressedBigDecimal}.
   */
  @Test
  public void testBigDecimalConstructorLargePositive()
  {
    // simple two int positive example
    BigDecimal bd = new BigDecimal(Long.MAX_VALUE).scaleByPowerOfTen(-9);
    ArrayCompressedBigDecimal d = new ArrayCompressedBigDecimal(bd);
    assertEquals(9, d.getScale());
    int[] array = d.getArray();
    assertEquals(2, array.length);
    assertEquals(-1, array[0]);
    assertEquals(Integer.MAX_VALUE, array[1]);
  }

  /**
   * Test method for {@link ArrayCompressedBigDecimal#ArrayCompressedBigDecimal}.
   */
  @Test
  public void testBigDecimalConstructorLargeNegative()
  {
    // simple two int negative example
    BigDecimal bd = new BigDecimal(Long.MIN_VALUE).scaleByPowerOfTen(-5);
    ArrayCompressedBigDecimal d = new ArrayCompressedBigDecimal(bd);
    assertEquals(5, d.getScale());
    int[] array = d.getArray();
    assertEquals(2, array.length);
    assertEquals(0, array[0]);
    assertEquals(Integer.MIN_VALUE, array[1]);
  }

  /**
   * Test method for {@link ArrayCompressedBigDecimal#ArrayCompressedBigDecimal}.
   */
  @Test
  public void testBigDecimalConstructorUnevenMultiplePositive()
  {
    // test positive when number of bytes in BigDecimal isn't an even multiple of sizeof(int)
    BigDecimal bd = new BigDecimal(new BigInteger(1, new byte[] {0x7f, -1, -1, -1, -1}));
    ArrayCompressedBigDecimal d = new ArrayCompressedBigDecimal(bd);
    assertEquals(0, d.getScale());
    int[] array = d.getArray();
    assertEquals(2, array.length);
    assertEquals(-1, array[0]);
    assertEquals(0x7f, array[1]);
  }

  /**
   * Test method for {@link ArrayCompressedBigDecimal#ArrayCompressedBigDecimal}.
   */
  @Test
  public void testBigDecimalConstructorUnevenMultipleNegative()
  {
    // test negative when number of bytes in BigDecimal isn't an even multiple of sizeof(int)
    BigDecimal bd = new BigDecimal(new BigInteger(-1, new byte[] {Byte.MIN_VALUE, 0, 0, 0, 0}));
    ArrayCompressedBigDecimal d = new ArrayCompressedBigDecimal(bd);
    assertEquals(0, d.getScale());
    int[] array = d.getArray();
    assertEquals(2, array.length);
    assertEquals(0, array[0]);
    assertEquals(Byte.MIN_VALUE, array[1]);
  }

  /**
   * Test method for {@link ArrayCompressedBigDecimal#ArrayCompressedBigDecimal(CompressedBigDecimal)}.
   */
  @Test
  public void testCopyConstructor()
  {
    BigDecimal bd = new BigDecimal(new BigInteger(1, new byte[] {0x7f, -1, -1, -1, -1}));
    ArrayCompressedBigDecimal d = new ArrayCompressedBigDecimal(bd);

    ArrayCompressedBigDecimal d2 = new ArrayCompressedBigDecimal(d);
    assertEquals(d.getScale(), d2.getScale());
    assertArrayEquals(d.getArray(), d2.getArray());
    assertNotSame(d.getArray(), d2.getArray());
  }

  /**
   * Test method for {@link ArrayCompressedBigDecimal#wrap(int[], int)}.
   */
  @Test
  public void testWrap()
  {
    int[] array = new int[] {Integer.MAX_VALUE, -1};
    ArrayCompressedBigDecimal bd = ArrayCompressedBigDecimal.wrap(array, 0);
    assertSame(array, bd.getArray());
    assertEquals(0, bd.getScale());
  }

  /**
   * Test method for {@link ArrayCompressedBigDecimal#allocate(int, int)}.
   */
  @Test
  public void testAllocate()
  {
    ArrayCompressedBigDecimal bd = ArrayCompressedBigDecimal.allocate(2, 5);
    assertEquals(5, bd.getScale());
    assertEquals(2, bd.getArray().length);
  }

  /**
   * Test method for {@link ArrayCompressedBigDecimal#accumulate(CompressedBigDecimal)}.
   */
  @Test
  public void testSimpleAccumulate()
  {
    ArrayCompressedBigDecimal bd = ArrayCompressedBigDecimal.allocate(2, 0);

    ArrayCompressedBigDecimal add = ArrayCompressedBigDecimal.wrap(new int[] {0x00000001, 0}, 0);
    bd.accumulate(add);
    assertArrayEquals(new int[] {1, 0}, bd.getArray());
    bd.accumulate(add);
    assertArrayEquals(new int[] {2, 0}, bd.getArray());
  }

  /**
   * Test method for {@link ArrayCompressedBigDecimal#accumulate(CompressedBigDecimal)}.
   */
  @Test
  public void testSimpleAccumulateOverflow()
  {
    ArrayCompressedBigDecimal bd = ArrayCompressedBigDecimal.wrap(new int[] {0x80000000, 0}, 0);
    ArrayCompressedBigDecimal add = ArrayCompressedBigDecimal.wrap(new int[] {0x7fffffff, 0}, 0);
    ArrayCompressedBigDecimal add1 = ArrayCompressedBigDecimal.wrap(new int[] {0x00000001, 0}, 0);
    bd.accumulate(add);
    assertArrayEquals(new int[] {0xffffffff, 0}, bd.getArray());
    bd.accumulate(add1);
    assertArrayEquals(new int[] {0, 1}, bd.getArray());
  }

  /**
   * Test method for {@link ArrayCompressedBigDecimal#accumulate(CompressedBigDecimal)}.
   */
  @Test
  public void testSimpleAccumulateUnderflow()
  {
    ArrayCompressedBigDecimal bd = ArrayCompressedBigDecimal.wrap(new int[] {0, 1}, 0);

    ArrayCompressedBigDecimal add = ArrayCompressedBigDecimal.wrap(new int[] {-1, -1}, 0);

    bd.accumulate(add);
    assertArrayEquals(new int[] {0xffffffff, 0}, bd.getArray());
  }

  /**
   * Test method for {@link ArrayCompressedBigDecimal#accumulate(CompressedBigDecimal)}.
   */
  @Test
  public void testUnevenAccumulateUnderflow()
  {
    ArrayCompressedBigDecimal bd = ArrayCompressedBigDecimal.wrap(new int[] {0, 1}, 0);

    ArrayCompressedBigDecimal add = ArrayCompressedBigDecimal.wrap(new int[] {-1}, 0);

    bd.accumulate(add);
    assertArrayEquals(new int[] {0xffffffff, 0}, bd.getArray());
  }

  /**
   * Test method for {@link ArrayCompressedBigDecimal#accumulate(CompressedBigDecimal)}.
   */
  @Test
  public void testUnevenAccumulateOverflow()
  {
    ArrayCompressedBigDecimal bd = ArrayCompressedBigDecimal.wrap(new int[] {0xffffffff, 1}, 0);

    ArrayCompressedBigDecimal add = ArrayCompressedBigDecimal.wrap(new int[] {1}, 0);

    bd.accumulate(add);
    assertArrayEquals(new int[] {0, 2}, bd.getArray());
  }

  /**
   * Test method for {@link ArrayCompressedBigDecimal#accumulate(CompressedBigDecimal)}.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testUnevenAccumulateOverflowWithTruncate()
  {
    ArrayCompressedBigDecimal bd = ArrayCompressedBigDecimal.wrap(new int[] {Integer.MAX_VALUE}, 0);

    ArrayCompressedBigDecimal add = ArrayCompressedBigDecimal.wrap(new int[] {1, 1}, 0);

    bd.accumulate(add);
  }

  /**
   * Test method for {@link ArrayCompressedBigDecimal#accumulate(CompressedBigDecimal)}.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testAccumulateScaleMismatch()
  {
    ArrayCompressedBigDecimal bd = ArrayCompressedBigDecimal.allocate(2, 1);
    ArrayCompressedBigDecimal add = new ArrayCompressedBigDecimal(1, 0);
    bd.accumulate(add);
  }

  /**
   * Test method for {@link ArrayCompressedBigDecimal#toBigDecimal()}.
   */
  @Test
  public void testToBigDecimal()
  {
    ArrayCompressedBigDecimal bd = ArrayCompressedBigDecimal.wrap(new int[] {1}, 0);
    assertEquals(BigDecimal.ONE, bd.toBigDecimal());

    bd = ArrayCompressedBigDecimal.wrap(new int[] {Integer.MAX_VALUE}, 0);
    assertEquals(new BigDecimal(Integer.MAX_VALUE), bd.toBigDecimal());

    bd = ArrayCompressedBigDecimal.wrap(new int[] {0}, 0);
    assertEquals(BigDecimal.ZERO, bd.toBigDecimal());
    bd = ArrayCompressedBigDecimal.wrap(new int[] {0, 0}, 0);
    assertEquals(BigDecimal.ZERO, bd.toBigDecimal());
    bd = new ArrayCompressedBigDecimal(-1, 9);
    assertEquals(new BigDecimal(-1).scaleByPowerOfTen(-9), bd.toBigDecimal());
    bd = ArrayCompressedBigDecimal.wrap(new int[] {1410065408, 2}, 9);
    assertEquals(new BigDecimal(10).setScale(9), bd.toBigDecimal());
  }
}
