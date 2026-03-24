import { describe, it, expect } from 'vitest';
import { jaccard } from './productSimilarity';

describe('jaccard similarity', () => {
  it('should return 1 when both sets are empty', () => {
    expect(jaccard(new Set(), new Set())).toBe(1);
  });

  it('should return 0 when one set is empty and the other is not', () => {
    expect(jaccard(new Set([1, 2]), new Set())).toBe(0);
    expect(jaccard(new Set(), new Set([1, 2]))).toBe(0);
  });

  it('should return 1 for identical sets', () => {
    expect(jaccard(new Set(['a', 'b', 'c']), new Set(['a', 'b', 'c']))).toBe(1);
    expect(jaccard(new Set([1, 2, 3]), new Set([1, 2, 3]))).toBe(1);
  });

  it('should return 0 for disjoint sets', () => {
    expect(jaccard(new Set(['a', 'b']), new Set(['c', 'd']))).toBe(0);
    expect(jaccard(new Set([1, 2]), new Set([3, 4]))).toBe(0);
  });

  it('should correctly calculate partial overlap', () => {
    // a: {1, 2, 3}, b: {2, 3, 4}
    // intersection: {2, 3} (size 2)
    // union: {1, 2, 3, 4} (size 4)
    // jaccard: 2 / 4 = 0.5
    expect(jaccard(new Set([1, 2, 3]), new Set([2, 3, 4]))).toBe(0.5);

    // a: {'x', 'y'}, b: {'y', 'z', 'w'}
    // intersection: {'y'} (size 1)
    // union: {'x', 'y', 'z', 'w'} (size 4)
    // jaccard: 1 / 4 = 0.25
    expect(jaccard(new Set(['x', 'y']), new Set(['y', 'z', 'w']))).toBe(0.25);
  });

  it('should handle different types in sets', () => {
    const setA = new Set<string | number>(['1', 2, 'three']);
    const setB = new Set<string | number>(['1', 2, 'four']);
    // intersection: {'1', 2} (size 2)
    // union: {'1', 2, 'three', 'four'} (size 4)
    // jaccard: 2 / 4 = 0.5
    expect(jaccard(setA, setB)).toBe(0.5);
  });
});
