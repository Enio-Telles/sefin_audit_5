import { describe, it, expect } from "vitest";
import {
  normalizeSimilarityTokens,
  similarityScore,
} from "./productSimilarity";

describe("normalizeSimilarityTokens", () => {
  it("should handle empty or whitespace-only strings", () => {
    expect(normalizeSimilarityTokens("")).toEqual([]);
    expect(normalizeSimilarityTokens("   ")).toEqual([]);
  });

  it("should convert to uppercase", () => {
    expect(normalizeSimilarityTokens("product")).toEqual(["PRODUCT"]);
    expect(normalizeSimilarityTokens("PrOdUcT")).toEqual(["PRODUCT"]);
  });

  it("should remove diacritics", () => {
    expect(normalizeSimilarityTokens("Ação")).toEqual(["ACAO"]);
    expect(normalizeSimilarityTokens("café")).toEqual(["CAFE"]);
    expect(normalizeSimilarityTokens("João")).toEqual(["JOAO"]);
    expect(normalizeSimilarityTokens("pão")).toEqual(["PAO"]);
  });

  it("should replace punctuation and special characters with spaces", () => {
    expect(normalizeSimilarityTokens("item, with: punctuation!")).toEqual([
      "ITEM",
      "WITH",
      "PUNCTUATION",
    ]);
    expect(normalizeSimilarityTokens("and/or symbols#")).toEqual([
      "AND",
      "OR",
      "SYMBOLS",
    ]);
    expect(normalizeSimilarityTokens("100% discount")).toEqual([
      "100",
      "DISCOUNT",
    ]);
  });

  it("should handle multiple spaces correctly", () => {
    expect(normalizeSimilarityTokens("  multiple   spaces  ")).toEqual([
      "MULTIPLE",
      "SPACES",
    ]);
  });

  it("should filter out tokens with length 1", () => {
    expect(normalizeSimilarityTokens("a b c d e product f g")).toEqual([
      "PRODUCT",
    ]);
    expect(normalizeSimilarityTokens("1 2 3 45")).toEqual(["45"]);
  });

  it("should process strings containing numbers correctly", () => {
    // 2.0 has its punctuation removed. "2 0" becomes "2" and "0", which are filtered out because length <= 1
    expect(normalizeSimilarityTokens("version 2.0")).toEqual(["VERSION"]);
    // 123 is kept because length > 1
    expect(normalizeSimilarityTokens("model 123")).toEqual(["MODEL", "123"]);
  });

  it("should process a complex string correctly", () => {
    expect(
      normalizeSimilarityTokens("  SABÃO em PÓ - OMO 1kg  (Lava-roupas)!! ")
    ).toEqual(["SABAO", "EM", "PO", "OMO", "1KG", "LAVA", "ROUPAS"]);
  });
});

describe("similarityScore", () => {
  it("should return 1 for identical strings", () => {
    expect(similarityScore("SABÃO EM PÓ OMO", "SABÃO EM PÓ OMO")).toBe(1);
  });

  it("should return 0 for completely different strings", () => {
    expect(similarityScore("SABÃO", "DETERGENTE")).toBe(0);
  });

  it("should return a high score for strings with similar tokens but different order", () => {
    const score = similarityScore("SABÃO EM PÓ OMO", "OMO SABÃO EM PÓ");
    expect(score).toBeGreaterThan(0.8);
    expect(score).toBeLessThanOrEqual(1);
  });

  it("should return a score between 0 and 1 for partial matches", () => {
    const score = similarityScore("SABÃO EM PÓ OMO 1KG", "SABÃO EM PÓ OMO 2KG");
    expect(score).toBeGreaterThan(0);
    expect(score).toBeLessThan(1);
  });

  it("should return 1 for empty strings", () => {
    expect(similarityScore("", "")).toBe(1);
  });

  it("should return 0 for one empty and one non-empty string", () => {
    expect(similarityScore("SABÃO", "")).toBe(0);
  });
});
