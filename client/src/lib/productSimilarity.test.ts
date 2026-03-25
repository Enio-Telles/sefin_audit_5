import { describe, it, expect } from "vitest";
import { normalizeSimilarityTokens, buildSimilarityCharNgrams } from "./productSimilarity";

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
    expect(normalizeSimilarityTokens("item, with: punctuation!")).toEqual(["ITEM", "WITH", "PUNCTUATION"]);
    expect(normalizeSimilarityTokens("and/or symbols#")).toEqual(["AND", "OR", "SYMBOLS"]);
    expect(normalizeSimilarityTokens("100% discount")).toEqual(["100", "DISCOUNT"]);
  });

  it("should handle multiple spaces correctly", () => {
    expect(normalizeSimilarityTokens("  multiple   spaces  ")).toEqual(["MULTIPLE", "SPACES"]);
  });

  it("should filter out tokens with length 1", () => {
    expect(normalizeSimilarityTokens("a b c d e product f g")).toEqual(["PRODUCT"]);
    expect(normalizeSimilarityTokens("1 2 3 45")).toEqual(["45"]);
  });

  it("should process strings containing numbers correctly", () => {
    // 2.0 has its punctuation removed. "2 0" becomes "2" and "0", which are filtered out because length <= 1
    expect(normalizeSimilarityTokens("version 2.0")).toEqual(["VERSION"]);
    // 123 is kept because length > 1
    expect(normalizeSimilarityTokens("model 123")).toEqual(["MODEL", "123"]);
  });

  it("should process a complex string correctly", () => {
    expect(normalizeSimilarityTokens("  SABÃO em PÓ - OMO 1kg  (Lava-roupas)!! ")).toEqual([
      "SABAO",
      "EM",
      "PO",
      "OMO",
      "1KG",
      "LAVA",
      "ROUPAS"
    ]);
  });
});

describe("buildSimilarityCharNgrams", () => {
  it("should return an empty set for an empty string or whitespace-only strings", () => {
    expect(buildSimilarityCharNgrams("")).toEqual(new Set());
    expect(buildSimilarityCharNgrams("   ")).toEqual(new Set());
  });

  it("should return the original string if length is less than or equal to size", () => {
    expect(buildSimilarityCharNgrams("AB", 3)).toEqual(new Set(["AB"]));
    expect(buildSimilarityCharNgrams("ABC", 3)).toEqual(new Set(["ABC"]));
  });

  it("should generate correct n-grams for strings longer than size", () => {
    // "ABCD" -> "ABC", "BCD"
    expect(buildSimilarityCharNgrams("ABCD", 3)).toEqual(new Set(["ABC", "BCD"]));
    // "ABCDE" -> "ABC", "BCD", "CDE"
    expect(buildSimilarityCharNgrams("ABCDE", 3)).toEqual(new Set(["ABC", "BCD", "CDE"]));
  });

  it("should normalize the string before generating n-grams", () => {
    // "Ação" -> "ACAO" -> "ACA", "CAO"
    expect(buildSimilarityCharNgrams("Ação", 3)).toEqual(new Set(["ACA", "CAO"]));
    // "café" -> "CAFE" -> "CAF", "AFE"
    expect(buildSimilarityCharNgrams("café", 3)).toEqual(new Set(["CAF", "AFE"]));
  });

  it("should remove punctuation before generating n-grams and replace with space", () => {
    expect(buildSimilarityCharNgrams("A-B-C", 3)).toEqual(new Set(["A B", "B", "B C"]));
  });

  it("should handle custom size parameter", () => {
    // size 2: "ABCD" -> "AB", "BC", "CD"
    expect(buildSimilarityCharNgrams("ABCD", 2)).toEqual(new Set(["AB", "BC", "CD"]));

    // size 4: "ABCDEF" -> "ABCD", "BCDE", "CDEF"
    expect(buildSimilarityCharNgrams("ABCDEF", 4)).toEqual(new Set(["ABCD", "BCDE", "CDEF"]));
  });

  it("should handle strings with consecutive punctuation replaced by a single space", () => {
    expect(buildSimilarityCharNgrams("A--B", 3)).toEqual(new Set(["A B"]));
  });
});
