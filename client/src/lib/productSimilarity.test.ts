import { describe, it, expect } from "vitest";
import { normalizeSimilarityTokens } from "./productSimilarity";

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
