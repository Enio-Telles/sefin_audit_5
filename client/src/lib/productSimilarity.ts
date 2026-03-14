export function normalizeSimilarityTokens(value: string): string[] {
  return value
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/[^A-Z0-9 ]+/g, " ")
    .split(/\s+/)
    .map((token) => token.trim())
    .filter((token) => token.length > 1);
}

export function buildSimilarityCharNgrams(value: string, size = 3): Set<string> {
  const compact = value
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, " ");
  const grams = new Set<string>();
  if (compact.length <= size) {
    if (compact.trim()) grams.add(compact.trim());
    return grams;
  }
  for (let i = 0; i <= compact.length - size; i += 1) {
    const slice = compact.slice(i, i + size).trim();
    if (slice) grams.add(slice);
  }
  return grams;
}

export function jaccard<T>(a: Set<T>, b: Set<T>): number {
  if (a.size === 0 && b.size === 0) return 1;
  const aItems = Array.from(a);
  const bItems = Array.from(b);
  const intersection = aItems.filter((item) => b.has(item)).length;
  const union = new Set([...aItems, ...bItems]).size;
  return union === 0 ? 0 : intersection / union;
}

export function similarityScore(a: string, b: string): number {
  const tokenScore = jaccard(new Set(normalizeSimilarityTokens(a)), new Set(normalizeSimilarityTokens(b)));
  const ngramScore = jaccard(buildSimilarityCharNgrams(a), buildSimilarityCharNgrams(b));
  return 0.6 * tokenScore + 0.4 * ngramScore;
}

export function analyzeDescriptions(descriptions: string[]) {
  let maxSimilarity = 0;
  let minSimilarity = 1;
  for (let i = 0; i < descriptions.length; i += 1) {
    for (let j = i + 1; j < descriptions.length; j += 1) {
      const score = similarityScore(descriptions[i], descriptions[j]);
      maxSimilarity = Math.max(maxSimilarity, score);
      minSimilarity = Math.min(minSimilarity, score);
    }
  }
  if (descriptions.length < 2) {
    minSimilarity = 1;
  }
  const bucket =
    maxSimilarity <= 0.2 ? "Muito dissimilares" : maxSimilarity <= 0.4 ? "Mistas" : "Proximas";
  return { maxSimilarity, minSimilarity, bucket };
}
