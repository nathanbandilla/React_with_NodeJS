function findCombinationsFromText(input) {
  const ORDER = [
    "Group",
    "Category",
    "Subcategory",
    "Make",
    "Model",
    "Diagram"
  ];

  const orderIndex = Object.fromEntries(
    ORDER.map((k, i) => [k, i])
  );

  const VALID_PREFIXES = new Set(ORDER);

  // Normalize separators → split into tokens
  const tokens = input
    .split(/[^A-Za-z0-9_-]+/) // split by anything invalid
    .filter(Boolean);

  if (tokens.length === 0) return [];

  const tagMap = new Map();

  for (const token of tokens) {
    // Must match prefix_format exactly
    const match = token.match(/^([A-Za-z]+)_(.+)$/);

    if (!match) continue; // ignore pure noise like "--"

    const [, prefix, value] = match;

    // Invalid prefix → whole input invalid
    if (!VALID_PREFIXES.has(prefix)) {
      return [];
    }

    //  Invalid value (must be alphanumeric + hyphen only)
    if (!/^[A-Za-z0-9-]+$/.test(value)) {
      return [];
    }

    //  Duplicate prefix → invalid
    if (tagMap.has(prefix)) {
      return [];
    }

    tagMap.set(prefix, `${prefix}_${value}`);
  }

  // No valid tags at all
  if (tagMap.size === 0) return [];

  // Sort by hierarchy
  const sorted = Array.from(tagMap.values()).sort((a, b) => {
    return (
      orderIndex[a.split("_")[0]] -
      orderIndex[b.split("_")[0]]
    );
  });

  // Build forward-only chain
  const chain = [];
  let lastIdx = null;

  for (const tag of sorted) {
    const type = tag.split("_")[0];
    const idx = orderIndex[type];

    if (lastIdx === null) {
      chain.push(tag);
      lastIdx = idx;
    } else if (idx > lastIdx) {
      chain.push(tag);
      lastIdx = idx;
    }
    // ignore backward (invalid order)
  }

  // If chain is empty (shouldn’t happen, but safe guard)
  if (chain.length === 0) return [];

  // Generate combinations
  const result = [];
  for (let i = chain.length; i > 0; i--) {
    result.push(chain.slice(0, i));
  }

  return result;
  //console.log(result);
}


// Commented for testing purposes only
// findCombinationsFromText(
//   "Group_Tools-Hardware-Category_Roll-Pin-Make_Atlas-WrongPrefix_Test"
// );

// findCombinationsFromText(
//   "Group_A,Category_B--Subcategory_C@@Make_D"
// );

export default findCombinationsFromText;