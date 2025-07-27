// * Statistics

/// Computes weighted quantiles for a numeric column.
///
/// - [valueCol] - name of the value column (must contain numeric values).
/// - [weightCol] - name of the weight column (must contain positive weights).
/// - [quantiles] - list of quantiles to compute (e.g., [0.25, 0.5, 0.75]).
///
/// Returns a list of weighted quantiles in the same order as [quantiles].
List<double> weightedQuantile(
  List values,
  List weights,
  List quantiles,
) {
  assert(values.length == weights.length, 'values and weights must have equal length');
  // Build (value, weight) pairs, skipping non-positive weights
  final pairs = <MapEntry<double, double>>[];
  for (var i = 0; i < values.length; i++) {
    final w = weights[i].toDouble();
    if (w > 0) {
      pairs.add(MapEntry(values[i].toDouble(), w));
    }
  }
  if (pairs.isEmpty) {
    // No positive weight -> undefined quantiles
    return List<double>.filled(quantiles.length, double.nan);
  }
  // Sort by data value
  pairs.sort((a, b) => a.key.compareTo(b.key));
  // Compute total weight
  final totalW = pairs.fold<double>(0.0, (sum, entry) => sum + entry.value);
  // Compute each quantile
  return quantiles.map((q) {
    final target = q * totalW;
    var c = 0.0;
    for (var entry in pairs) {
      c += entry.value;
      if (c >= target) {
        return entry.key;
      }
    }
    // Fallback (shouldn't happen unless rounding): return max value
    return pairs.last.key;
  }).toList();
}