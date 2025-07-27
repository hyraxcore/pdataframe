import 'package:matcher/matcher.dart';

Matcher equalsWithNaN(List<double> expected, {double tolerance = 1e-6}) =>
    _EqualsWithNaN(expected, tolerance);

class _EqualsWithNaN extends Matcher {
  final List<double> _expected;
  final double _tolerance;

  _EqualsWithNaN(this._expected, this._tolerance);

  @override
  bool matches(item, Map matchState) {
    if (item is! List<double> || item.length != _expected.length) return false;
    for (int i = 0; i < item.length; i++) {
      final a = item[i];
      final b = _expected[i];
      if (a.isNaN && b.isNaN) continue;
      if ((a - b).abs() > _tolerance) return false;
    }
    return true;
  }

  @override
  Description describe(Description description) =>
      description.add('equals list with NaN-aware comparison: $_expected');
}