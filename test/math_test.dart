import 'package:pdataframe/pdataframe.dart';
import 'package:pdataframe/src/math_utils.dart';
import 'package:test/test.dart';
import 'dart:math';
import 'utils/matchers.dart';
void main() {
  group('m() math tests', () {
    late DataFrame df;
    setUp(() {df = DataFrame([1, 4, 7], columns: ['A']);});
    test('Test m() by multiplying first column values by 2, returning a new DataFrame', () {
      var df2 = df.m('A', (e) => e * 2);
      expect(df2['A'], equals([2.0, 8.0, 14.0]));  // The column values doubled and returned as a DataFrame
      expect(df['A'], equals([1, 4, 7]));  // Original DataFrame unchanged
    });
    test('Test m() editing original DataFrame via inplace parameter', () {
      var df2 = df.m('A', (e) => e * 2, inplace: true);
      expect(df['A'], equals([2.0, 8.0, 14.0]));  // The column values doubled and returned as a DataFrame
      expect(df2, isNull); // Check that when inplace is true, function doesn't return a value
    });
    test('Test m() with List return via asList = true', () {
      final result = df.m('A', (e) => e * 2, asList: true);
      expect(result, equals([2.0, 8.0, 14.0]));  // The column values doubled and returned as a list
    });
    test('Test m() with default behavior', () {
      final newDf = df.m('A', (e) => e - 1);
      expect(newDf['A'], equals([0.0, 3.0, 6.0]));  // The column values decremented by 1
      expect(df['A'], equals([1, 4, 7]));  // Original DataFrame should be unchanged
    });
    test('Test m() throws ArgumentError for asList = true and inplace = true', () {
      expect(() => df.m('A', (e) => e * 2, asList: true, inplace: true), throwsArgumentError);
    });
  });

  late DataFrame df1;

  setUp(() {
    df1 = DataFrame({
      'A': [1, 2, null, 4, double.nan, 0],
      'B': [5, 0, 0, 8, 9, 10]
    });
  });
  group('filterNulls tests', () {
    test('filterNulls with skipNull = true (default)', () {
      final result = df1.filterNulls(0);
      expect(result, equals([1.0, 2.0, 4.0, 0.0]));
    });
    test('filterNulls with skipNull = false', () {
      final result = df1.filterNulls(0, skipNull: false);
      expect(result, equals([1.0, 2.0, 0.0, 4.0, 0.0, 0.0]));
    });
  });
  group('countNulls tests', () {
    test('countNulls counts null and NaN', () {
      expect(df1.countNulls('A'), equals(2));
    });
  });
  group('countZeros', () {
    test('countZeros default', () {
      expect(df1.countZeros('A'), equals(1));
    });
    test('countZeros custom', () {
      expect(df1.countZeros('B', zeroValues: [0, 'zero']), equals(2));
    });
  });
  group('sum/mean/max/min', () {
    test('sumCol sums non-null values', () {
      expect(df1.sumCol(0), equals(7.0));
    });
    test('mean computes mean of non-null values', () {
      expect(df1.mean(0), closeTo(7.0 / 4.0, 0.0001));
    });
    test('max gets max value', () {
      expect(df1.max(0), equals(4.0));
    });
    test('min gets min value', () {
      expect(df1.min(0), equals(0.0));
    });
  });
  group('rolling tests', () {
    late DataFrame df;
    setUp(() {
      df = DataFrame({
        'A': [1, 2, 3, 4, 5]
      });
    });
    test('rollingSum with window=3', () {
      final result = df.rollingSum(0, 3);
      expect(result, equalsWithNaN([double.nan,double.nan,6.0,9.0,12.0,]));
    });
    test('rollingMean with window=2', () {
      final result = df.rollingMean(0, 2);
      expect(result, equalsWithNaN([double.nan,1.5,2.5,3.5,4.5,]));
    });
    test('rollingStd with window=2', () {
      final result = df.rollingStd(0, 2);
      expect(
        result.map((v) => v.isNaN ? v : double.parse(v.toStringAsFixed(6))).toList(),
        equalsWithNaN([double.nan,0.5,0.5,0.5,0.5,]),
      );
    });
    test('rollingApply with window=3, max function', () {
      final result = df.rollingApply(0, 3, (List<double> w) => w.reduce(max));
      expect(result, equalsWithNaN([double.nan,double.nan,3.0,4.0,5.0,]));
    });
    test('rollingApply with window=2, custom mean-square function', () {
      double meanSquare(List<double> w) => w.map((x) => x * x).reduce((a, b) => a + b) / w.length;
      final result = df.rollingApply(0, 2, meanSquare);
      expect(
        result.map((v) => v.isNaN ? v : double.parse(v.toStringAsFixed(2))).toList(),
        equalsWithNaN([double.nan,2.5,6.5,12.5,20.5,]),
      );
    });
    test('rollingSum throws on window=0', () {
      expect(() => df.rollingSum(0, 0), throwsArgumentError);
    });
    test('rollingStd throws on negative window', () {
      expect(() => df.rollingStd(0, -1), throwsArgumentError);
    });
    test('rollingApply throws on window=0', () {
      expect(() => df.rollingApply(0, 0, (w) => w.reduce(max)), throwsArgumentError);
    });    
  });
  group('expandingMin', () {
    test('expanding minimum on mixed values', () {
      final df = DataFrame({'A': [3, 1, 4, 2, 5]});
      final res = df.expandingMin(0);
      expect(res, equals([3.0, 1.0, 1.0, 1.0, 1.0])); // min up to each index
    });
  });
  group('expandingMax', () {
    test('expanding maximum on mixed values', () {
      final df = DataFrame({'A': [3, 1, 4, 2, 5]});
      final res = df.expandingMax(0);
      expect(res, equals([3.0, 3.0, 4.0, 4.0, 5.0])); // max up to each index
    });
  });
  group('expandingMean', () {
    test('expanding mean on mixed values', () {
      final df = DataFrame({'A': [3, 1, 4, 2, 5]});
      final res = df.expandingMean(0);
      expect(res,equals([3.0, 2.0, 2.6666666666666665, 2.5, 3.0]),);
    });
  });
  group('expandingVar', () {
    test('expanding variance (ddof=1) on increasing sequence', () {
      final df = DataFrame({'A': [1, 2, 3, 4, 5]});
      final res = df.expandingVar(0);
      expect(res[0].isNaN, isTrue);                      // count=1 → NaN
      expect(res[1], equals(0.5));                       // sample var of [1,2]
      expect(res[2], closeTo(1.0, 1e-9));                // sample var of [1,2,3]
      expect(res[3], closeTo(1.6666666667, 1e-9));       // sample var of [1,2,3,4]
      expect(res[4], equals(2.5));                       // sample var of [1,2,3,4,5]
    });
  });
  group('groupBy', () {
    test('transform: group mean', () {
      final df = DataFrame({
        'g': ['A', 'B', 'A', 'B'],
        'v': [1.0, 2.0, 3.0, 4.0],
      });
      final res = df.groupBy(
        'g',
        valueColName: 'v',
        transform: (List<double> xs) => xs.reduce((a, b) => a + b) / xs.length,
      )['v_transformed'];
      expect(res, equals([2.0, 3.0, 2.0, 3.0])); // A -> mean(1,3)=2, B -> mean(2,4)=3
    });
    test('aggregate: sum per group', () {
      final df = DataFrame({
        'g': ['A', 'B', 'A', 'B'],
        'v': [1.0, 2.0, 3.0, 4.0],
      });
      final out = df.groupBy(
        'g',
        valueColName: 'v',
        aggregate: (List<double> xs) => xs.reduce((a, b) => a + b),
      );
      expect(out['g'], equals(['A', 'B']));
      expect(out['v'], equals([4.0, 6.0])); // sum(1,3)=4, sum(2,4)=6
    });

    test('filter: keep groups with sum>4', () {
      final df = DataFrame({
        'g': ['A', 'B', 'A', 'B'],
        'v': [1.0, 2.0, 3.0, 4.0],
      });
      final out = df.groupBy(
        'g',
        valueColName: 'v',
        filter: (List<double> xs) => xs.reduce((a, b) => a + b) > 4.0,
      );
      expect(out['g'], equals(['B', 'B']));
      expect(out['v'], equals([2.0, 4.0])); // only group B kept
    });
  });
  group('pivotTable', () {
    test('default sum aggregator', () {
      final df = DataFrame({
        'idx': ['A', 'A', 'A', 'B', 'B'],
        'col': ['x', 'x', 'y', 'x', 'y'],
        'val': [1.0, 2.0, 3.0, 4.0, 5.0],
      });
      final out = df.pivotTable(
        indexCol: 'idx',
        columnCol: 'col',
        valueCol: 'val',
      );
      expect(out['idx'], equals(['A', 'B']));
      expect(out['x'], equals([3.0, 4.0]));
      expect(out['y'], equals([3.0, 5.0]));
    });
    test('custom aggregator (mean)', () {
      final df = DataFrame({
        'idx': ['A', 'A', 'B', 'B', 'B'],
        'col': ['x', 'x', 'x', 'y', 'y'],
        'val': [2.0, 4.0, 6.0, 8.0, 10.0],
      });
      final out = df.pivotTable(
        indexCol: 'idx',
        columnCol: 'col',
        valueCol: 'val',
        agg: (xs) => xs.reduce((a, b) => a + b) / xs.length,
      );
      expect(out['idx'], equals(['A', 'B']));
      expect(out['x'], equals([3.0, 6.0])); 
      expect(out['y'], equalsWithNaN([double.nan, 9.0])); 
    });
    test('empty bucket yields NaN', () {
      final df = DataFrame({
        'idx': ['A', 'B'],
        'col': ['x', 'y'],
        'val': [1.0, 2.0],
      });

      final out = df.pivotTable(
        indexCol: 'idx',
        columnCol: 'col',
        valueCol: 'val',
      );

      expect(out['idx'], equals(['A', 'B']));
      expect(out['x'], equalsWithNaN([1.0, double.nan]));    
      expect(out['y'], equalsWithNaN([double.nan, 2.0]));    
    });
  });
  group('melt', () {
    test('melt test', (){
        final df = DataFrame({
        'id': [1, 2],
        'A':  [10, 20],
        'B':  [100, 200],
      });
      final melted = df.melt(
        ['id'],         // Keep “id” as identifier
        ['A', 'B'],     // Pivot columns A and B
        varName:   'variable',
        valueName: 'value',
      );
      expect(melted['id'], equals([1,1,2,2]));
      expect(melted['variable'], equals(['A','B','A','B']));
      expect(melted['value'], equals([10,100,20,200]));
    });
  });
  group('resample', () {
    test('resample 1-hour linear interpolation', () {
      final df = DataFrame(
        { 'value': [10.0, 20.0, 40.0] },
        index: [DateTime.utc(2025, 1, 1, 0, 0),DateTime.utc(2025, 1, 1, 1, 0),DateTime.utc(2025, 1, 1, 3, 0),],
      );
      final out = df.resample(
        'ignoreTimeCol',
        'value',
        Duration(hours: 1),
      );
      expect(       // Verify new DateTime index
        out.index,
        equals([
          DateTime.utc(2025, 1, 1, 0, 0),
          DateTime.utc(2025, 1, 1, 1, 0),
          DateTime.utc(2025, 1, 1, 2, 0),
          DateTime.utc(2025, 1, 1, 3, 0),
        ]),
      );
      expect(out['value'], equals([10.0, 20.0, 30.0, 40.0]));
    });
  });
  group('autocorrelation', () {
    test('autocorrelation simple', () {
      final df = DataFrame({
        'A': [1.0, 2.0, 3.0, 4.0],
      });
      final acf = df.autocorrelation(0, 3);
      expect(acf.map((v) => double.parse(v.toStringAsFixed(6))),equals([1.0, 0.333333, -0.6, -1.8]),);
    });
  });
  group('partialAutocorrelation', () {
    test('PACF for [1.0, 2.0, 3.0, 4.0]', () {
      final df = DataFrame({'A': [1.0, 2.0, 3.0, 4.0],});
      final pacf = df.partialAutocorrelation(0, 3);
      expect(
        pacf.map((v) => double.parse(v.toStringAsFixed(6))),
        equals([1.0, 0.25, -0.386667, -0.312709]),
      );
    });
  });
  group('seasonalDecompose', () {
    test('period=2 on [1,3,1,3,1,3]', () {
      final df = DataFrame({'A': [1.0, 3.0, 1.0, 3.0, 1.0, 3.0],});
      final out = df.seasonalDecompose(0, 2);
      final trend    = out['trend'];   
      final seasonal = out['seasonal'];
      final residual = out['residual'];
      expect(trend[0].isNaN, isTrue);
      expect(trend.sublist(1), equals([2.0, 2.0, 2.0, 2.0, 2.0]));
      expect(
        seasonal.map((v) => v.isNaN ? double.nan : v).toList(),
        equals([-1.0, 1.0, -1.0, 1.0, -1.0, 1.0]),
      );
      expect(residual[0].isNaN, isTrue);
      expect(residual.sublist(1), equals([0.0, 0.0, 0.0, 0.0, 0.0]));
    });
  });
  group('covarianceMatrix', () {
    test('covariance of X and Y', () {
      final df = DataFrame({
        'X': [1.0, 2.0, 3.0],
        'Y': [2.0, 4.0, 6.0],
      });
      final covMat = df.covarianceMatrix(['X', 'Y']);
      expect(
        covMat['X'], 
        equals([0.6666666666666666, 1.3333333333333333])
      );
      expect(
        covMat['Y'], 
        equals([1.3333333333333333, 2.6666666666666665])
      );
    });
  });
  group('pca', () {
    test('PCA on perfectly correlated [1,2,3] vs [1,2,3]', () {
      final df = DataFrame({'X': [1.0, 2.0, 3.0],'Y': [1.0, 2.0, 3.0],});
      final model = df.pca();
      final load = model.loadings; // DataFrame with index ['PC1','PC2'], columns ['X','Y']
      final score = model.scores;  // DataFrame with index [0,1,2], columns ['PC1','PC2']
      // Round loadings to 3 decimals
      expect(load['X'].map((v) => double.parse(v.toStringAsFixed(3))),equals([0.707, 0.707]),);
      expect(load['Y'].map((v) => double.parse(v.toStringAsFixed(3))),equals([0.707, -0.707]),);
      // Scores on PC1 are projections of centered data [(-1,-1),(0,0),(1,1)] onto [0.7071,0.7071]:
      // approx [ -1.41421356, 0.0,  1.41421356 ], and PC2 scores all zero.
      expect(score['PC1'].map((v) => double.parse(v.toStringAsFixed(3))),equals([-1.414, 0.000, 1.414]),);
      expect(score['PC2'].map((v) => double.parse(v.toStringAsFixed(3))),equals([0.000, 0.000, 0.000]),);
    });
    test('collinear 3x2 data (Y = 2·X)', () {
      final df = DataFrame({
        'X': [1.0, 2.0, 3.0],
        'Y': [2.0, 4.0, 6.0], // exactly twice X
      });
      // Centering only (scale = false)
      final model1 = df.pca(cols: ['X', 'Y'], center: true, scale: false);
      final load1 = model1.loadings; // index ['PC1','PC2'], columns ['X','Y']
      final score1 = model1.scores;  // rows 0..2, columns ['PC1','PC2']
      // After centering: Xc = [−1, 0, 1], Yc = [−2, 0, 2].
      // Covariance matrix = [[2/3, 4/3], [4/3, 8/3]], first eigenvector ∝ [1,2] -> normalized [0.447,0.894].
      expect(load1['X'].map((v) => double.parse(v.toStringAsFixed(3))),equals([0.447, 0.894]),);
      expect(load1['Y'].map((v) => double.parse(v.toStringAsFixed(3))),equals([0.894, -0.447]),);

      // Scores on PC1 = projection of centered rows onto [0.447,0.894]:
      // Row 0: (−1,−2)·(0.447,0.894) = (−0.447 −1.788) = −2.235 → ≈ −2.236 (rounded)
      // Row 1: (0, 0) → 0.000
      // Row 2: (1, 2)·(0.447,0.894) = 0.447 + 1.788 = 2.235 → ≈ 2.236
      expect(score1['PC1'].map((v) => double.parse(v.toStringAsFixed(3))),equals([-2.236, 0.000, 2.236]),);
      expect(score1['PC2'].map((v) => double.parse(v.toStringAsFixed(3))),equals([0.000, 0.000, 0.000]),);

      // 2) Now with both centering and scaling
      final model2 = df.pca(cols: ['X', 'Y'], center: true, scale: true);
      final load2 = model2.loadings;
      //final score2 = model2.scores;

      // After centering & scaling: Xs = [−1, 0, 1]/σX  with σX=√(2/3)=0.816, 
      // similarly Ys = [−2,0,2]/σY with σY=√(8/3)=1.633. 
      // Cov matrix in scaled space becomes identity, so first PC ∝ [1, 0] or [0, 1]. 
      // Depending on implementation, loadings should be something like [0.707,0.707] or [0.707, −0.707].
      // We assert orthonormality: |loadings| = 1 and orthogonal to second PC.
      final lX = load2['X'].map((v) => double.parse(v.toStringAsFixed(3))).toList();
      final lY = load2['Y'].map((v) => double.parse(v.toStringAsFixed(3))).toList();
      expect((lX[0] * lX[0] + lY[0] * lY[0]).toStringAsFixed(0), equals('1'));
      expect((lX[1] * lX[1] + lY[1] * lY[1]).toStringAsFixed(0), equals('1'));
      expect((lX[0] * lX[1] + lY[0] * lY[1]).toStringAsFixed(0), equals('0'));
    });
    test('4×2 data, X ascending vs Y descending', () {
      final df = DataFrame({
        'X': [1.0, 2.0, 3.0, 4.0],
        'Y': [4.0, 3.0, 2.0, 1.0], // perfectly negatively correlated
      });
      final model = df.pca();
      final load = model.loadings;
      final score = model.scores;
      // After centering: Xc = [−1.5, −0.5, 0.5, 1.5], Yc = [1.5, 0.5, −0.5, −1.5].
      // Cov = [[1.25, −1.25], [−1.25, 1.25]]; first eigenvector ∝ [1, −1]/√2 ≈ [0.707, −0.707].
      expect(load['X'].map((v) => double.parse(v.toStringAsFixed(3))),equals([0.707, 0.707]),);
      expect(load['Y'].map((v) => double.parse(v.toStringAsFixed(3))),equals([-0.707, 0.707]),);

      // Scores on PC1:
      // Row0: (−1.5, 1.5)·(0.707, −0.707) = −1.0605 −(−1.0605) = −2.121  → −2.121
      // Row1: (−0.5, 0.5)·(0.707, −0.707) = −0.354 −(−0.354) = −0.707  → −0.707
      // Row2: (0.5, −0.5)·(0.707, −0.707) = 0.354 −(0.354) = 0.707     → 0.707
      // Row3: (1.5, −1.5)·(0.707, −0.707) = 1.0605 −(0.707?) = 2.121    → 2.121
      expect(score['PC1'].map((v) => double.parse(v.toStringAsFixed(3))),equals([-2.121, -0.707, 0.707, 2.121]),);
      expect(score['PC2'].map((v) => double.parse(v.toStringAsFixed(3))),equals([0.000, 0.000, 0.000, 0.000]),);
    });
    
  });
  group('svd', () {
    test('2×2 identity matrix', () {
      final df = DataFrame({'A': [1.0, 0.0],'B': [0.0, 1.0],});
      final result = df.svd();
      final U   = result.U;   // DataFrame with columns ['A','B'], index [0,1]
      final S   = result.S;   // List<double> of length 2
      final vt  = result.Vt;  // DataFrame with columns ['V1','V2'], index ['A','B']

      expect(S, equals([1.0, 1.0])); // Singular values should both be 1.0
      expect(U['A'], equals([1.0, 0.0])); // U should equal the identity (columns ['A','B'])
      expect(U['B'], equals([0.0, 1.0]));
      expect(vt['V1'], equals([1.0, 0.0])); // vt should be identity
      expect(vt['V2'], equals([0.0, 1.0]));
    });
    test('3×2 matrix with one zero row', () {
      final df = DataFrame({
        'X': [1.0, 0.0, 0.0],
        'Y': [0.0, 1.0, 0.0],
      });
      final result = df.svd();
      final U   = result.U;   // DataFrame with columns ['X','Y'], index [0,1,2]
      final S   = result.S;   // List<double> of length 2
      final vt  = result.Vt;  // DataFrame with columns ['V1','V2'], index ['X','Y']

      // Singular values should both be 1.0
      expect(S, equals([1.0, 1.0]));
      // U should match the original data matrix
      expect(U['X'], equals([1.0, 0.0, 0.0]));
      expect(U['Y'], equals([0.0, 1.0, 0.0]));
      // Vt should again be identity
      expect(vt['V1'], equals([1.0, 0.0]));
      expect(vt['V2'], equals([0.0, 1.0]));
    });
  });
  group('knnImputer', () {
    test('simple k=2 imputations', () {
      final df = DataFrame({
        'F1': [1.0, 2.0, 3.0, 4.0],
        'F2': [4.0, 3.0, 2.0, 1.0],
        'T1': [10.0, double.nan, 30.0, double.nan],
      });
      final imputed = df.knnImputer(
        featureCols: ['F1', 'F2'],
        targetCols: ['T1'],
        k: 2,
      );
      // For row 1: neighbors with non-missing T1 are row0 and row2 -> (10 + 30)/2 = 20
      // For row 3: neighbors row2 (dist ~1.414) and row0 (dist ~4.243) -> average = (30 + 10)/2 = 20
      expect(imputed['T1'],equals([10.0, 20.0, 30.0, 20.0]),
      );
    });
  });
  group('interpolate', () {
    test('polynomial interpolation (degree=2, precision=2)', () {
      final df = DataFrame({
        'value': [1.0, null, 9.0, 16.0],
      });
      final out = df.interpolate(
        'timeCol_unused',
        'value',
        method: InpMethod.polynomial,
        degree: 2,
        precision: 2,
      );
      expect(out['value'], equals([1.0, 4.0, 9.0, 16.0])); // Quadratic through (0,1), (2,9), (3,16) at x=1 → 4.00
    });

    test('linear interpolation (degree=1)', () {
      final df = DataFrame({
        'value': [1.0, null, 3.0],
      });
      final out = df.interpolate(
        'timeCol_unused',
        'value',
        method: InpMethod.linear,
        degree: 1,
      );
      expect(out['value'], equals([1.0, 2.0, 3.0])); // Line between (0,1) and (2,3) at x=1 → 2.0
    });

    test('time‐based linear interpolation', () {
      final df = DataFrame(
        { 'value': [10.0, null, 30.0] }, // time index in milliseconds
        index: [0, 50, 100],  
      );
      final out = df.interpolate(
        'timeCol', 
        'value',
        method: InpMethod.time,
      );
      expect(out['value'], equals([10.0, 20.0, 30.0])); // Linear in time: (0,10)→(100,30), at t=50 → 20.0
    });

    test('spline interpolation (degree=2, precision=2)', () {
      final df = DataFrame({
        'value': [1.0, null, 9.0, 16.0],
      });
      final out = df.interpolate(
        'timeCol_unused',
        'value',
        method: InpMethod.spline,
        degree: 2,
        precision: 2,
      );
      expect(out['value'], equals([1.0, 4.25, 9.0, 16.0])); // Natural cubic‐spline through (0,1),(2,9),(3,16) at x=1 → 4.25
    });
  });
  group('tTest', () {
    test('two‐sample t-test with equal variances', () {
      final df = DataFrame({ // Group A: [1,2,3] → mean = 2.0, var = 1.0, Group B: [2,3,4] → mean = 3.0, var = 1.0
        'Group': ['A', 'A', 'A', 'B', 'B', 'B'], 
        'Value': [1.0, 2.0, 3.0, 2.0, 3.0, 4.0],
      });
      final result = df.tTest('Group', 'Value');
      expect(result.t, closeTo(-1.224744871, 1e-6));
      expect(result.df, closeTo(4.0, 1e-6));
      expect(result.pValue, closeTo(0.2879, 1e-4));
    });
  });
  group('anova', () {
    test('ANOVA basic two‐group test', () {
      final df = DataFrame({
        'Group': ['A', 'A', 'A', 'B', 'B', 'B'],
        'Value': [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
      });
      final result = df.anova('Group', 'Value');
      expect(result.f, closeTo(13.5, 1e-6));
      expect(result.dfBetween, equals(1));
      expect(result.dfWithin, equals(4));
      expect(result.pValue, closeTo(0.021, 1e-3)); // approx from F(1,4)=13.5
      expect(result.etaSquared, closeTo(0.7714285714, 1e-6));
    });
  });
  group('chiSquare', () {
    test('chi-square uniform 2×2 independence', () {
      final df = DataFrame({
        'X': ['A', 'A', 'B', 'B'],
        'Y': ['Yes', 'No', 'Yes', 'No'],
      });
      final result = df.chiSquare('X', 'Y');
      // Observed and expected tables both [[1,1],[1,1]]
      expect(result.chi2, closeTo(0.0, 1e-9));
      expect(result.dof, equals(1));
      expect(result.pValue, closeTo(1.0, 1e-9));
      expect(result.observed['Yes'], equals([1.0, 1.0]));
      expect(result.observed['No'],  equals([1.0, 1.0]));
      expect(result.expected['Yes'], equals([1.0, 1.0]));
      expect(result.expected['No'],  equals([1.0, 1.0]));
    });
  });
  group('bootstrap', () {
    test('bootstrap mean on constant data returns same value', () {
      final df = DataFrame({
        'X': [5.0, 5.0, 5.0, 5.0],
      });
      // With all values equal, any resample statistic (mean) must equal 5.0
      final result = df.bootstrap(
        valueCol: 'X',
        statistic: (List<double> xs) => xs.reduce((a, b) => a + b) / xs.length,
        nBoot: 10,
      );
      // Expect exactly ten replicates, all equal to 5.0
      expect(result.samples, equals(List.filled(10, 5.0)));
    });
  });
  group('convolve', () {
    test('equal-length sequences convolution', () {
      final df = DataFrame({'x': [1, 2, 3],'y': [1, 2, 3],});
      final out = df.convolve('x', 'y');
      expect(out['conv'], equals([1.0, 4.0, 10.0, 12.0, 9.0]));
    });
  });
  group('crossCorrelate', () {
    test('cross‐correlation for [1,2,3] and [4,5,6]', () {
      final df = DataFrame({'x': [1, 2, 3],'y': [4, 5, 6],});
      final out = df.crossCorrelate('x', 'y');
      expect(out.index, equals([-2, -1, 0, 1, 2]));
      expect(out['xcorr'], equals([12.0, 23.0, 32.0, 17.0, 6.0]));
    });
  });
  group('fft', () {
    test('DFT of two-point constant sequence with tolerance', () {
      final df = DataFrame(
        { 'value': [1, 1] },
        index: [
          DateTime.utc(2025, 1, 1, 0, 0, 0),
          DateTime.utc(2025, 1, 1, 0, 0, 1),
        ],
      );

      final out = df.fft('time', 'value');

      // Frequencies should be exact
      expect(out.index, equals([0.0, 0.5]));

      // Real parts: [2.0, 0.0] within floating tolerance
      expect(out['real'][0], closeTo(2.0, 1e-12));
      expect(out['real'][1], closeTo(0.0, 1e-12));

      // Imag parts: [0.0, ~0.0] (second entry may be a tiny negative epsilon)
      expect(out['imag'][0], closeTo(0.0, 1e-12));
      expect(out['imag'][1], closeTo(0.0, 1e-12));
    });
  });
  group('outlierIQR', () {
    test('no outliers with default k=1.5', () { // With k=1.5, IQR = 52 - 1.5 = 50.5, bounds [-74.25, 127.75], no flags
      final df = DataFrame({'value': [1, 2, 3, 4, 100],});
      final out = df.outlierIQR('value'); 
      expect(out['value_outlierIQR'],equals([false, false, false, false, false]),);
    });
    test('flags extreme point with k=0.5', () { // With k=0.5, bounds ≈[-23.75, 77.25], only 100 is outlier
      final df = DataFrame({'value': [1, 2, 3, 4, 100],});
      final out = df.outlierIQR('value', k: 0.5); 
      expect(out['value_outlierIQR'],equals([false, false, false, false, true]),);
    });
  });
  group('outlierZScore', () {
    test('no outliers with default threshold=3.0', () {       // mean=2, std=4, z for 10 -> (10−2)/4=2 < 3 -> no flags
      final df = DataFrame({'value': [0, 0, 0, 0, 10],});
      final out = df.outlierZScore('value');
      expect(out['value_outlierZ'],equals([false, false, false, false, false]),);
    });
    test('flags extreme value with threshold=1.5', () { // mean=2, std=4, z for 10 -> 2 > 1.5 -> only last is outlier
      final df = DataFrame({'value': [0, 0, 0, 0, 10],});
      final out = df.outlierZScore('value', threshold: 1.5);
      expect(out['value_outlierZ'],equals([false, false, false, false, true]),);
    });
  });
  group('rollingMad', () {
    test('rolling MAD window=3 on increasing sequence', () {
      final df = DataFrame({'value': [1, 2, 3, 4, 5],});
      final out = df.rollingMad('value', 3);
      final res = out['value_rollingMAD'];
      expect(res[0].isNaN, isTrue); // First two entries should be NaN
      expect(res[1].isNaN, isTrue);
      expect(res[2], equals(1.0)); // For window=3, each median absolute deviation is 1.0
      expect(res[3], equals(1.0));
      expect(res[4], equals(1.0));
    });
  });
  group('localOutlierFactor', () {
    test('uniform spacing yields LOF ≈ correct k=2 values', () {
      final df = DataFrame({'X': [1, 2, 3, 4, 5],});
      final lof = df.localOutlierFactor(cols: ['X'], k: 2)['lof'];
      final rounded = lof.map((v) => double.parse(v.toStringAsFixed(3))).toList();   // Round to 3 dp
      expect(rounded, equals([1.306, 1.044, 0.833, 1.044, 1.306]));
    });
    test('single extreme outlier has LOF > 1', () {
      final df = DataFrame({'X': [1, 2, 3, 100],});
      final lof = df.localOutlierFactor(cols: ['X'], k: 2)['lof'];
      expect(lof[0], closeTo(1.0, 1e-6)); // The first three are in a tight cluster -> LOF about 1
      expect(lof[1], closeTo(1.0, 1e-6));
      expect(lof[2], closeTo(1.0, 1e-6));
      expect(lof[3], greaterThan(1.0)); // The value 100 is far away -> LOF > 1
    });
  });
  group('weightedHistogram', () {
    test('explicit bins override', () {
      final df = DataFrame({'value': [1, 2, 3, 4],'weight': [1, 2, 3, 4],});
      final out = df.weightedHistogram('value', 'weight', bins: 3);
      expect(out['binStart'], equals([1.0, 2.0, 3.0]));
      expect(out['binEnd'],   equals([2.0, 3.0, 4.0]));
      expect(out['weight'],   equals([1.0, 2.0, 7.0]));
    });
  });  
  group('exponentialSmoothing', () {
    test('SES with α=0.5', () {
      final df = DataFrame({'v': [1.0, 2.0, 3.0]});
      final fitted = df.exponentialSmoothing('v', 1, method: 'ses', alpha: 0.5)['fitted'];
      expect(fitted, equals([1.000, 1.500, 2.250]));
    });

    test('Holt’s Linear Trend with α=0.5, β=0.5', () {
      final df = DataFrame({'v': [3.0, 5.0, 9.0, 12.0]});
      final out = df.exponentialSmoothing('v', 1, method: 'holt', alpha: 0.5, beta: 0.5);
      expect(out['level'],  equals([3.000, 5.000, 8.000, 11.250]));
      expect(out['trend'],  equals([2.000, 2.000, 2.500, 2.875]));
      expect(out['fitted'], equals([3.000, 7.000, 10.500, 14.125]));
    });

    test('Holt-Winters additive, period=2, α=0.5, β=0.5, γ=0.5', () {
      final df = DataFrame({'v': [10.0, 12.0, 11.0, 13.0]});
      final out = df.exponentialSmoothing('v', 2,method: 'hw',alpha: 0.5,beta: 0.5,gamma: 0.5,seasonalType: 'additive',);
      final level    = out['level'];
      final trend    = out['trend'];
      final seasonal = out['seasonal'].map((v) => double.parse(v.toStringAsFixed(3))).toList();
      final fitted   = out['fitted'].map((v) => double.parse(v.toStringAsFixed(3))).toList();
      expect(level[0].isNaN, isTrue);
      expect(level.sublist(1).map((v) => double.parse(v.toStringAsFixed(3))).toList(),equals([11.000, 11.750, 12.188]));
      expect(trend[0].isNaN, isTrue);
      expect(trend.sublist(1).map((v) => double.parse(v.toStringAsFixed(3))).toList(),equals([0.500, 0.625, 0.531]));
      expect(seasonal, equals([-1.000, 1.000, -0.750, 0.813]));
      expect(fitted,   equals([10.000, 12.000, 11.375, 13.719]));
    });
  });
  
  // * math_utils methods
  group('weightedQuantile standalone', () {
    test('uniform weights quantiles', () {
      final values = [1.0, 2.0, 3.0, 4.0];
      final weights = [1.0, 1.0, 1.0, 1.0];
      final result = weightedQuantile(values, weights, [0, 0.25, 0.5, 0.75, 1]);
      expect(result, equals([1.0, 1.0, 2.0, 3.0, 4.0]));
    });
    test('nonuniform weights median only', () {
      final values = [1.0, 2.0, 3.0];
      final weights = [1.0, 3.0, 1.0];
      final result = weightedQuantile(values, weights, [0.5]);
      expect(result, equals([2.0]));
    });
  });
}