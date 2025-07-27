part of 'dataframe.dart';

extension DataFrameMath on DataFrame{

  // * Group-wise transforms and aggregations

  /// GroupBy with transform, filter, or aggregate.
  ///
  /// - Parameters:
  ///   - [byColName] - column used for grouping.
  ///   - [valueColName] - column to apply transformation or aggregation (required if using [transform] or [aggregate]).
  ///   - [transform] - returns one value per group, broadcast to original group rows.
  ///   - [aggregate] - returns one value per group, one row per group in output.
  ///   - [filter] - keeps groups that pass the predicate.
  DataFrame groupBy(
    String byColName, {
    String? valueColName,
    double Function(List<double>)? transform,
    bool Function(List<double>)? filter,
    double Function(List<double>)? aggregate,
  }) {
    if ((transform != null || aggregate != null) && valueColName == null) {
    throw ArgumentError(
       'valueColName must be provided when using transform or aggregate'
     );
    }
    // extract group keys and values
    final keys = this[byColName];
    final values = valueColName != null
        ? filterNulls(_colIndex(valueColName))
        : <double>[];
    // build groups of row‐indices
    final Map<Object, List<int>> idxs = {};
    for (var i = 0; i < keys.length; i++) {
      idxs.putIfAbsent(keys[i]!, () => []).add(i);
    }
    // transform → one output per original row
    if (transform != null) {
      final out = List<double>.filled(keys.length, double.nan);
      idxs.forEach((k, list) {
        final t = transform(list.map((i) => values[i]).toList());
        for (var i in list) out[i] = t;
      });
      // build one row-per-input as single-column "transformed" DataFrame
      final rows = out.map((v) => [v]).toList();
      return DataFrame(
       rows,
      columns: ['${valueColName!}_transformed'],
      index: this.index,  // preserve original row labels
     );
    }
    // aggregate → one row per group
    else if (aggregate != null) {
      final rows = <List<Object?>>[];
      idxs.forEach((k, list) {
        final agg = aggregate(list.map((i) => values[i]).toList());
        rows.add([k, agg]);
      });
      return DataFrame(rows, columns: [byColName, valueColName!]);
    }
    // filter → keep only rows in groups passing the test
    else if (filter != null) {
      final keep = <int>[];
      idxs.forEach((k, list) {
        if (filter(list.map((i) => values[i]).toList())) {
          keep.addAll(list);
        }
      });
      // select by a list of row‐indices
      return iloc(row: keep);
    }
    else {
      throw ArgumentError('Must supply transform, filter, or aggregate');
    }
  }

  /// Dynamic pivot table for reshaping data.
  ///
  /// - Parameters:
  ///   - [indexCol] - column whose values form the table’s row index.
  ///   - [columnCol] - column whose values become the new columns.
  ///   - [valueCol] - column of values to aggregate.
  ///   - [agg] - aggregation function for each (row, column) group; defaults to sum.
  DataFrame pivotTable({
    required String indexCol,
    required String columnCol,
    required String valueCol,
    double Function(List<double>)? agg,
  }) {
    // use the provided agg or default to simple sum
    final aggregator = agg ?? (List<double> xs) => xs.reduce((a, b) => a + b);

    final idxs = this[indexCol];
    final cols = this[columnCol];
    final vals = filterNulls(_colIndex(valueCol));

    final uniqIdx  = idxs.toSet().toList()..sort();
    final uniqCols = cols.toSet().toList()..sort();
    final table    = <List<Object?>>[];

    for (var iKey in uniqIdx) {
      final row = <Object?>[iKey];
      for (var jKey in uniqCols) {
        final bucket = <double>[];
        for (var i = 0; i < idxs.length; i++) {
          if (idxs[i] == iKey && cols[i] == jKey) {
            bucket.add(vals[i]);
          }
        }
        row.add(bucket.isEmpty ? double.nan : aggregator(bucket));
      }
      table.add(row);
    }

    return DataFrame(table, columns: [indexCol, ...uniqCols.cast<String>()]);
  }
  /// Converts wide-form data to long-form.
  ///
  /// - Parameters:
  ///   - [idVars] - columns to keep as identifier variables (not unpivoted).
  ///   - [valueVars] - columns to unpivot into long-form.
  ///   - [varName] - name of the new column holding former column names (default: `'variable'`).
  ///   - [valueName] - name of the new column holding values (default: `'value'`).
  DataFrame melt(
    List<String> idVars,
    List<String> valueVars, {
    String varName = 'variable',
    String valueName = 'value',
  }) {
    if (idVars.isEmpty) {
      throw ArgumentError('melt(): idVars must not be empty');
    }
    final longRows = <List<Object?>>[];
    // Use the length of any id‐column for row count
    final n = this[idVars.first].length;
    for (var i = 0; i < n; i++) {
      // grab all id‐values for row i
      final base = idVars.map((c) => this[c][i]).toList();
      for (var v in valueVars) {
        // build one long form row: [id1, id2…, varName, valueName]
        final row = List<Object?>.from(base)
          ..add(v)
          ..add(this[v][i]);
        longRows.add(row);
      }
    }
    return DataFrame(longRows, columns: [...idVars, varName, valueName]);
  }

  /// Converts long-form data back to wide-form (unstack).
  ///
  /// - Parameters:
  ///   - [indexCol] - column to use as row index in the wide table.
  ///   - [varName] - column whose values become new column headers.
  ///   - [valueName] - column holding values to populate the wide table.
  DataFrame unstack({
    required String indexCol,
    required String varName,
    required String valueName,
  }) {
    final idxs = this[indexCol];
    final vars = this[varName];
    final vals = this[valueName];
    final uniqIdx = idxs.toSet().toList();
    final uniqVars = vars.toSet().toList();
    final wide = <List<Object?>>[];

    for (var key in uniqIdx) {
      final row = <Object?>[key];
      // for each possible varName value, find the corresponding cell
      for (var v in uniqVars) {
        Object? cell;
        for (var j = 0; j < idxs.length; j++) {
          if (idxs[j] == key && vars[j] == v) {
            cell = vals[j];
            break;
          }
        }
        row.add(cell);
      }
      wide.add(row);
    }
    return DataFrame(wide, columns: [indexCol, ...uniqVars.cast<String>()]);
  }

  // ** Mathematical Operations and Data Cleaning **
  
  /// Applies a mathematical function to all elements in a column.
  ///
  /// - Parameters:
  ///   - colName: The name of the column to which the function is applied.
  ///   - operation: The math function that is applied to all the elements of the column.
  ///   - asList: (Optional, default false) If true, returns the calculations as a List.
  ///   - inplace: (Optional, default false) If true, applies the math function to the current DataFrame. If false, applies the function on a returned copy.
  ///
  /// - Example:
  ///   ```dart
  ///   var df2 = df.m(0, (e)=>sqrt(e)); // Square roots each element in the column (function from dart:math).
  ///   var df2 = df.m('ColumnName', (x) => x + 2); // Adds 2 to each element in the column.
  ///   ```
  m(var colName, double Function(num) operation, {bool asList = false, bool inplace = false}) {
    if(asList == true && inplace == true){
      throw ArgumentError('asList and inplace parameters cannot both be true');
    }
    List<num> list = (this[colName] as List).cast<num>();
    List<double> newList = <double>[];
    for (num e in list) {
      newList.add(operation(e));
    }
    if(asList){
      return newList;
    }
    if(inplace){
      this[colName] = newList;
      return;
    } else {
      var newdf = DataFrame._copyDataframe(this);
      newdf[colName] = newList;
      return newdf;
    }
  }
  /// Filters out null and NaN values from a specified column and returns a list of doubles.
  /// - Parameters:
  ///   - columnIndex: The index of the column to filter.
  ///   - skipNull: If true, skips null values; if false, replaces nulls with 0.0.
  List<double> filterNulls(var columnIndex, {bool skipNull = true}) {
    final rawData = List.from(_dataCore.data[columnIndex]);
    List<double> processedData;
    if (skipNull == true) {
      // Skip null values and convert to double
      processedData = [];
      for (var e in rawData) {
        if (e is num && (!(e is double) || !e.isNaN)) {
          processedData.add(e.toDouble());
        }
      }
    } else {
      // Replace null with 0.0 and convert to double
      processedData = rawData.map((e) {
        if (e is num) {
          return (e is double && e.isNaN) ? 0.0 : e.toDouble();
        } else {
          return 0.0;
        }
      }).toList();
    }
    return processedData;
  }
  /// Counts the number of `null` or `NaN` values in the specified column.
  ///
  /// Example:
  /// ```dart
  /// int nullCount = df.countNulls('ColumnName');
  /// ```
  int countNulls(var colName) {
    final column = this[colName];  
    int n = 0;
    for(var e in column){
      if(e == null || (e is double && e.isNaN)){
        ++n;
      }
    }
    return n;
  }
  /// Counts the number of zero values in a specified column.
  ///
  /// - Parameters:
  ///   - colName: The name of the column in which to count zero values.
  ///   - zeroValues: An optional list of values to be considered as zero. Default is `[0]`.
  ///
  /// - Example:
  ///   ```dart
  ///   int zeroCount = df.countZeros('ColumnName'); // Counts occurrences of 0 in the 'ColumnName' column.
  ///   int customZeroCount = df.countZeros('ColumnName', zeroValues: [0, 0.0, 'zero']); // Counts occurrences of 0, 0.0, or 'zero'.
  ///   ```
  int countZeros(var colName, {List<Object> zeroValues = const <Object>[0]}) {
    var dataInput = this[colName];
    final n = countainsValues(dataInput, zeroValues);
    return n;
  }
  /// Sums the values in a specified column.
  double sumCol(int columnIndex) {
    return filterNulls(columnIndex)
        .reduce((total, val) => total + val); // Sum non-null values in the column
  }
  /// Calculates the mean (average) of the values in a specified column.
  double mean(int columnIndex) {
    return sumCol(columnIndex) / filterNulls(columnIndex).length; // Divide the sum by the count of non-null values
  }
  /// Returns the maximum value in a specified column.
  double max(int columnIndex) {
    return filterNulls(columnIndex).reduce((a, b) => a > b ? a : b); // Find the maximum of the non-null values
  }
  /// Returns the minimum value in a specified column.
  double min(int columnIndex) {
    return filterNulls(columnIndex).reduce((a, b) => a < b ? a : b);
  }

  // * Moving Window Operations 

  /// Computes the rolling sum over a fixed-size window on a numeric column.
  ///
  /// [columnIndex] is the index of the column to operate on.
  /// [window] is the number of consecutive values to include in each sum.
  List<double> rollingSum(int columnIndex, int window) {
    if (window <= 0) throw ArgumentError('Window must be > 0');
    final data = filterNulls(columnIndex);
    final n = data.length;
    final result = List<double>.filled(n, double.nan);
    double sum = 0;
    for (var i = 0; i < n; i++) {
      sum += data[i];
      if (i >= window) sum -= data[i - window];
      if (i >= window - 1) result[i] = sum;
    }
    return result;
  }
  /// Rolling mean (average) over a fixed-size window.
  /// 
  /// [columnIndex] is the index of the column to operate on.
  /// [window] is the number of consecutive values to include in each sum.
  List<double> rollingMean(int columnIndex, int window) {
    final sums = rollingSum(columnIndex, window);
    return sums
        .map((s) => s.isNaN ? double.nan : s / window)
        .toList(growable: false);
  }
  /// Rolling standard deviation (population std) over a fixed-size window.  
  ///
  /// [columnIndex] is the index of the column to operate on.
  /// [window] is the number of consecutive values to include in each sum.
  List<double> rollingStd(int columnIndex, int window) {
    if (window <= 0) throw ArgumentError('Window must be > 0');
    final data = filterNulls(columnIndex);
    final n = data.length;
    final result = List<double>.filled(n, double.nan);
    for (var i = window - 1; i < n; i++) {
      // compute mean for this window
      double m = 0;
      for (var j = i - window + 1; j <= i; j++) {
        m += data[j];
      }
      m /= window;
      // compute variance
      double sumSq = 0;
      for (var j = i - window + 1; j <= i; j++) {
        final d = data[j] - m;
        sumSq += d * d;
      }
      result[i] = math.sqrt(sumSq / window);
    }
    return result;
  }
  /// Apply a custom function over each fixed-size window.
  /// 
  /// [columnIndex] is the index of the column to operate on.
  /// [window] is the number of consecutive values to include in each sum.
  List<double> rollingApply(
    int columnIndex,
    int window,
    double Function(List<double>) func,
  ) {
    if (window <= 0) throw ArgumentError('Window must be > 0');
    final data = filterNulls(columnIndex);
    final n = data.length;
    final result = List<double>.filled(n, double.nan);
    for (var i = window - 1; i < n; i++) {
      final windowData = data.sublist(i - window + 1, i + 1);
      result[i] = func(windowData);
    }
    return result;
  }

  // * Expanding Window Operations

  /// Computes the expanding minimum for a column.
  List<double> expandingMin(int columnIndex) {
    final data = filterNulls(columnIndex);
    final n = data.length;
    final result = List<double>.filled(n, double.nan);
    if (n == 0) return result;
    double m = data[0];
    result[0] = m;
    for (var i = 1; i < n; i++) {
      if (data[i] < m) m = data[i];
      result[i] = m;
    }
    return result;
  }
  /// Computes the expanding maximum for a column.
  List<double> expandingMax(int columnIndex) {
    final data = filterNulls(columnIndex);
    final n = data.length;
    final result = List<double>.filled(n, double.nan);
    if (n == 0) return result;
    double m = data[0];
    result[0] = m;
    for (var i = 1; i < n; i++) {
      if (data[i] > m) m = data[i];
      result[i] = m;
    }
    return result;
  }
  /// Computes the expanding mean for a column.
  List<double> expandingMean(int columnIndex) {
    final data = filterNulls(columnIndex);
    final n = data.length;
    final result = List<double>.filled(n, double.nan);
    double sum = 0;
    for (var i = 0; i < n; i++) {
      sum += data[i];
      result[i] = sum / (i + 1);
    }
    return result;
  }
  /// Computes the expanding variance for a column.
  /// The [ddof] parameter determines the type of variance: `0` for population, `1` for sample.
  List<double> expandingVar(int columnIndex, {int ddof = 1}) {
    final data = filterNulls(columnIndex);
    final n = data.length;
    final result = List<double>.filled(n, double.nan);
    double sum = 0, sumSq = 0;

    for (var i = 0; i < n; i++) {
      sum += data[i];
      sumSq += data[i] * data[i];
      final count = i + 1;

      if (count > ddof) {
        final mean = sum / count;
        // Variance = (E[X²] - mean²) * n / (n - ddof)
        result[i] = ((sumSq / count) - (mean * mean)) * count / (count - ddof);
      } else {
        result[i] = double.nan; // Not enough data for the given ddof
      }
    }
    return result;
  }

  // * Exponential Weighted Moving Operations

  /// Computes the exponentially weighted moving average (EWM) for a column.
  /// The [alpha] parameter is the smoothing factor and must be in the range `(0, 1]`.
  List<double> ewmMean(int columnIndex, double alpha) {
    if (alpha <= 0 || alpha > 1) {
      throw ArgumentError.value(alpha, 'alpha', 'must be in (0,1]');
    }
    final data = filterNulls(columnIndex);
    final n = data.length;
    final result = List<double>.filled(n, double.nan);
    if (n == 0) return result;
    result[0] = data[0];
    for (var i = 1; i < n; i++) {
      result[i] = alpha * data[i] + (1 - alpha) * result[i - 1];
    }
    return result;
  }
  /// Computes the exponentially weighted moving variance for a column (population).
  /// The [alpha] parameter is the smoothing factor and must be in the range `(0, 1]`.
  List<double> ewmVar(int columnIndex, double alpha) {
    if (alpha <= 0 || alpha > 1) {
      throw ArgumentError.value(alpha, 'alpha', 'must be in (0,1]');
    }
    final data = filterNulls(columnIndex);
    final n = data.length;
    final mean = ewmMean(columnIndex, alpha);
    final result = List<double>.filled(n, double.nan);
    if (n == 0) return result;
    result[0] = 0.0;
    for (var i = 1; i < n; i++) {
      final dev = data[i] - mean[i];
      result[i] =
          alpha * dev * dev + (1 - alpha) * result[i - 1];
    }
    return result;
  }
  /// Exponential Weighted Moving Covariance (EWMCov) between two columns.
  /// 
  /// - Parameters: 
  ///   - [alpha] - smoothing factor, must be in range `(0, 1]`.
  ///   - [useLaggedMean]:
  ///     - `false` (default): residual uses current mean — matches pandas
  ///     - `true`: residual uses lagged mean — matches signal processing
  List<double> ewmCov(
    int columnIndexX,
    int columnIndexY,
    double alpha, {
    bool useLaggedMean = false,
  }) {
    if (alpha <= 0 || alpha > 1) {
      throw ArgumentError.value(alpha, 'alpha', 'must be in (0,1]');
    }

    final x = filterNulls(columnIndexX);
    final y = filterNulls(columnIndexY);
    if (x.length != y.length) {
      throw ArgumentError('Columns must have same length: got ${x.length} vs ${y.length}');
    }

    final n = x.length;
    final result = List<double>.filled(n, double.nan);
    if (n == 0) return result;

    // Instead of 0.0, the first EWM covariance is undefined (NaN)
    // because there is no prior value to form a covariance.
    result[0] = double.nan;

    double meanX = x[0];
    double meanY = y[0];
    double cov = 0.0; // This is just a running accumulator; not yet “returned” at index 0.

    for (var i = 1; i < n; i++) {
      double dx, dy;

      if (useLaggedMean) {
        // Lagged mode: residuals use the previous mean
        dx = x[i] - meanX;
        dy = y[i] - meanY;
      } else {
        // Concurrent mode (pandas‐style): update means first
        meanX = alpha * x[i] + (1 - alpha) * meanX;
        meanY = alpha * y[i] + (1 - alpha) * meanY;
        dx = x[i] - meanX;
        dy = y[i] - meanY;
      }

      cov = alpha * dx * dy + (1 - alpha) * cov;
      result[i] = cov;

      if (useLaggedMean) {
        // Update means *after* computing residuals
        meanX = alpha * x[i] + (1 - alpha) * meanX;
        meanY = alpha * y[i] + (1 - alpha) * meanY;
      }
    }

    return result;
  }
  /// EWM correlation between two columns.
  ///
  /// - Parameters:
  ///   - [alpha] - smoothing factor, must be in range `(0, 1]`.
  List<double> ewmCorr(
    int columnIndexX,
    int columnIndexY,
    double alpha,
  ) {
    final cov = ewmCov(columnIndexX, columnIndexY, alpha);
    final varX = ewmVar(columnIndexX, alpha);
    final varY = ewmVar(columnIndexY, alpha);
    final n = cov.length;
    final corr = List<double>.filled(n, double.nan);
    for (var i = 0; i < n; i++) {
      final denom = math.sqrt(varX[i] * varY[i]);
      corr[i] = (denom == 0 || cov[i].isNaN) ? double.nan : cov[i] / denom;
    }
    return corr;
  }

  // * Time‐Series Methods

  /// Resamples a time series by upsampling or downsampling with optional interpolation.
  ///
  /// - Parameters:
  ///   - [timeCol] - column containing time values (must match index).
  ///   - [valueCol] - numeric column to resample.
  ///   - [freq] - target frequency (interval between points).
  ///   - [agg] - aggregation function for downsampling (default: mean).
  ///   - [interpolation] - method to fill missing values (default: `'linear'`).
  DataFrame resample(
    String timeCol,
    String valueCol,
    Duration freq, {
    double Function(List<double>)? agg,
    String interpolation = 'linear',
  }) {
    // 1) Turn index entries into a List<DateTime> for arithmetic:
    final times = index.map((t) {
      if (t is DateTime) return t;
      if (t is Timestamp) return t.to_datetime();
      throw ArgumentError("Index entries must be DateTime or Timestamp");
    }).cast<DateTime>().toList();

    // 2) Extract numeric column and bounding times
    final vals = filterNulls(_colIndex(valueCol));
    final low  = times.first;
    final high = times.last;

    // 3) Build a plain DateTime grid [low, low+freq, ..., high]:
    final newDateTimes = <DateTime>[];
    for (var t = low; t.isBefore(high) || t == high; t = t.add(freq)) {
      newDateTimes.add(t);
    }

    // 4) Compute newVals by aggregating or linearly interpolating:
    final downAgg = agg ?? (List<double> xs) => xs.reduce((a, b) => a + b) / xs.length;
    final newVals = <double>[];
    for (var t in newDateTimes) {
      // a) collect any original points in [t, t+freq):
      final bucket = <double>[];
      for (var i = 0; i < times.length; i++) {
        if (!times[i].isBefore(t) && times[i].isBefore(t.add(freq))) {
          bucket.add(vals[i]);
        }
      }
      if (bucket.isNotEmpty) {
        newVals.add(downAgg(bucket));
      } else if (interpolation == 'linear') {
        // b) find neighbors for linear interpolation
        final hiIdx = times.indexWhere((d) => d.isAfter(t));
        final loIdx = hiIdx - 1;
        if (loIdx < 0 || hiIdx < 0 || hiIdx >= times.length) {
          newVals.add(double.nan);
        } else {
          final t0 = times[loIdx], t1 = times[hiIdx];
          final v0 = vals[loIdx],    v1 = vals[hiIdx];
          final frac = t.difference(t0).inMilliseconds /
                      (t1.difference(t0).inMilliseconds);
          newVals.add(v0 + (v1 - v0) * frac);
        }
      } else {
        newVals.add(double.nan);
      }
    }

    // 5) Decide output index type based on the original:
    final bool wasTimestamp = index.first is Timestamp;
    final newTimes = wasTimestamp
      ? newDateTimes.map((dt) => Timestamp(dt)).toList()
      : newDateTimes;

    // 6) Return one‐column DataFrame (using a Map to avoid column‐size errors)
    return DataFrame(
      { valueCol: newVals },
      index: newTimes,
    );
  }
  /// Computes partial autocorrelation using the Durbin–Levinson algorithm.
  ///
  /// - Parameters:
  ///   - [columnIndex] - index of the numeric column to analyze.
  ///   - [maxLag] - maximum lag to compute partial autocorrelations for.
  List<double> partialAutocorrelation(int columnIndex, int maxLag) {
    final data = filterNulls(columnIndex);
    final n = data.length;
    final mean = data.reduce((a,b)=>a+b) / n;
    // compute autocovariances r[0..maxLag]
    final r = List<double>.filled(maxLag+1, 0);
    for (var k = 0; k <= maxLag; k++) {
      double sum = 0;
      for (var i = k; i < n; i++) {
        sum += (data[i] - mean) * (data[i - k] - mean);
      }
      r[k] = sum / n;
    }
    // Durbin‐Levinson recursion
    final phi = List.generate(maxLag+1, (_) => List<double>.filled(maxLag+1, 0));
    final pacf = List<double>.filled(maxLag+1, double.nan);
    double sigma = r[0];

    phi[1][1] = r[1] / r[0];
    pacf[1] = phi[1][1];
    sigma *= (1 - phi[1][1] * phi[1][1]);

    for (var k = 2; k <= maxLag; k++) {
      double acc = 0;
      for (var j = 1; j < k; j++) {
        acc += phi[k-1][j] * r[k - j];
      }
      phi[k][k] = (r[k] - acc) / sigma;
      for (var j = 1; j < k; j++) {
        phi[k][j] = phi[k-1][j] - phi[k][k] * phi[k-1][k - j];
      }
      sigma *= (1 - phi[k][k] * phi[k][k]);
      pacf[k] = phi[k][k];
    }
    pacf[0] = 1.0;
    return pacf;
  }
  /// Computes autocorrelation up to a specified lag.
  ///
  /// - Parameters:
  ///   - [columnIndex] - index of the numeric column to analyze.
  ///   - [maxLag] - maximum lag value to compute autocorrelation for.
  List<double> autocorrelation(int columnIndex, int maxLag) {
    final data = filterNulls(columnIndex);
    final n = data.length;
    final mean = data.reduce((a,b)=>a+b) / n;
    final var0 = data.map((x)=> (x-mean)*(x-mean)).reduce((a,b)=>a+b) / n;
    final acf = List<double>.filled(maxLag+1, double.nan);
    for (var k = 0; k <= maxLag; k++) {
      double c = 0;
      for (var i = k; i < n; i++) {
        c += (data[i] - mean) * (data[i - k] - mean);
      }
      acf[k] = (c / (n - k)) / var0;
    }
    return acf;
  }

  /// Classical seasonal-trend decomposition using an additive model.
  ///
  /// - Parameters:
  ///   - [columnIndex] - index of the numeric column to decompose.
  ///   - [period] - number of steps per seasonal cycle (e.g. 12 for monthly data).
  DataFrame seasonalDecompose(
    int columnIndex,
    int period,
  ) {
    final data = filterNulls(columnIndex);
    final n = data.length;

    // 4a) Trend: centered moving average
    final trend = List<double>.filled(n, double.nan);
    for (var i = period ~/ 2; i < n - (period - 1) ~/ 2; i++) {
      double sum = 0;
      for (var j = i - period ~/ 2; j <= i + (period - 1) ~/ 2; j++) {
        sum += data[j];
      }
      trend[i] = sum / period;
    }

    // 4b) Seasonal: average of detrended by phase
    final seasonal = List<double>.filled(n, double.nan);
    final bucketSum   = List<double>.filled(period, 0);
    final bucketCount = List<int>.filled(period, 0);
    for (var i = 0; i < n; i++) {
      if (!trend[i].isNaN) {
        final ph = i % period;
        bucketSum[ph] += data[i] - trend[i];
        bucketCount[ph]++;
      }
    }
    for (var ph = 0; ph < period; ph++) {
      if (bucketCount[ph] > 0) {
        final avg = bucketSum[ph] / bucketCount[ph];
        for (var i = ph; i < n; i += period) {
          seasonal[i] = avg;
        }
      }
    }

    // 4c) Residual
    final resid = List<double>.filled(n, double.nan);
    for (var i = 0; i < n; i++) {
      if (!trend[i].isNaN && !seasonal[i].isNaN) {
        resid[i] = data[i] - trend[i] - seasonal[i];
      }
    }

    return DataFrame(
      {'trend':trend, 'seasonal':seasonal, 'residual':resid},
      index: index,
    );
  }

  /// Covariance matrix for a list of columns (population).
  ///
  /// - Parameters:
  ///   - [cols] - list of column names to include in the covariance matrix.
  DataFrame covarianceMatrix(List<String> cols) {
    final n = cols.length;
    // 1) raw (nullable) columns
    final raw = cols
        .map((c) => (this[c] as List).cast<double?>())   // List<double?>
        .toList();

    final rowCount = raw.first.length;                   // List length → OK

    // 2) rows where *every* column is non-null
    final validRows = <int>[];
    for (var r = 0; r < rowCount; r++) {
      if (raw.every((col) => col[r] != null)) {         // [] now on List<double?>
        validRows.add(r);
      }
    }
    if (validRows.isEmpty) {
      throw StateError('No rows with all-non-null values in the requested cols.');
    }

    // 3) align columns on those good rows
    final data = List.generate(
      n,
      (j) => validRows.map((i) => raw[j][i]!).toList(), // [] on List<double?>
      growable: false,
    );

    // 4) column means (values are non-null doubles here) 
    final means = List<double>.generate(
      n,
      (j) => data[j].reduce((a, b) => a + b) / data[j].length,
      growable: false,
    );

    // 5) covariance matrix
    final cov = List.generate(
      n,
      (_) => List<double>.filled(n, 0.0, growable: false),
      growable: false,
    );

    for (var i = 0; i < n; i++) {
      for (var j = i; j < n; j++) {
        var sum = 0.0;
        for (var k = 0; k < data[i].length; k++) {
          sum += (data[i][k] - means[i]) * (data[j][k] - means[j]);
        }
        cov[i][j] = cov[j][i] = sum / data[i].length;   // population covariance
      }
    }
    return DataFrame(cov, columns: cols, index: cols);
  }
  /// Principal Component Analysis (PCA) on a group of columns.
  ///
  /// - Parameters:
  ///   - [cols] - list of columns to include; uses all columns if `null`.
  ///   - [center] - whether to subtract mean from each column (default: `true`).
  ///   - [scale] - whether to divide by standard deviation (default: `false`).
  PCAModel pca({
    List<String>? cols,
    bool center = true,
    bool scale  = false,
  }) {
    final useCols = cols ?? columns; // inside DataFrame class
    // 1. Build data matrix (rows = observations, cols = variables)
    final mat = filterNulls(_colIndex(useCols.first))
        .asMap()
        .keys
        .map((i) => useCols.map((c) => filterNulls(_colIndex(c))[i]).toList())
        .toList();
    // 2. Center/scale in-place
    for (var j = 0; j < useCols.length; j++) {
      final col = mat.map((row) => row[j]).toList();
      final mu = col.reduce((a, b) => a + b) / col.length;
      final sigma = math.sqrt(
        col.map((x) => math.pow(x - mu, 2)).reduce((a, b) => a + b) /
        col.length
      );
      for (var i = 0; i < mat.length; i++) {
        var v = mat[i][j];
        if (center) v -= mu;
        if (scale && sigma > 0) v /= sigma;
        mat[i][j] = v;
      }
    }
    // 3. Covariance + eigen
    final covMat = _covarianceFromMat(mat);
    final eig = computeEigenDecomposition(covMat)
                  ..sort((a, b) => b.value.compareTo(a.value));
    // Gram–Schmidt for 2x2 case
    final v1 = eig[0].vector;
    final v2 = eig[1].vector;
    // Subtract projection of v2 onto v1:
    final dot    = v2[0] * v1[0] + v2[1] * v1[1];
    final normSq = v1[0] * v1[0] + v1[1] * v1[1];
    var w0 = v2[0] - (dot / normSq) * v1[0];
    var w1 = v2[1] - (dot / normSq) * v1[1];
    var wlen = math.sqrt(w0 * w0 + w1 * w1);
    if (wlen < 1e-12) {
      // If it collapsed to zero, pick the perpendicular [b, -a]:
      w0 = v1[1];
      w1 = -v1[0];
      wlen = math.sqrt(w0 * w0 + w1 * w1);
    }
    // Normalize
    if (wlen > 0) {
      w0 /= wlen;
      w1 /= wlen;
    }
    // Enforce first component ≥ 0 for consistent sign
    if (w0 < 0) {
      w0 = -w0;
      w1 = -w1;
    }
    eig[1].vector[0] = w0;
    eig[1].vector[1] = w1;
    // 4. Build loadings and scores
    final loadMat = eig.map((e) => e.vector).toList(); // each vector is a PC row
    final scoreMat = mat
        .map((row) => eig.map((e) => _dot(row, e.vector)).toList())
        .toList();
    return PCAModel(
      loadings: DataFrame(
        loadMat,
        columns: useCols,
        index: List.generate(useCols.length, (i) => 'PC${i + 1}'),
      ),
      scores: DataFrame(
        scoreMat,
        columns: List.generate(useCols.length, (i) => 'PC${i + 1}'),
        index: index,
      ),
    );
  }

  /// Singular Value Decomposition (SVD) on a group of columns.
  ///
  /// - Parameters:
  ///   - [cols] - list of columns to include; uses all columns if `null`.
  ///   - [sweeps] - number of Jacobi sweeps (default: `100`).
  SVDResult svd({List<String>? cols, int sweeps = 100}) { 
    final useCols = cols ?? columns;
    final mat = filterNulls(_colIndex(useCols.first))
        .asMap()
        .keys
        .map((i) => useCols
            .map((c) => filterNulls(_colIndex(c))[i].toDouble())
            .toList())
        .toList();
    final numeric = jacobiSvd(mat, maxSweeps: sweeps);
    return SVDResult(
      U: DataFrame(numeric.U, columns: useCols, index: index),
      S: numeric.S,
      Vt: DataFrame(
        numeric.Vt,
        columns: List.generate(useCols.length, (i) => 'V${i + 1}'),
        index: useCols,
      ),
    );
  }

  // * Interpolation and imputation functions

  /// K-Nearest Neighbors (KNN) imputation for missing values.
  ///
  /// - Parameters:
  ///   - [featureCols] - columns used to compute distance between rows.
  ///   - [targetCols] - columns with missing values to impute.
  ///   - [k] - number of nearest neighbors to average (default: `5`).
  DataFrame knnImputer({
    required List<String> featureCols,
    required List<String> targetCols,
    int k = 5,
  }) {
    final n = _dataCore.rowLastIndexVal + 1;
    // 1) pull full feature & target columns (including NaN placeholders)
    final feats = featureCols.map((c) => this[c] as List<double>).toList();
    final targs = targetCols.map((c) => this[c] as List<double>).toList();
    // 2) helper to compute distance between rows i & j
    double rowDist(int i, int j) {
      var sum = 0.0, cnt = 0;
      for (var col in feats) {
        final xi = col[i], xj = col[j];
        if (xi.isNaN || xj.isNaN) continue;
        sum += (xi - xj) * (xi - xj);
        cnt++;
      }
      return cnt > 0 ? math.sqrt(sum) : double.infinity;
    }
    // 3) deep-copy all data into rows (preserving NaN where present)
    final rows = List.generate(n, (i) {
      return columns.map((c) => (this[c] as List<double>)[i]).toList();
    });
    // 4) for each target column & each row with missing, impute
    for (var t = 0; t < targetCols.length; t++) {
      final colIdx = _colIndex(targetCols[t]);
      for (var i = 0; i < n; i++) {
        if (targs[t][i].isNaN) {
          // gather distances to all other rows with non-null target
          final dists = <int, double>{};
          for (var j = 0; j < n; j++) {
            if (j != i && !targs[t][j].isNaN) {
              dists[j] = rowDist(i, j);
            }
          }
          // select k closest (with finite distance)
          final neigh = dists.entries.toList()
            ..sort((a, b) => a.value.compareTo(b.value));
          final idxs = neigh
              .take(k)
              .where((e) => e.value < double.infinity)
              .map((e) => e.key)
              .toList();
          if (idxs.isNotEmpty) {
            final imputed = idxs
                .map((j) => targs[t][j])
                .reduce((a, b) => a + b) /
              idxs.length;
            rows[i][colIdx] = imputed;
          }
        }
      }
    }
    // 5) build new DataFrame
    return DataFrame(rows, columns: columns, index: index);
  }
  /// Interpolates missing values in a column using time-aware or index-based methods.
  ///
  /// - Parameters:
  ///   - [timeCol] - column representing index/time values.
  ///   - [colName] - name of the column to interpolate.
  ///   - [method] - interpolation method (default: `InpMethod.polynomial`). Support for: linear, spline , time.
  ///   - [degree] - number of points used in polynomial interpolation (default: `3`).
  ///   - [precision] - optional number of decimal places to round; default is no rounding.
  DataFrame interpolate(
    String timeCol,
    String colName, {
    InpMethod method = InpMethod.polynomial,
    int degree = 3,
    int? precision, // if null, do not round
  }) {
    final times = index;
    final convertedIndex = times.map((t) {
      if (t is num) return t.toDouble();
      if (t is DateTime) return t.millisecondsSinceEpoch.toDouble();
      if (t is Timestamp) return t.timestamp();
      throw ArgumentError('Unsupported type for timeCol: $t');
    }).toList();

    final raw = this[colName] as List;
    final vals = raw.map((e) {
      if (e == null || (e is double && e.isNaN)) return double.nan;
      return (e is num) ? e.toDouble() : double.nan;
    }).toList();

    final n = convertedIndex.length;
    final newVals = List<double>.from(vals);

    // Indices of known values
    final nonNan = [ for (var i = 0; i < n; i++) if (!vals[i].isNaN) i ];

    for (var i = 0; i < n; i++) {
      if (newVals[i].isNaN) {
        nonNan.sort((a, b) =>
          (convertedIndex[a] - convertedIndex[i]).abs()
            .compareTo((convertedIndex[b] - convertedIndex[i]).abs())
        );
        final sel = nonNan.take(degree + 1)
          .toList()
          ..sort((a, b) => convertedIndex[a].compareTo(convertedIndex[b]));
        List<double> xsTime   = sel.map((j) => convertedIndex[j]).toList();
        List<double> xsLinear = sel.map((j) => j.toDouble()).toList();
        List<double> ysSel    = sel.map((j) => vals[j]).toList();

        double interpolated = 0;
        switch (method) {
          case InpMethod.polynomial:
            interpolated = lagrangeInterpolate(
              x: convertedIndex[i], xs: xsTime, ys: ysSel);
            break;

          case InpMethod.linear:
            // equally spaced: x = i, xs = [sel positions]
            interpolated = linearInterpolate(
              x: i.toDouble(), xs: xsLinear, ys: ysSel);
            break;

          case InpMethod.spline:
            interpolated = splineInterpolate(
              x: convertedIndex[i], xs: xsTime, ys: ysSel);
            break;

          case InpMethod.time:
            interpolated = linearInterpolate(
              x: convertedIndex[i], xs: xsTime, ys: ysSel);
            break;
        }

        if (precision != null) {
          interpolated =
              double.parse(interpolated.toStringAsFixed(precision));
        }

        newVals[i] = interpolated;
      }
    }

    return DataFrame(
      { colName: newVals },
      index: times,
    );
  }

  // * Hypothesis testing and bootstrap resampling

  /// Two-sample t-test comparing means between two groups.
  ///
  /// - Parameters:
  ///   - [groupCol] - column containing group labels (must be exactly two).
  ///   - [valueCol] - numeric column to compare between groups.
  TTestResult tTest(String groupCol, String valueCol) {
    final keys = this[groupCol];
    final vals = filterNulls(_colIndex(valueCol));
    // split into two groups
    final g1 = <double>[], g2 = <double>[];
    for (var i = 0; i < keys.length; i++) {
      final v = vals[i];
      if (keys[i] == keys.first) g1.add(v);
      else g2.add(v);
    }
    final n1 = g1.length, n2 = g2.length;
    final m1 = g1.reduce((a,b)=>a+b)/n1;
    final m2 = g2.reduce((a,b)=>a+b)/n2;
    final s1 = g1.map((x)=>math.pow(x-m1,2)).reduce((a,b)=>a+b)/(n1-1);
    final s2 = g2.map((x)=>math.pow(x-m2,2)).reduce((a,b)=>a+b)/(n2-1);
    final t = (m1 - m2) / math.sqrt(s1/n1 + s2/n2);
    final df = math.pow(s1/n1 + s2/n2, 2) /
      ((math.pow(s1/n1,2)/(n1-1)) + (math.pow(s2/n2,2)/(n2-1)));
    final pValue = 2 * (1 - _tCDF(t.abs(), df));
    return TTestResult(t, df, pValue);
  }

  /// One-way ANOVA comparing means across multiple groups.
  ///
  /// - Parameters:
  ///   - [groupCol] - column containing group labels.
  ///   - [valueCol] - numeric column to test for group-wise differences.
  ANOVAResult anova(String groupCol, String valueCol) {
    final keys = this[groupCol];
    final vals = filterNulls(_colIndex(valueCol));
    // group values
    final Map<Object, List<double>> groups = {};
    for (var i = 0; i < keys.length; i++) {
      groups.putIfAbsent(keys[i]!, () => []).add(vals[i]);
    }
    final N = vals.length, k = groups.length;
    final grandMean = vals.reduce((a,b)=>a+b)/N;
    // SS between
    double ssb = 0;
    groups.forEach((_, g) {
      final ni = g.length;
      final mi = g.reduce((a,b)=>a+b)/ni;
      ssb += ni * math.pow(mi - grandMean, 2);
    });
    // SS within
    double ssw = 0;
    groups.forEach((_, g) {
      final mi = g.reduce((a,b)=>a+b)/g.length;
      ssw += g.map((x)=>math.pow(x-mi,2)).reduce((a,b)=>a+b);
    });
    final dfB = k - 1;
    final dfW = N - k;
    final msb = ssb / dfB;
    final msw = ssw / dfW;
    final f = msb / msw;
    double fCDF(double f, double df1, double df2) {
      final x = (df1 * f) / (df1 * f + df2);
      return _betaIncReg(df1 / 2.0, df2 / 2.0, x);
    }
    final pValue = 1.0 - fCDF(f, dfB.toDouble(), dfW.toDouble());
    final etaSquared = ssb / (ssb + ssw);
    return ANOVAResult(f, dfB, dfW, pValue, etaSquared);
  }

  /// Chi-Square test (Pearson) for independence between two categorical columns.
  ///
  /// - Parameters:
  ///   - [colX] - first categorical column (rows of the contingency table).
  ///   - [colY] - second categorical column (columns of the contingency table).
  ChiSquareResult chiSquare(String colX, String colY) {
    final x = this[colX] as List;
    final y = this[colY] as List;
    final n = x.length;

    final rows = x.toSet().toList();
    final cols = y.toSet().toList();

    final obs = List.generate(rows.length, (_) => List<int>.filled(cols.length, 0));
    for (var i = 0; i < n; i++) {
      final r = rows.indexOf(x[i]);
      final c = cols.indexOf(y[i]);
      obs[r][c] += 1;
    }

    final rowSums = obs.map((r) => r.reduce((a, b) => a + b)).toList();
    final colSums = List<int>.filled(cols.length, 0);
    for (var c = 0; c < cols.length; c++) {
      for (var r = 0; r < rows.length; r++) {
        colSums[c] += obs[r][c];
      }
    }

    final total = n;
    final expected = List.generate(rows.length, (r) => List<double>.filled(cols.length, 0.0));
    double chi2 = 0;
    for (var i = 0; i < rows.length; i++) {
      for (var j = 0; j < cols.length; j++) {
        final e = rowSums[i] * colSums[j] / total;
        expected[i][j] = e;
        chi2 += math.pow(obs[i][j] - e, 2) / e;
      }
    }
    final dof = (rows.length - 1) * (cols.length - 1);
    final pValue = 1.0 - _chi2CDF(chi2, dof.toDouble());
    return ChiSquareResult(
      chi2,
      dof,
      pValue,
      DataFrame(obs.map((r) => r.map((e) => e.toDouble()).toList()).toList(),
          columns: cols.cast<String>(), index: rows.cast<String>()),
      DataFrame(expected, columns: cols.cast<String>(), index: rows.cast<String>()),
    );
  }
  /// Bootstrapped sampling distribution of a user-defined statistic.
  ///
  /// - Parameters:
  ///   - [valueCol] - numeric column to resample.
  ///   - [statistic] - function applied to each bootstrap sample.
  ///   - [nBoot] - number of bootstrap samples to generate (default: `1000`).
  BootstrapResult bootstrap({
    required String valueCol,
    required double Function(List<double>) statistic,
    int nBoot = 1000,
  }) {
    final vals = filterNulls(_colIndex(valueCol));
    final n = vals.length;
    final samples = <double>[];
    final rand = math.Random();
    for (var b = 0; b < nBoot; b++) {
      // draw with replacement
      final draw = List<double>.generate(n,
        (_) => vals[rand.nextInt(n)]);
      samples.add(statistic(draw));
    }
    return BootstrapResult(samples);
  }

  // * Signal Processing
  
  /// Computes the discrete linear convolution of two columns.
  ///
  /// - Parameters:
  ///   - [xCol] - first input column.
  ///   - [yCol] - second input column.
  DataFrame convolve(String xCol, String yCol) {
    // grab full columns (null→NaN, num→double)
    final rawX = this[xCol] as List;
    final rawY = this[yCol] as List;
    final x = rawX.map((e) => (e is num) ? e.toDouble() : double.nan).toList();
    final y = rawY.map((e) => (e is num) ? e.toDouble() : double.nan).toList();

    final n = x.length, m = y.length, p = n + m - 1;
    final result = List<double>.filled(p, 0.0);
    for (var i = 0; i < n; i++) {
      for (var j = 0; j < m; j++) {
        result[i + j] += x[i] * y[j];
      }
    }
    // one-column DataFrame, index = [0,1,...,p-1]
    final rows  = result.map((v) => [v]).toList();
    final index = List<int>.generate(p, (i) => i);
    return DataFrame(rows, columns: ['conv'], index: index);
  }

  /// Computes the discrete cross-correlation of two columns.
  ///
  /// - Parameters:
  ///   - [xCol] - first input column.
  ///   - [yCol] - second input column.
  DataFrame crossCorrelate(String xCol, String yCol) {
    final rawX = this[xCol] as List;
    final rawY = this[yCol] as List;
    final x = rawX.map((e) => (e is num) ? e.toDouble() : double.nan).toList();
    final y = rawY.map((e) => (e is num) ? e.toDouble() : double.nan).toList();

    final n = x.length, p = 2 * n - 1;
    final result = List<double>.filled(p, 0.0);
    // lags = –(n-1)…0…(n-1)
    final lags = List<int>.generate(p, (i) => i - (n - 1));
    for (var idx = 0; idx < p; idx++) {
      final lag = lags[idx];
      var sum = 0.0;
      for (var i = 0; i < n; i++) {
        final j = i + lag;
        if (j >= 0 && j < n) sum += x[i] * y[j];
      }
      result[idx] = sum;
    }
    final rows = result.map((v) => [v]).toList();
    return DataFrame(rows, columns: ['xcorr'], index: lags);
  }

  /// Computes the discrete Fourier transform (DFT) of a numeric column using FFT logic.
  ///
  /// - Parameters:
  ///   - [timeCol] - time column (used to infer sampling interval).
  ///   - [valueCol] - numeric signal to transform.
  DataFrame fft(String timeCol, String valueCol) {
    final times = this.index.cast<DateTime>();
    final ts = times
        .map((t) => t.millisecondsSinceEpoch.toDouble())
        .toList();
    // full series (null→NaN, num→double)
    final raw = this[valueCol] as List;
    final x = raw.map((e) => (e is num) ? e.toDouble() : double.nan).toList();
    final N = x.length;
    if (N < 2) return DataFrame([], columns: ['real','imag'], index: <double>[]);

    // assume uniform sampling: dt in seconds
    final dt = (ts[1] - ts[0]) / 1000.0;
    final freqs = List<double>.generate(N, (k) => k / (N * dt));

    final out = List<List<double>>.generate(N, (k) {
      var re = 0.0, im = 0.0;
      for (var n0 = 0; n0 < N; n0++) {
        final angle = 2 * math.pi * k * n0 / N;
        re += x[n0] * math.cos(angle);
        im -= x[n0] * math.sin(angle);  // negative for e^{-iωn}
      }
      return [re, im];
    }, growable: false);

    return DataFrame(out, columns: ['real','imag'], index: freqs);
  }
  
  // * General Statistical Methods 

  /// Flags outliers using the interquartile range (IQR) method.
  ///
  /// - Parameters:
  ///   - [valueCol] - numeric column to test for outliers.
  ///   - [k] - multiplier for IQR to define outlier bounds (default: `1.5`).
  DataFrame outlierIQR(String valueCol, { double k = 1.5 }) {
    // 1) pull your raw values and normalize to doubles (NaN for nulls/non-nums)
    final raw = this[valueCol] as List;
    final vals = raw
        .map((e) => (e is num) ? e.toDouble() : double.nan)
        .toList();

    // 2) build a sorted clean list for quartile calculation
    final clean = vals.where((v) => !v.isNaN).toList()..sort();
    final n = clean.length;
    if (n < 2) {
      // too few points → no outliers
      return DataFrame(
        [List<bool>.filled(vals.length, false)],
        columns: ['${valueCol}_outlierIQR'],
        index: this.index,
      );
    }

    // 3) split into lower/upper halves and compute Q1, Q3
    final lower = clean.sublist(0, n ~/ 2);
    final upper = clean.sublist((n + 1) ~/ 2, n);
    final q1 = _median(lower);
    final q3 = _median(upper);
    final iqr = q3 - q1;

    // 4) define your bounds
    final lb = q1 - k * iqr;
    final ub = q3 + k * iqr;

    // 5) flag anything outside [lb, ub]
    final flags = vals
        .map((v) => (!v.isNaN && (v < lb || v > ub)))
        .toList();

    // 6) transpose flags into a Nx1 List<List<bool>>
    final data = flags.map((f) => [f]).toList();

    return DataFrame(
      data,
      columns: ['${valueCol}_outlierIQR'],
      index: this.index,
    );
  }

  /// Flags outliers using the Z-score method.
  ///
  /// - Parameters:
  ///   - [valueCol] - numeric column to test for outliers.
  ///   - [threshold] - Z-score cutoff for identifying outliers (default: `3.0`).
  DataFrame outlierZScore(String valueCol, { double threshold = 3.0 }) {
    // 1) extract and normalize to doubles (NaN for non-nums)
    final raw = this[valueCol] as List;
    final vals = raw
        .map((e) => (e is num) ? e.toDouble() : double.nan)
        .toList();

    // 2) build clean list for stats
    final clean = vals.where((v) => !v.isNaN).toList();
    final n = clean.length;
    if (n < 2) {
      // too few points → no outliers
      return DataFrame(
        [List<bool>.filled(vals.length, false)],
        columns: ['${valueCol}_outlierZ'],
        index: this.index,
      );
    }

    // 3) compute mean & std
    final mean = clean.reduce((a, b) => a + b) / n;
    final var0 = clean
        .map((v) => (v - mean) * (v - mean))
        .reduce((a, b) => a + b) / n;
    final std = math.sqrt(var0);

    // 4) flag by Z-score threshold
    final flags = vals.map((v) {
      if (v.isNaN || std == 0) return false;
      return (v - mean).abs() / std > threshold;
    }).toList();

    // 5) transpose into N rows × 1 column
    final data = flags.map((f) => [f]).toList();

    return DataFrame(
      data,
      columns: ['${valueCol}_outlierZ'],
      index: this.index,
    );
  }

  /// Computes the rolling median absolute deviation (MAD) for a numeric column.
  ///
  /// - Parameters:
  ///   - [valueCol] - numeric column to compute rolling MAD on.
  ///   - [window] - number of values in each rolling window (must be > 0).
  DataFrame rollingMad(String valueCol, int window) {
    if (window <= 0) {
      throw ArgumentError('window must be > 0');
    }

    // 1) extract and normalize values
    final raw = this[valueCol] as List;
    final vals = raw
        .map((e) => (e is num) ? e.toDouble() : double.nan)
        .toList();
    final n = vals.length;
    final result = List<double>.filled(n, double.nan);

    // 2) for each window-end, compute MAD
    for (var i = window - 1; i < n; i++) {
      final win = vals
          .sublist(i - window + 1, i + 1)
          .where((v) => !v.isNaN)
          .toList();
      if (win.isNotEmpty) {
        final m = _median(win);
        final dev = win.map((v) => (v - m).abs()).toList();
        result[i] = _median(dev);
      }
    }
    // 3) transpose into N rows × 1 column
    final data = result.map((v) => [v]).toList();
    return DataFrame(
      data,
      columns: ['${valueCol}_rollingMAD'],
      index: this.index,
    );
  }

  /// Computes Local Outlier Factor (LOF) scores for multivariate outlier detection.
  ///
  /// - Parameters:
  ///   - [cols] - list of numeric columns to use; uses all columns if `null`.
  ///   - [k] - number of nearest neighbors to evaluate local density (default: `20`).
  DataFrame localOutlierFactor({
    List<String>? cols,
    int k = 20,
  }) {
    final useCols  = cols ?? this.columns;
    final rowCount = this.index.length;

    // 1) build full row‐vectors (double.nan for missing)
    final rows = List<List<double>>.generate(
      rowCount,
      (i) => useCols.map((c) {
        final e = (this[c] as List)[i];
        return (e is num) ? e.toDouble() : double.nan;
      }).toList(),
      growable: false,
    );

    // 2) pick only complete rows
    final validIdx = <int>[];
    for (var i = 0; i < rowCount; i++) {
      if (rows[i].every((v) => !v.isNaN)) validIdx.add(i);
    }
    final m = validIdx.length;

    // 3) if no valid rows, return all-NaN column
    if (m == 0) {
      final nanList = List<double>.filled(rowCount, double.nan);
      final data    = nanList.map((v) => [v]).toList();
      return DataFrame(
        data,
        columns: ['lof'],
        index: this.index,
      );
    }

    // 4) distance function & matrix among valid rows
    double dist(int i, int j) {
      var s = 0.0;
      for (var d = 0; d < useCols.length; d++) {
        final diff = rows[i][d] - rows[j][d];
        s += diff * diff;
      }
      return math.sqrt(s);
    }

    final dmat = List.generate(m, (_) => List<double>.filled(m, 0.0));
    for (var a = 0; a < m; a++) {
      for (var b = a; b < m; b++) {
        final dmatVal = dist(validIdx[a], validIdx[b]);
        dmat[a][b] = dmat[b][a] = dmatVal;
      }
    }

    // 5) k-distance for each valid point
    final kdist = List<double>.generate(m, (i) {
      final neigh = List<double>.from(dmat[i])..sort();
      return neigh[math.min(k, m - 1)];
    });

    // 6) local reachability density (lrd)
    final lrd = List<double>.filled(m, 0.0);
    for (var i = 0; i < m; i++) {
      final nbrs = [
        for (var j = 0; j < m; j++)
          if (dmat[i][j] <= kdist[i]) j
      ];
      final sumRD = nbrs.fold<double>(
        0.0,
        (sum, j) => sum + math.max(kdist[j], dmat[i][j]),
      );
      lrd[i] = nbrs.isNotEmpty ? (nbrs.length / sumRD) : double.nan;
    }

    // 7) compute LOF scores
    final lof = List<double>.filled(rowCount, double.nan);
    for (var i = 0; i < m; i++) {
      final nbrs = [
        for (var j = 0; j < m; j++)
          if (dmat[i][j] <= kdist[i]) j
      ];
      final sumRatio = nbrs.fold<double>(
        0.0,
        (sum, j) => sum + (lrd[j] / lrd[i]),
      );
      lof[validIdx[i]] = nbrs.isNotEmpty ? (sumRatio / nbrs.length) : double.nan;
    }

    // 8) transpose into N rows × 1 column
    final data = lof.map((v) => [v]).toList();
    return DataFrame(
      data,
      columns: ['lof'],
      index: this.index,
    );
  }

  // * Weighted Statistics

  /// Computes a weighted histogram of a column using another column as weights (Freedman–Diaconis).
  ///
  /// - Parameters:
  ///   - [valueCol] - numeric values to bin.
  ///   - [weightCol] - weights corresponding to each value.
  ///   - [bins] - optional number of histogram bins (if omitted, estimated via IQR rule).
  DataFrame weightedHistogram(
    String valueCol,
    String weightCol, {
    int? bins,
  }) {
    final rawV = this[valueCol] as List;
    final rawW = this[weightCol] as List;
    final pairs = <MapEntry<double,double>>[];

    for (var i = 0; i < rawV.length; i++) {
      final v = rawV[i], w = rawW[i];
      if (v is num && w is num) {
        final value = v.toDouble();
        final weight = w.toDouble();
        if (weight > 0) pairs.add(MapEntry(value, weight));
      }
    }
    if (pairs.isEmpty) {
      return DataFrame([], columns: ['binStart','binEnd','weight']);
    }
    pairs.sort((a, b) => a.key.compareTo(b.key));
    final values  = pairs.map((e) => e.key).toList();
    final weights = pairs.map((e) => e.value).toList();
    final n = values.length;
    final minv = values.first;
    final maxv = values.last;

    // compute IQR for bin width
    final q = weightedQuantile(rawV, rawW, [0.25, 0.75]);
    final iqr = q[1] - q[0];
    final width = (iqr > 0)
        ? 2 * iqr / math.pow(n, 1/3)
        : (maxv - minv) / (bins ?? 1);

    // compute a positive integer for bins (at least 1)
    final computed = ((maxv - minv) / width).ceil();
    final binCount = bins ?? (computed < 1 ? 1 : computed);
    final binWidth = (maxv - minv) / binCount;
    final edges = List<double>.generate(
      binCount + 1,
      (i) => minv + binWidth * i,
    );
    final hist = List<double>.filled(binCount, 0.0);
    for (var i = 0; i < n; i++) {
      var idx = ((values[i] - minv) / binWidth).floor();
      if (idx < 0) idx = 0;
      if (idx >= binCount) idx = binCount - 1;
      hist[idx] += weights[i];
    }

    final rows = <List<Object?>>[];
    for (var b = 0; b < binCount; b++) {
      rows.add([edges[b], edges[b + 1], hist[b]]);
    }
    return DataFrame(rows, columns: ['binStart','binEnd','weight']);
  }

  /// Applies single, double, or triple exponential smoothing to a series.
  /// 
  /// - [valueCol]: name of the column containing the input series.
  /// - [period]: seasonal period (used only for Holt–Winters).
  /// - [method]: smoothing variant—
  ///   • 'ses' for Simple Exponential Smoothing  
  ///   • 'holt' for Holt’s Linear Trend  
  ///   • 'hw' (default) for Holt–Winters triple smoothing (requires 2×[period] data points+) 
  /// - [alpha]: level smoothing factor (0–1).  
  /// - [beta]: trend smoothing factor (0–1), used by 'holt' and 'hw'.  
  /// - [gamma]: seasonal smoothing factor (0–1), used by 'hw'.  
  /// - [seasonalType]: 'additive' (default) or 'multiplicative'.
  ///
  /// Returns a new DataFrame with columns:
  ///   • level      — smoothed level estimate  
  ///   • trend      — smoothed trend estimate (zero for 'ses')  
  ///   • seasonal   — seasonal component (zero for 'ses'/'holt')  
  ///   • fitted     — in‐sample forecast at each time step
  DataFrame exponentialSmoothing(
    String valueCol,
    int period, {
    String method = 'hw',           // 'ses', 'holt', or 'hw'
    double alpha = 0.2,
    double beta  = 0.1,
    double gamma = 0.1,
    String seasonalType = 'additive', // 'additive' or 'multiplicative'
  }) {
    // 1) pull your values as doubles
    final raw    = this[valueCol] as List;
    final series = raw.map((e) => (e is num) ? e.toDouble() : double.nan).toList();
    final n      = series.length;

    // 2) prepare containers
    final level    = List<double>.filled(n, double.nan);
    final trend    = List<double>.filled(n, double.nan);
    final seasonal = List<double>.filled(n, double.nan);
    final fitted   = List<double>.filled(n, double.nan);

    // 3) compute smoothing
    switch (method) {
      case 'ses':
        level[0]  = series[0];
        fitted[0] = series[0];
        for (var t = 1; t < n; t++) {
          level[t]  = alpha * series[t] + (1 - alpha) * level[t - 1];
          fitted[t] = level[t];
        }
        break;

      case 'holt':
        if (n < 2) throw ArgumentError('Need ≥2 points for Holt’s method');
        level[0]  = series[0];
        trend[0]  = series[1] - series[0];
        fitted[0] = series[0];
        for (var t = 1; t < n; t++) {
          final prevL = level[t - 1], prevT = trend[t - 1];
          level[t]  = alpha * series[t] + (1 - alpha) * (prevL + prevT);
          trend[t]  = beta  * (level[t] - prevL) + (1 - beta) * prevT;
          fitted[t] = level[t] + trend[t];
        }
        break;

      case 'hw':
      default:
        if (n < 2 * period) {
          throw ArgumentError('Need ≥2×period data points for Holt-Winters');
        }
        final avg1 = series.sublist(0, period).reduce((a, b) => a + b) / period;
        final avg2 = series.sublist(period, 2 * period).reduce((a, b) => a + b) / period;
        level[period - 1] = avg1;
        trend[period - 1] = (avg2 - avg1) / period;

        for (var i = 0; i < period; i++) {
          seasonal[i] = (seasonalType == 'additive')
              ? series[i] - avg1
              : series[i] / avg1;
          fitted[i] = series[i];
        }
        for (var t = period; t < n; t++) {
          final prevL = level[t - 1], prevT = trend[t - 1];
          if (seasonalType == 'additive') {
            level[t]    = alpha * (series[t] - seasonal[t - period]) +
                          (1 - alpha) * (prevL + prevT);
            trend[t]    = beta  * (level[t] - prevL) +
                          (1 - beta)  * prevT;
            seasonal[t] = gamma * (series[t] - prevL - prevT) +
                          (1 - gamma) * seasonal[t - period];
            fitted[t]   = level[t] + trend[t] + seasonal[t - period];
          } else {
            level[t]    = alpha * (series[t] / seasonal[t - period]) +
                          (1 - alpha) * (prevL + prevT);
            trend[t]    = beta  * (level[t] - prevL) +
                          (1 - beta)  * prevT;
            seasonal[t] = gamma * (series[t] / (prevL + prevT)) +
                          (1 - gamma) * seasonal[t - period];
            fitted[t]   = (level[t] + trend[t]) * seasonal[t - period];
          }
        }
        break;
    }

    // 4) pick columns & build matching rows
    late final List<String> cols;
    late final List<List<double>> data;

    if (method == 'ses') {
      cols = ['observed', 'level', 'fitted'];
      data = List.generate(n, (t) => [series[t], level[t], fitted[t]]);
    } else if (method == 'holt') {
      cols = ['observed', 'level', 'trend', 'fitted'];
      data = List.generate(n, (t) => [series[t], level[t], trend[t], fitted[t]]);
    } else {
      cols = ['observed', 'level', 'trend', 'seasonal', 'fitted'];
      data = List.generate(n, (t) => [
        series[t],
        level[t],
        trend[t],
        seasonal[t],
        fitted[t],
      ]);
    }

    return DataFrame(
      data,
      columns: cols,
      index: this.index,
    );
  }

  // * Helpers
  // Median computation helper
  double _median(List<double> v) {
    final a = List<double>.from(v)..sort();
    final n = a.length;
    if (n == 0) return double.nan;
    final m = n ~/ 2;
    return (n.isOdd) ? a[m] : (a[m - 1] + a[m]) / 2;
  }
}

// Add transpose method; requires rectangular matrix.
extension MatrixUtils on List<List<double>> {
  // Transpose a rectangular matrix.
  List<List<double>> transpose() {
    if (isEmpty) return <List<double>>[];
    final r = length; 
    final c = first.length;
    return List.generate(
      c,
      (j) => List.generate(r, (i) => this[i][j], growable: false),
      growable: false,
    );
  }
}

// Dot-product
double _dot(List<double> a, List<double> b) {
  var s = 0.0;
  for (var i = 0; i < a.length; i++) s += a[i] * b[i];
  return s;
}

// Covariance of a *centred* matrix (rows = obs, cols = vars)
List<List<double>> _covarianceFromMat(List<List<double>> m) {
  final n = m.length;          // observations
  final p = m.first.length;    // variables
  final cov = List.generate(
    p, (_) => List<double>.filled(p, 0.0, growable: false),
    growable: false,
  );
  for (var i = 0; i < p; i++) {
    for (var j = i; j < p; j++) {
      var s = 0.0;
      for (var k = 0; k < n; k++) s += m[k][i] * m[k][j];
      final v = s / (n - 1);           // sample cov
      cov[i][j] = cov[j][i] = v;
    }
  }
  return cov;
}

// Eigen-pair
class EigenPair {
  final double value;
  final List<double> vector;
  EigenPair(this.value, this.vector);
}

/// Computes all eigenvalues and eigenvectors of a symmetric matrix using power iteration with deflation.
/// Note: Slow, O(n³), use for ≤ 50×50 covariance/SVD work.
///
/// - Parameters:
///   - [matrix] - real symmetric square matrix (NxN).
///   - [maxIter] - maximum iterations for convergence per eigenpair (default = 1000).
///   - [tol] - convergence threshold on eigenvalue changes (default = 1e-10).
///
/// Returns a list of [EigenPair]s, each containing:
///   - `value`  — the eigenvalue  
///   - `vector` — the corresponding normalized eigenvector
List<EigenPair> computeEigenDecomposition(
  List<List<double>> matrix, {
  int maxIter = 1000,
  double tol   = 1e-10,
}) {
  final n = matrix.length;
  // mutable copy for deflation
  final A = List.generate(n, (i) => List<double>.from(matrix[i]));
  final rand = math.Random(1);               // deterministic seed
  final pairs = <EigenPair>[];

  List<double> _matVec(List<List<double>> M, List<double> v) {
    final res = List<double>.filled(M.length, 0.0);
    for (var i = 0; i < M.length; i++) {
      var s = 0.0;
      for (var j = 0; j < v.length; j++) s += M[i][j] * v[j];
      res[i] = s;
    }
    return res;
  }

  for (var k = 0; k < n; k++) {
    // 1. random start, unit length
    var v = List<double>.generate(n, (_) => rand.nextDouble());
    var nv = math.sqrt(_dot(v, v));
    for (var i = 0; i < n; i++) v[i] /= nv;

    double lambda = 0.0, lambdaPrev = 0.0;
    for (var it = 0; it < maxIter; it++) {
      final Av = _matVec(A, v);
      lambda = _dot(v, Av);                       // Rayleigh quotient
      final norm = math.sqrt(_dot(Av, Av));
      if (norm == 0) break;
      for (var i = 0; i < n; i++) v[i] = Av[i] / norm;
      if ((lambda - lambdaPrev).abs() < tol) break;
      lambdaPrev = lambda;
    }
    pairs.add(EigenPair(lambda, v));

    // 2. deflate: A -= λ v vᵀ
    for (var i = 0; i < n; i++) {
      for (var j = 0; j < n; j++) {
        A[i][j] -= lambda * v[i] * v[j];
      }
    }
  }
  return pairs;
}

class PCAModel {
  final DataFrame loadings;
  final DataFrame scores;
  PCAModel({
    required this.loadings,
    required this.scores,
  });
}

class TTestResult {
  final double t;
  final double df;
  final double pValue;
  TTestResult(this.t, this.df, this.pValue);
}

class ANOVAResult {
  final double f;
  final int dfBetween;
  final int dfWithin;
  final double pValue;
  final double etaSquared;
  ANOVAResult(this.f, this.dfBetween, this.dfWithin, this.pValue, this.etaSquared);
}

class ChiSquareResult {
  final double chi2;
  final int dof;
  final double pValue;
  final DataFrame observed;
  final DataFrame expected;
  ChiSquareResult(this.chi2, this.dof, this.pValue, this.observed, this.expected);
}

class BootstrapResult {
  final List<double> samples;
  BootstrapResult(this.samples);
  // Returns [lower, upper] percentile CI at level (e.g. 0.95).
  List<double> percentileCI(double level) {
    final sorted = List<double>.from(samples)..sort();
    final lowerIdx = ((1 - level) / 2 * sorted.length).floor();
    final upperIdx = ((1 + level) / 2 * sorted.length).ceil() - 1;
    return [sorted[lowerIdx], sorted[upperIdx]];
  }
}

// Low-level Jacobi SVD (numeric only)
class _Jacobi {
  static (_Mat U, List<double> S, _Mat Vt) svd(_Mat A,
      {int maxSweeps = 100, double tol = 1e-12}) {
    final m = A.rows, n = A.cols;
    final V = _Mat.eye(n);                     // get right vectors
    final sigma = List<double>.filled(n, 0);

    for (var sweep = 0; sweep < maxSweeps; sweep++) {
      var off = 0.0;
      for (var p = 0; p < n - 1; p++) {
        for (var q = p + 1; q < n; q++) {
          double app = 0, aqq = 0, apq = 0;
          for (var i = 0; i < m; i++) {
            final aip = A[i][p], aiq = A[i][q];
            app += aip * aip;
            aqq += aiq * aiq;
            apq += aip * aiq;
          }
          off += apq * apq;
          if (apq.abs() <= tol * math.sqrt(app * aqq)) continue;
          final tau = (aqq - app) / (2 * apq);
          final t = tau.sign /
              (tau.abs() + math.sqrt(1 + tau * tau)); //  <-- tau.sign not math.sign
          final c = 1 / math.sqrt(1 + t * t);
          final s = c * t;
          // rotate columns p and q of A
          for (var i = 0; i < m; i++) {
            final aip = A[i][p], aiq = A[i][q];
            A[i][p] = c * aip - s * aiq;
            A[i][q] = s * aip + c * aiq;
          }
          // accumulate V (V <- Rᵀ V)
          for (var i = 0; i < n; i++) {
            final vip = V[i][p], viq = V[i][q];
            V[i][p] = c * vip - s * viq;
            V[i][q] = s * vip + c * viq;
          }
        }
      }
      if (off < tol) break; // converged
    }
    // column norms to singular values; normalize columns to form U 
    final U = _Mat.zero(m, n);
    for (var j = 0; j < n; j++) {
      double norm = 0;
      for (var i = 0; i < m; i++) norm += A[i][j] * A[i][j];
      norm = math.sqrt(norm);
      sigma[j] = norm;
      if (norm == 0) continue;
      for (var i = 0; i < m; i++) U[i][j] = A[i][j] / norm;
    }
    // sort largest to smallest σ 
    final idx = List<int>.generate(n, (i) => i)
      ..sort((i, j) => sigma[j].compareTo(sigma[i]));
    final S = [for (var k in idx) sigma[k]];
    final Usorted = _Mat.fromColumns([for (var k in idx) U.col(k)], m, n);
    final Vtsorted = _Mat.fromRows([for (var k in idx) V.row(k)]);

    return (Usorted, S, Vtsorted);
  }
}

class _Mat {
  final List<List<double>> _d; // row-major
  int get rows => _d.length;
  int get cols => _d[0].length;

  _Mat(this._d);
  _Mat.zero(int r, int c)
      : _d = List.generate(r, (_) => List.filled(c, 0.0));
  _Mat.eye(int n)
      : _d = List.generate(
            n, (i) => List.generate(n, (j) => (i == j ? 1.0 : 0.0)));
  // column and row  
  List<double> col(int j) =>
      [for (var i = 0; i < rows; i++) _d[i][j]];
  List<double> row(int i) => List<double>.from(_d[i]);
  // constructors from row/col lists 
  factory _Mat.fromColumns(List<List<double>> cols, int r, int c) {
    final m = _Mat.zero(r, c);
    for (var j = 0; j < c; j++) {
      for (var i = 0; i < r; i++) m._d[i][j] = cols[j][i];
    }
    return m;
  }
  factory _Mat.fromRows(List<List<double>> rows) => _Mat(rows);
  _Mat transpose() => _Mat.fromColumns([for (var j = 0; j < cols; j++) col(j)], cols, rows);
  // matrix-vector 
  List<double> mulVec(List<double> v) {
    final res = List<double>.filled(rows, 0);
    for (var i = 0; i < rows; i++) {
      var s = 0.0;
      for (var j = 0; j < cols; j++) s += _d[i][j] * v[j];
      res[i] = s;
    }
    return res;
  }
  // index operator (row only, then manual col)
  List<double> operator [](int i) => _d[i];
}

class SVDNumeric {
  final List<List<double>> U;
  final List<double> S;
  final List<List<double>> Vt;
  const SVDNumeric(this.U, this.S, this.Vt);
}

SVDNumeric jacobiSvd(List<List<double>> A,
    {int maxSweeps = 100, double tol = 1e-12}) {
  final copied =
      List<List<double>>.from(A.map((r) => List<double>.from(r)));
  final (_Mat U, List<double> S, _Mat Vt) =
      _Jacobi.svd(_Mat(copied), maxSweeps: maxSweeps, tol: tol);
  return SVDNumeric(U._d, S, Vt._d);
}

class SVDResult {
  final DataFrame U;
  final List<double> S;
  final DataFrame Vt;
  const SVDResult({required this.U, required this.S, required this.Vt});
}

// Enum helper for interpolate method
enum InpMethod { polynomial, linear, spline , time}

///  Computes linear interpolation.
///
/// - Parameters:
///   - [x]  - x-value to interpolate at.
///   - [xs] - known x-values (must be sorted).
///   - [ys] - known y-values (same length as [xs]).
double linearInterpolate({
  required double x,
  required List<double> xs,
  required List<double> ys,
}) {
  final m = xs.length;
  if (m < 2) {
    throw ArgumentError('Need at least 2 points for linear interpolation');
  }
  // pick segment index j so xs[j] ≤ x ≤ xs[j+1]
  int j;
  if (x <= xs.first) {
    j = 0;
  } else if (x >= xs.last) {
    j = m - 2;
  } else {
    j = 0;
    for (var k = 0; k < m - 1; k++) {
      if (x >= xs[k] && x <= xs[k + 1]) {
        j = k;
        break;
      }
    }
  }
  final x0 = xs[j], x1 = xs[j + 1];
  final y0 = ys[j], y1 = ys[j + 1];
  final t = (x - x0) / (x1 - x0);
  return y0 + (y1 - y0) * t;
}
/// Computes Lagrange polynomial interpolation.
///
/// - Parameters:
///   - [x]  - x-value to interpolate at.
///   - [xs] - known x-values (must be distinct).
///   - [ys] - known y-values (same length as [xs]).
double lagrangeInterpolate({
  required double x,
  required List<double> xs,
  required List<double> ys,
}) {
  final n = xs.length;
  var y = 0.0;
  for (var i = 0; i < n; i++) {
    var term = ys[i];
    for (var j = 0; j < n; j++) {
      if (j == i) continue;
      term *= (x - xs[j]) / (xs[i] - xs[j]);
    }
    y += term;
  }
  return y;
}
/// Computes natural cubic spline interpolation.
///
/// - Parameters:
///   - [x]  - x-value to interpolate at.
///   - [xs] - known x-values (must be sorted and distinct).
///   - [ys] - known y-values (same length as [xs]).
double splineInterpolate({
  required double x,
  required List<double> xs,
  required List<double> ys,
}) {
  final m = xs.length;
  if (m < 2) {
    throw ArgumentError('Need at least 2 points for spline interpolation');
  }

  // 1. Build coefficient arrays
  final h = List<double>.generate(m - 1, (i) => xs[i + 1] - xs[i]);
  final alpha = List<double>.filled(m, 0.0);
  for (var i = 1; i < m - 1; i++) {
    alpha[i] = 3 * (ys[i + 1] - ys[i]) / h[i]
             - 3 * (ys[i] - ys[i - 1]) / h[i - 1];
  }

  final l = List<double>.filled(m, 1.0);
  final mu = List<double>.filled(m, 0.0);
  final z = List<double>.filled(m, 0.0);

  for (var i = 1; i < m - 1; i++) {
    l[i] = 2 * (xs[i + 1] - xs[i - 1]) - h[i - 1] * mu[i - 1];
    mu[i] = h[i] / l[i];
    z[i] = (alpha[i] - h[i - 1] * z[i - 1]) / l[i];
  }

  final c = List<double>.filled(m, 0.0);
  final b = List<double>.filled(m - 1, 0.0);
  final d = List<double>.filled(m - 1, 0.0);

  for (var j = m - 2; j >= 0; j--) {
    c[j] = z[j] - mu[j] * c[j + 1];
    b[j] = (ys[j + 1] - ys[j]) / h[j]
         - h[j] * (c[j + 1] + 2 * c[j]) / 3;
    d[j] = (c[j + 1] - c[j]) / (3 * h[j]);
  }

  // 2. Find the right segment j so xs[j] ≤ x ≤ xs[j+1]
  var j = 0;
  for (var k = 0; k < m - 1; k++) {
    if (x >= xs[k] && x <= xs[k + 1]) {
      j = k;
      break;
    }
  }

  final dx = x - xs[j];
  // 3. Evaluate the cubic polynomial
  return ys[j]
       + b[j] * dx
       + c[j] * dx * dx
       + d[j] * dx * dx * dx;
}

// * p-value helpers

double _logGamma(double x) {
  // Lanczos approximation for log gamma
  const coeffs = [
    76.18009172947146,   -86.50532032941677,
    24.01409824083091,   -1.231739572450155,
    0.1208650973866179e-2, -0.5395239384953e-5
  ];
  var y = x;
  var tmp = x + 5.5;
  tmp -= (x + 0.5) * math.log(tmp);
  var ser = 1.000000000190015;
  for (int j = 0; j < 6; j++) {
    y += 1;
    ser += coeffs[j] / y;
  }
  return -tmp + math.log(2.5066282746310005 * ser / x);
}

double _betaIncReg(double a, double b, double x) {
  // Regularized incomplete beta function using continued fraction expansion
  const maxIter = 100;
  const epsilon = 1e-15;

  double cf(double a, double b, double x) {
    double qab = a + b;
    double qap = a + 1;
    double qam = a - 1;
    double c = 1.0;
    double d = 1.0 - qab * x / qap;
    if (d.abs() < epsilon) d = epsilon;
    d = 1.0 / d;
    double h = d;

    for (int m = 1; m <= maxIter; m++) {
      final m2 = 2 * m;
      var aa = m * (b - m) * x / ((qam + m2) * (a + m2));
      d = 1.0 + aa * d;
      if (d.abs() < epsilon) d = epsilon;
      c = 1.0 + aa / c;
      if (c.abs() < epsilon) c = epsilon;
      d = 1.0 / d;
      h *= d * c;

      aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2));
      d = 1.0 + aa * d;
      if (d.abs() < epsilon) d = epsilon;
      c = 1.0 + aa / c;
      if (c.abs() < epsilon) c = epsilon;
      d = 1.0 / d;
      final del = d * c;
      h *= del;
      if ((del - 1.0).abs() < epsilon) break;
    }

    return h;
  }

  final bt = (x == 0.0 || x == 1.0)
      ? 0.0
      : math.exp(_logGamma(a + b) - _logGamma(a) - _logGamma(b) +
          a * math.log(x) + b * math.log(1.0 - x));

  if (x < (a + 1.0) / (a + b + 2.0)) {
    return bt * cf(a, b, x) / a;
  } else {
    return 1.0 - bt * cf(b, a, 1.0 - x) / b;
  }
}

double _tCDF(double t, double df) {
  final x = df / (df + t * t);
  return 1.0 - 0.5 * _betaIncReg(0.5 * df, 0.5, x);
}

double _gammaIncReg(double s, double x) {
  // Regularized lower incomplete gamma function P(s, x)
  const maxIter = 100;
  const epsilon = 1e-14;
  if (x < 0 || s <= 0) return double.nan;
  if (x == 0) return 0.0;

  double sum = 1.0 / s;
  double term = sum;
  for (int n = 1; n < maxIter; n++) {
    term *= x / (s + n);
    sum += term;
    if (term < sum * epsilon) break;
  }
  return sum * math.exp(-x + s * math.log(x) - _logGamma(s));
}

double _chi2CDF(double x, double df) {
  return _gammaIncReg(df / 2.0, x / 2.0);
}