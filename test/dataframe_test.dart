import 'package:pdataframe/pdataframe.dart';
import 'package:test/test.dart';

void main() {
  group('[] operator tests', (){
    final df = DataFrame({'a':[1,2,3], 0:[4,5,6], true:[7,8,9]}, );
    test('Test that [] operator returns a column of data', (){
      expect(df['a'], equals([1,2,3])); // Returns column 'a'
      expect(df[0], equals([4,5,6])); // Returns column 0
      // Returns a DataFrame containing only columns 'a' and 0 when argument is a List of Lists
      var df2 = df[['a',0]];
      expect(df2.columns, equals(['a',0])); 
      expect(df2.values, equals([[1,2,3],[4,5,6]]));
    });
    test('Test that []= operator ', (){
      // Replace an int value with a double
      df['a'] = [1,2.2,3]; // Replace a column 'a' int value with a double
      expect(df['a'], equals([1.0,2.2,3.0])); // Check that column int values are converted to double
      expect(df['a'].runtimeType, equals(List<double>));  // Check that List<int> is now List<double>
      expect(df.dtypes, equals([double, int, int]));  // Check that dtypes correctly shows double
      // Replacing int column with String
      df[0] = ['a', 'bee', 'cee'];      
      expect(df[0], equals(['a', 'bee', 'cee'])); // Check that column 0 values have been replaced
      expect(df[0].runtimeType, equals(List<String>));  // Check that List<int> is now List<String>
      expect(df.dtypes, equals([double, String, int]));  // Check that dtypes correctly shows String
      // Replacing int column with mixed type (Object)
      df[true] = ['a', true, 1];
      expect(df[true], equals(['a', true, 1])); // Check that column 0 values have been replaced
      expect(df[true].runtimeType, equals(List<Object>));  // Check that List<int> is now List<String>
      expect(df.dtypes, equals([double, String, Object]));  // Check that dtypes correctly shows String
    });
  });
  group('DataFrame List input tests', () {
    final df = DataFrame([[1, 4, 7], [2, 'hi'], [3,6,9]], index: ['three', 2, 'four'], columns: ['zero', 1, 'two']);
    test('Data and index/column labels are correctly stored', () {
      expect(df['zero'], equals([1,2,3])); 
      expect(df.index, equals(['three', 2, 'four']));  // Check for index labels are correctly applied
      expect(df.columns, equals(['zero', 1, 'two']));  // Check for column labels are correctly applied
    });
    test('For row entries with missing data, Nan is inserted', () { // (pd)
      expect(df['two'][1].isNaN, isTrue);
    });
    test('dtypes - Test for Type being stored correctly', () { 
      expect(df.dtypes, <Type>[int,Object,double]);
    });
  });
  group('DataFrame Map input tests', () {
    final mapInput = {'A': [1, 2, 3],'B': [4, 5, 6],'C': [7, 8, 9]};
    test('Data and index/column labels are correctly stored', () {
      final df = DataFrame(mapInput);
      expect(df['A'], equals([1, 2, 3]));  // Check that column 'A' is correctly stored
      expect(df['B'], equals([4, 5, 6]));  // Check that column 'B' is correctly stored
      expect(df.index, equals([0, 1, 2]));  // Auto-generated index
      expect(df.columns, equals(['A', 'B', 'C']));  // Check that column names match Map keys
    });
    test('Map argument with columns parameter', () {
      // Test DataFrame created using an empty Map with the columns argument
      var dfm = DataFrame({}, columns: ['a', 'b']);
      expect(dfm.columns, equals(['a', 'b'])); // The DataFrame is initialized with column labels 'a' and 'b'
      expect(dfm.values, equals([[], []])); // The DataFrame contains no data; columns are empty lists

      // Test DataFrame with an input Map containing data and a matching columns argument
      dfm = DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]}, columns: ['a', 'b']);
      expect(dfm.columns, equals(['a', 'b'])); // The DataFrame contains the specified column labels 'a' and 'b'
      expect(dfm.values, equals([[1, 2, 3], [4, 5, 6]])); // Data matches the input Map values for 'a' and 'b'

      // Test DataFrame with an input Map that partially matches the column names in the columns argument
      dfm = DataFrame({'c': [7, 8, 9], 'b': [1, 2, 3]}, columns: ['a', 'b']);
      expect(dfm.columns, equals(['a', 'b'])); // The DataFrame only includes the specified columns 'a' and 'b'; 'c' and its data are ignored
      expect(dfm.values[0].every((element) => (element as double).isNaN), isTrue); // The column for 'a' is filled with NaN, as no matching data is provided in the Map
      expect(dfm.values[1], equals([1, 2, 3])); // The column for 'b' contains the corresponding data from the input Map
    });
    test('Throws error if column values have inconsistent lengths', () {
      final invalidMap = {
        'A': [1, 2],
        'B': [3, 4, 5]
      };
      expect(() => DataFrame(invalidMap), throwsArgumentError);  // Check for error
    });
    test('Supports primitive values in the Map', () {
      final primitiveMap = {'A': 1,'B': 2,'C': 3
      };
      final df = DataFrame(primitiveMap);
      expect(df['A'], equals([1]));  // Single element wrapped in a List
      expect(df['B'], equals([2]));
      expect(df['C'], equals([3]));
      expect(df.index, equals([0]));  // Auto-generated index for single row
    });
    test('Index provided in constructor matches the number of rows', () {
      final mapWithIndex = {
        'A': [1, 2, 3],
        'B': [4, 5, 6],
      };
      final df = DataFrame(mapWithIndex, index: ['row1', 'row2', 'row3']);
      expect(df.index, equals(['row1', 'row2', 'row3']));  // Custom index check
    });
    test('Throws error when index length does not match row length', () {
      final mapWithInvalidIndex = {'A': [1, 2, 3],'B': [4, 5, 6]};
      expect(() => DataFrame(mapWithInvalidIndex, index: ['row1', 'row2']), throwsArgumentError);  // Check for error
    });
  });
  group('drop() tests', () {
    late DataFrame df;
    setUp((){df = DataFrame({'A': [1, 2, 3, 4], 'B': [5, 6, 7, 8],'C': [9, 10, 11, 12]});});
    test('Drop a single row by label (axis=0)', () {
      final droppedDf = df.drop(1, axis: 0);
      expect(droppedDf.index, equals([0, 2, 3]));  // Check that row 1 is removed
      expect(droppedDf['A'], equals([1, 3, 4]));   // Remaining values in column 'A'
      expect(droppedDf['B'], equals([5, 7, 8]));   // Remaining values in column 'B'
    });
    test('Drop a single column by label (axis=1)', () {
      final droppedDf = df.drop('B', axis: 1);
      expect(droppedDf.columns, equals(['A', 'C']));  // Check that column 'B' is removed
      expect(droppedDf['A'], equals([1, 2, 3, 4]));   // Remaining values in column 'A'
      expect(droppedDf['C'], equals([9, 10, 11, 12])); // Remaining values in column 'C'
    });
    test('Drop a row with inplace=true (axis=0)', () {
      df.drop(2, axis: 0, inplace: true);
      expect(df.index, equals([0, 1, 3]));   // Check that row 2 is removed inplace
      expect(df['A'], equals([1, 2, 4]));    // Check remaining rows in column 'A'
    });
    test('Drop a column with inplace=true (axis=1)', () {
      df.drop('C', axis: 1, inplace: true);
      expect(df.columns, equals(['A', 'B']));  // Check that column 'C' is removed inplace
      expect(df['A'], equals([1, 2, 3, 4]));   // Check that data in other column remains the same
    });
    test('Drop a specific occurrence of a row (select)', () {
      final dfWithDuplicates = DataFrame({
        'A': [1, 2, 2, 4],
        'B': [5, 6, 6, 8],
      }, index:[0,'hi','hi',2]);
      final droppedDf = dfWithDuplicates.drop('hi', axis: 0, select: 2);
      expect(droppedDf.index, equals([0, 'hi', 2]));  // Only the second occurrence of '2' is removed
      expect(droppedDf['A'], equals([1, 2, 4]));   // Remaining rows in 'A'
    });
    test('Throws error if select exceeds occurrences', () {
      final dfWithDuplicates = DataFrame({'A': [1, 2, 2, 4],'B': [5, 6, 6, 8],});
      expect(() => dfWithDuplicates.drop(2, axis: 0, select: 3), throwsArgumentError);  // Exceeds occurrences
    });
    test('Throws error for invalid axis value', () {
      expect(() => df.drop('A', axis: 2), throwsArgumentError);  // Invalid axis
    });
    test('Throws error if input not found', () {
      expect(() => df.drop('D', axis: 1), throwsStateError);  // Non-existent column
    });
  });
  group('rename() tests', () {
    final df = DataFrame({'A': [1, 2, 3],'B': [4, 5, 6],'C': [7, 8, 9]}, index: ['row1', 'row2', 'row3']);
    test('Rename rows without inplace (index)', () {
      final renamedDf = df.rename(index: {'row1': 'newRow1'});
      expect(renamedDf.index, equals(['newRow1', 'row2', 'row3'])); // Check renamed index
      expect(renamedDf['A'], equals([1, 2, 3])); // Check data remains unchanged
    });
    test('Rename columns without inplace (columns)', () {
      final renamedDf = df.rename(columns: {'A': 'X', 'B': 'Y'});
      expect(renamedDf.columns, equals(['X', 'Y', 'C'])); // Check renamed columns
      expect(renamedDf['X'], equals([1, 2, 3])); // Check data remains unchanged
      expect(renamedDf['Y'], equals([4, 5, 6]));
    });
    test('Rename rows inplace', () {
      df.rename(index: {'row1': 'newRow1'}, inplace: true);
      expect(df.index, equals(['newRow1', 'row2', 'row3'])); // Check renamed index with inplace=true
    });
    test('Rename columns inplace', () {
      df.rename(columns: {'A': 'X', 'B': 'Y'}, inplace: true);
      expect(df.columns, equals(['X', 'Y', 'C'])); // Check renamed columns with inplace=true
      expect(df['X'], equals([1, 2, 3]));
      expect(df['Y'], equals([4, 5, 6]));
    });
    test('Throws error when no index or columns provided', () {
      expect(() => df.rename(), throwsArgumentError); // No index or columns to rename
    });
    test('Throws error when renaming non-existent row', () {
      expect(() => df.rename(index: {'nonExistentRow': 'newRow'}), throwsArgumentError); // Invalid row name
    });
    test('Throws error when renaming non-existent column', () {
      expect(() => df.rename(columns: {'D': 'X'}), throwsArgumentError); // Invalid column name
    });
    test('Rename a specific occurrence of a row using atIndex', () {
      final dfWithDuplicates = DataFrame({
        'A': [1, 2, 2, 4],
        'B': [5, 6, 6, 8]
      }, index: ['row1', 'row2', 'row2', 'row4']);
      final renamedDf = dfWithDuplicates.rename(index: {'row2': 'newRow2'}, atIndex: 2);
      expect(renamedDf.index, equals(['row1', 'row2', 'newRow2', 'row4'])); // Only second occurrence renamed
    });
    test('Rename a specific occurrence of a column using atCol', () {
      final dfWithDuplicateCols = DataFrame([[1, 2],[3, 4]], columns: ['A', 'A']);
      final renamedDf = dfWithDuplicateCols.rename(columns: {'A': 'newA'}, atCol: 2);
      expect(renamedDf.columns, equals(['A', 'newA'])); // Only second occurrence renamed
    });
  });
  group('reindex() tests', () {
    late DataFrame df = DataFrame({'A': [1, 2, 3, 4],'B': [5, 6, 7, 8],'C': [9, 10, 11, 12]}, index: ['row1', 'row2', 'row3', 'row4']);
    setUp((){df = DataFrame({'A': [1, 2, 3, 4],'B': [5, 6, 7, 8],'C': [9, 10, 11, 12]}, index: ['row1', 'row2', 'row3', 'row4']);});
    test('Reindex rows by List', () {
      final reindexedDf = df.reindex(['row3', 'row1', 'row2', 'row4']);
      expect(reindexedDf.index, equals(['row3', 'row1', 'row2', 'row4'])); // Check reordered rows
      expect(reindexedDf['A'], equals([3, 1, 2, 4])); // Data is reordered
      expect(reindexedDf['B'], equals([7, 5, 6, 8]));
    });
    test('Reindex rows by List, inplace is true', () {
      var r = df.reindex(['row4', 'row1', 'row2', 'row3'], inplace: true);
      expect(df.index, equals(['row4', 'row1', 'row2', 'row3'])); // Reordered inplace
      expect(df['A'], equals([4, 1, 2, 3])); // Data is reordered
      expect(r, equals(null));  // When inplace is true, void function (returns null)
    });
    test('Reindex by moving a row with Map input', () {
      final reindexedDf = df.reindex({'row1': 'row4'});
      expect(reindexedDf.index, equals(['row2', 'row3', 'row4', 'row1'])); // Moved 'row1' to 'row4' position
      expect(reindexedDf['A'], equals([2, 3, 4, 1])); // Data in 'A' adjusted accordingly
    });
    test('Reindex by moving a row with Map input and inplace is true', () {
      var r = df.reindex({'row1': 'row4'}, inplace:true);
      expect(df.index, equals(['row2', 'row3', 'row4', 'row1'])); // Moved 'row1' to 'row4' position
      expect(df['A'], equals([2, 3, 4, 1])); // Data in 'A' adjusted accordingly
      expect(r, equals(null));  // When inplace is true, void function (returns null)
    });
    test('Throws error when reindexing with invalid row names in list', () {
      expect(() => df.reindex(['invalidRow', 'row1', 'row2', 'row3']), throwsArgumentError); // Invalid row name
    });
    test('Throws error when reindexing with invalid row names in Map', () {
      expect(() => df.reindex({'invalidRow': 'row1'}), throwsNoSuchMethodError); // Invalid row name in Map
    });
    test('Throws error for invalid input type', () {
      expect(() => df.reindex(123), throwsArgumentError); // Invalid input type (not Map or List)
    });
    test('Reindex by moving a row with select (Map input)', () {
      final dfWithDuplicates = DataFrame({'A': [1, 2, 5, 4],'B': [5, 6, 7, 8]}, index: ['row1', 'row2', 'row2', 'row4']);
      final reindexedDf = dfWithDuplicates.reindex({'row2': 'row4'}, select: {2: 1});
      expect(reindexedDf.index, equals(['row1', 'row2', 'row4', 'row2'])); // Moved one occurrence of 'row2'
    });
    test('Reindex rows with list but different order of occurrence', () {
      final dfWithDuplicates = DataFrame({'A': [1, 2, 2, 4],'B': [5, 6, 6, 8]}, index: ['row1', 'row2', 'row2', 'row4']);
      final reindexedDf = dfWithDuplicates.reindex(['row2', 'row1', 'row4', 'row2']);
      expect(reindexedDf.index, equals(['row2', 'row1', 'row4', 'row2'])); // Data reordered by row name occurrence
      expect(reindexedDf['A'], equals([2, 1, 4, 2])); // Data remains consistent
    });
  });
  group('sort() tests', () {
    final df = DataFrame({'A': [3, 1, 4, 2],'B': ['banana', 'Apple', 'pear', 'apple']});
    test('Sort column A in ascending order (default)', () {
      final sortedDf = df.sort('A', inplace: false);
      expect(sortedDf['A'], equals([1, 2, 3, 4])); // Column A sorted
      expect(sortedDf['B'], equals(['Apple', 'apple', 'banana', 'pear'])); // Column B reordered
    });
    test('Sort column A in descending order', () {
      final sortedDf = df.sort('A', inplace: false, ascending: false);
      expect(sortedDf['A'], equals([4, 3, 2, 1])); // Column A sorted in descending order
      expect(sortedDf['B'], equals(['pear', 'banana', 'apple', 'Apple'])); // Column B reordered
    });
    test('Sort column B alphabetically (case-insensitive)', () {
      final sortedDf = df.sort('B', inplace: false, ); //todo edit
      expect(sortedDf['B'], equals(['Apple', 'apple', 'banana', 'pear'])); // Column B sorted alphabetically
      expect(sortedDf['A'], equals([1, 2, 3, 4])); // Column A reordered accordingly
    });
    test('Sort column A with NaN first', () {
      final dfWithNulls = DataFrame({'A': [3, double.nan, 4, 1],'B': ['banana', 'Apple', 'pear', 'apple']});
      final sortedDf = dfWithNulls.sort('A', inplace: false, nullIsFirst: true); 
      expect(sortedDf.index, equals([1,3,0,2])); // Rows sorted with row index 1 first
    });
    test('Sort column A with nulls last', () {
      final dfWithNulls = DataFrame({'A': [3, double.nan, 4, 1],'B': ['banana', 'Apple', 'pear', 'apple']});
      final sortedDf = dfWithNulls.sort('A', inplace: false, nullIsFirst: false);
      expect(sortedDf.index, equals([3,0,2,1])); // Rows sorted with row index 1 last
    });
    test('Sort column B with custom comparator (ASCII case-insensitive)', () {
      int customComparator(a, b) {
        if (a is String && b is String) {
          return a.toLowerCase().compareTo(b.toLowerCase());
        }
        return 0;
      }
      final sortedDf = df.sort('B', inplace: false, comparator: customComparator);
      expect(sortedDf['B'], equals(['Apple', 'apple', 'banana', 'pear'])); // Sorted alphabetically, case-insensitive
      expect(sortedDf['A'], equals([1, 2, 3, 4])); // Column A reordered accordingly
    });
    test('Sort inplace', () {
      df.sort('A', inplace: true);
      expect(df['A'], equals([1, 2, 3, 4])); // Column A sorted
      expect(df['B'], equals(['Apple', 'apple', 'banana', 'pear'])); // Column B reordered
    });
  });
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
  group('reset_index() tests', () {
    late DataFrame df;
    setUp(() {
      df = DataFrame(
        [[1, 4, 7], [2, 5, 8], [3, 6, 9]],
        index: ['row1', 'row2', 'row3'],
        columns: ['A', 'B', 'C']
      );
    });
    test('Test reset_index() with inplace = true', () {
      df.reset_index(inplace: true);
      expect(df.index, equals([0, 1, 2]));  // Default index should be applied
      expect(df['A'], equals([1, 2, 3]));  // Check that data remains unchanged
    });
    test('Test reset_index() with inplace = false', () {
      final newDf = df.reset_index(inplace: false);
      expect(newDf.index, equals([0, 1, 2]));  // Default index should be applied
      expect(newDf['A'], equals([1, 2, 3]));  // Check that data remains unchanged
      expect(df.index, equals(['row1', 'row2', 'row3'])); // Ensure the original DataFrame's index remains unchanged
    });
  });
  group('astype() tests', () {
    final df = DataFrame([]); 
    test('Convert list elements to double', () {
      final originalList = [1, 2, 3];
      final convertedList = df.astype(double, originalList);
      expect(convertedList, equals([1.0, 2.0, 3.0])); 
      expect(convertedList.every((element) => element is double), isTrue); 
    });
    test('Convert list of strings to double', () {
      final originalList = ['1.5', '2.3', '3.7'];
      final convertedList = df.astype(double, originalList.map(double.parse).toList());
      expect(convertedList, equals([1.5, 2.3, 3.7]));
      expect(convertedList.every((element) => element is double), isTrue);
    });
    test('No conversion for list of non-double type', () {
      final originalList = ['a', 'b', 'c'];
      final convertedList = df.astype(String, originalList);
      // Ensure the list remains as it is with strings
      expect(convertedList, equals(originalList));
      expect(convertedList.every((element) => element is String), isTrue);
    });
    test('Handle mixed data types in the list for double conversion', () {
      final originalList = [1, 2.5, 3];
      final convertedList = df.astype(double, originalList);
      expect(convertedList, equals([1.0, 2.5, 3.0]));  // All elements as doubles
      expect(convertedList.every((element) => element is double), isTrue);
    });

    test('Empty list conversion', () {
      final originalList = [];
      final convertedList = df.astype(double, originalList);
      expect(convertedList, isEmpty);
    });
  });
  group('iloc() tests', () {
    late DataFrame df;
    setUp(() {
      df = DataFrame([[1, 4, 7],[2, 5, 8],[3, 6, 9]],index: ['row1', 'row2', 'row3'],columns: ['A', 'B', 'C']);
    });
    test('Retrieve a single row by integer index', () {
      final row = df.iloc(row: 1);
      expect(row, equals([2, 5, 8]));  // Retrieves the second row
    });
    test('Retrieve a specific cell by row and column indices', () {
      final cell = df.iloc(row: 1, col: 2);
      expect(cell, equals(8));  // Retrieves the cell in the second row, third column
    });
    test('Edit a single cell by row and column indices', () {
      df.iloc(row: 1, col: 2, edit: 10);
      expect(df.iloc(row: 1, col: 2), equals(10));  // Confirms the cell was updated
    });
    test('Edit entire row with a single value', () {
      df.iloc(row: 1, edit: 99);
      expect(df.iloc(row: 1), equals([99, 99, 99]));  // The entire row is set to 99
    });
    test('Edit entire row with a list of values', () {
      df.iloc(row: 1, edit: [20, 25, 30]);
      expect(df.iloc(row: 1), equals([20, 25, 30]));  // Row updated with the provided values
    });
    test('Retrieve multiple rows by list of indices', () {
      final resultDf = df.iloc(row: [0, 2]);
      expect(resultDf.index, equals(['row1', 'row3']));
      expect(resultDf['A'], equals([1, 3]));  // Only rows at indices 0 and 2 are selected
    });
    test('Retrieve a submatrix with specified row and column ranges', () {
      final resultDf = df.iloc(row:{1: 3}, col: {1: null});
      expect(resultDf.index, equals(['row2', 'row3']));  // Rows 1 to 2 
      expect(resultDf.columns, equals(['B', 'C']));  // Columns starting from index 1
      expect(resultDf['B'], equals([5, 6]));  // Check the specific values in the submatrix
    });
    test('Retrieve sublist with invalid column range', () {
      expect(() => df.iloc(row: {0: 2}, col: {1: 3, 2:3}), throwsArgumentError); // Throws error when column range is invalid
    });
    test('Edit entire row with a list that doesn’t match column count', () {
      expect(() => df.iloc(row: 1, edit: [20, 25]), throwsArgumentError); // Mismatched edit list length
    });
  });
  group('loc() tests', () {
    late DataFrame df;
    late DataFrame df2;

    setUp(() {
      df = DataFrame([[1, 4, 7],[2, 5, 8],[3, 6, 9]],index: ['1', 2, 'row3'],columns: ['A', 'B', 'C']);
      df2 = DataFrame([[1,4,7,2,6],[2,5,8,3,5,],[3,6,9,1,2],[7,2,5,6,4]],index: [0, 2, 2,'hi'],columns: ['A', 'A', 'B','C','D']);
    });
    test('Retrieve a single row by entering in the integer index as String', () {
      final row = df.loc(row:'1');
      expect(row, equals([1, 4, 7]));  // Retrieves the first row
    });
    test('Retrieve a single row by name', () {
      final row = df.loc(row:2);
      expect(row, equals([2, 5, 8]));  // Retrieves the second row
    });
    test('Retrieves a row by integer index as string', () {
      final row = df.loc(row:'2');
      expect(row, equals([2, 5, 8]));  // Retrieves the second row using string index "2"
    });
    test('Retrieve a row range using a Map', () {
      final rangeDf = df.loc(row:{'1': 'row3'});
      expect(rangeDf.index, equals(['1', 2]));  // Only rows between row1 and row2
      expect(rangeDf['A'], equals([1, 2]));  // Column 'A' values for the selected rows
    });
    test('Retrieve row and column range with Maps', () {
      final rangeDf = df.loc(row:{'1': 2}, col:{'A': 'B'});
      expect(rangeDf.index, equals(['1']));  // Only row 'row1'
      expect(rangeDf.columns, equals(['A', 'B']));  // Columns 'A' and 'B'
      expect(rangeDf['A'], equals([1]));  // Check 'A' values in the sub-DataFrame
      expect(rangeDf['B'], equals([4]));  // Check 'B' values in the sub-DataFrame
      final rangeDf2 = df2.loc(row:0, col:{null:'B'}); // col Map input with null key
      expect(rangeDf2, equals([1,4,7])); // All values of row 0 up to and including col B
      final rangeDf3 = df2.loc(row:0, col:{'B':null}); // col Map input with null value
      expect(rangeDf3, equals([7,2,6])); // row 0 values starting from column B to the end
      final rangeDf4 = df2.loc(row:2); // Identical row index name '2'
      expect(rangeDf4.values, equals([[2,3],[5,6],[8,9],[3,1],[5,2]]));  // Returns DataFrame with those rows
    });
    test('Retrieve row range with non-existent row throws error', () {
      expect(() => df.loc(row:{'row1': 'row4'}), throwsA(isA<NoSuchMethodError>()));  // row4 does not exist
    });
    test('Retrieve column range with non-existent column throws error', () {
      expect(() => df.loc(row:'row1', col:{'A': 'D'}), throwsA(isA<NoSuchMethodError>()));  // Column 'D' does not exist
    });
    test('Invalid row argument type throws error', () {
      expect(() => df.loc(row:null), throwsArgumentError);  // Row argument is null
    });
    test('Retrieve a submatrix with default columns', () {
      final subDf = df.loc(row:{'1':'row3'});
      expect(subDf.index, equals(['1', 2]));  // Expected rows
      expect(subDf.columns, equals(['A', 'B', 'C']));  // All columns by default
      expect(subDf['A'], equals([1, 2]));  // Verify values in submatrix
    });
    test('Retrieve a row with numeric row name', () {
      df = DataFrame([[10, 20, 30],[40, 50, 60]], index: [0, 1],columns: ['X', 'Y', 'Z']);
      final row = df.loc(row:0);
      expect(row, equals([10, 20, 30]));  // Retrieves row with name '0'
    });
    test('Set a value with edit parameter; single row', () {  
      df.loc(row:'1', edit: 11 ); // Single row, no col argument. 
      expect(df.loc(row:'1'), equals([11,11,11])); //All values in row 0 assigned value of 11
      
      df.loc(row:'1', col:'A', edit: 22 ); // Single row, single col, 
      expect(df.loc(row:'1'), equals([22,11,11])); // Row '1', Column 'A' set to 22  

      df.loc(row:2, col:{'A':'B'}, edit: 53 ); // Single row, multi column
      expect(df.loc(row:2), equals([53,53,8])); // Row 2, column A to B (inc B) values changed to 53

      df2.loc(row:2, col:'A', edit: 22 );  // Single row, single column; index names are non-unique
      final mdf = df2.loc(row:2); // Return a DataFrame with only row 2's
      expect(mdf.values, equals([[22,22],[22,22],[8,9],[3,1],[5,2]])); // Only row 2 and column A elements changed to 22
    });
    test('Set a value with edit parameter; multi row', () {  
      df.loc(row:{'1':'row3'},edit: 33);  // Multi row, no col argument
      expect(df.values, equals([[33,33,3],[33,33,6],[33,33,9]])); // Row 1 and 2 has all values changed to 33
      
      df.loc(row:{'1':'row3'}, col: 'A', edit: 47);  // Multi row, single col argument
      expect(df.values, equals([[47,47,3],[33,33,6],[33,33,9]])); // Row 1 and 2 has column A values changed to 47

      df.loc(row:{'1':'row3'}, col: {'A':'B'}, edit: 88);  // Multi row, multi col argument
      expect(df.values, equals([[88,88,3],[88,88,6],[33,33,9]])); // Row 1 and 2 have column A and B changed to 88
    });
    test('re-order DataFrame rows using List input', () {  
      DataFrame newOrder = df2.loc(row:[2,2,0,'hi']);
      expect(newOrder.index, equals([2,2,0,'hi'])); // The new order of row indices
      expect(newOrder.values, equals([[2, 3, 1, 7], [5, 6, 4, 2], [8, 9, 7, 5], [3, 1, 2, 6], [5, 2, 6, 4]])); // Data aligns with new row order
    });
  });
  group('concat() tests', () {
    final df1 = DataFrame([[1, 2],[3, 4]], index: ['row1', 'row2'], columns: ['A', 'B']);
    final df2 = DataFrame([[5, 6],[7, 8]], index: ['row2', 'row3'], columns: ['A', 'B']);
    final df3 = DataFrame([[9, 10],[11, 12]], index: ['row3', 'row4'], columns: ['A', 'B']);
    test('Concatenate vertically with axis=0 and outer join', () {
      final result = concat([df1, df2], axis: 0, join: 'outer');
      expect(result.index, equals(['row1', 'row2', 'row2', 'row3']));  // Combined row indices
      expect(result.columns, equals(['A', 'B']));  // Columns remain the same
      expect(result['A'], equals([1, 3, 5, 7]));  // Values in column 'A'
      expect(result['B'], equals([2, 4, 6, 8]));  // Values in column 'B'
    });
    test('Concatenate vertically with axis=0 and inner join (non-matching columns); should only combine indices', () {
      final dfMismatch = DataFrame([[13, 14]], index: ['row7'], columns: ['C', 'D']);
      var result = concat([df1, dfMismatch], axis: 0, join: 'inner');
      expect(result.index, equals(['row1','row2','row7']));
      expect(result.columns, equals([]));
      expect(result.values, equals([]));
    });
    test('Concatenate horizontally with axis=1 and inner join', () {
      final result = concat([df1, df2], axis: 1, join: 'inner');     
      expect(result.index, equals(['row2']));  // Only common rows
      expect(result.columns, equals(['A', 'B', 'A', 'B']));  // Duplicate column names in result
      expect(result['A'], equals([[3],[5]]));  // Values in both 'A' columns
      expect(result['B'], equals([[4],[6]]));  // Values in both 'B' columns
    });
    test('Concatenate horizontally with axis=1 and outer join', () {
      final dfExtra = DataFrame([[9, 10],[11, 12]], index: ['row3', 'row2'], columns: ['C', 'D']);
      final result = concat([df1, dfExtra], axis: 1, join: 'outer');
      expect(result.index, equals(['row1', 'row2', 'row3']));  // Includes all rows
      expect(result.columns, equals(['A', 'B', 'C', 'D']));  // Combined columns
      expect(result['A'][0], equals(1));  // Existing row1 value for 'A'
      expect(result['A'][1], equals(3));  // Existing row2 value for 'A'
      expect(result['A'][2].isNaN, isTrue); // New row value for 'A' should be NaN, since new columns do not match
      expect(result.loc(row:'row2'), equals([3.0,4.0,11.0,12.0]));
      expect(result['D'][0].isNaN, isTrue);  // Column D with NaN as first value
      expect(result['D'][1], equals(12));  // Column D with original first value
      expect(result['D'][2], equals(10));  // Column D with original second value
    });
    test('Concatenate multiple DataFrames with axis=0 and ignore_index=true', () {
      final result = concat([df1, df2, df3], axis: 0, ignore_index: true);
      expect(result.index, equals([0, 1, 2, 3, 4, 5]));  // New index generated
      expect(result.columns, equals(['A', 'B']));  // Columns remain the same
      expect(result['A'], equals([1, 3, 5, 7, 9, 11]));  // Values in column 'A'
      expect(result['B'], equals([2, 4, 6, 8, 10, 12]));  // Values in column 'B'
    });
  });
  group('append() tests', () {
    late DataFrame df;
    setUp((){ 
      df = DataFrame([[1, 2],[3, 4]]);
    });
    test('Append List of Lists without inplace', () {
      final newRows = [[5, 6], [7, 8]];
      final appendedDf = df.append(newRows, ignore_index: true);
      expect(appendedDf[0], equals([1, 3, 5, 7])); // Check data in column A
      expect(appendedDf[1], equals([2, 4, 6, 8])); // Check data in column B
      expect(appendedDf.index, equals([0, 1, 2, 3])); // Index is reset with ignore_index
    });
    test('Append List of Lists with inplace', () {
      final newRows = [[5, 6], [7, 8]];
      df.append(newRows, ignore_index: true, inplace: true);
      expect(df[0], equals([1, 3, 5, 7])); // Check data in column A
      expect(df[1], equals([2, 4, 6, 8])); // Check data in column B
      expect(df.index, equals([0, 1, 2, 3])); // Index is reset with ignore_index
    });
    test('Append Map with columns that do not exist, ignore_index true', () {
      final newRow = {'A': 9, 'B': 10};  // When appended, should create new columns, filling previous rows with NaN. Old rows have NaN added.
      final appendedDf = df.append(newRow, ignore_index: true);
      expect(appendedDf[0][0], equals(1)); // Data in column 0
      expect(appendedDf[0][1], equals(3)); // Data in column 0
      expect(appendedDf[0][2].isNaN, isTrue); // Data in column 0
      expect(appendedDf['B'][0].isNaN, isTrue); // Data in column B
      expect(appendedDf['B'][1].isNaN, isTrue); // Data in column B
      expect(appendedDf['B'][2], equals(10)); // Data in column B
      expect(appendedDf.index, equals([0, 1, 2])); // Index reset with ignore_index
    });
    test('Throws error when appending Map with ignore_index false', () {
      final newRow = {'A': 9, 'B': 10};
      expect(() => df.append(newRow), throwsArgumentError); // ignore_index must be true for Map
    });
    test('Append List of primitives', () {
      final newRow = [9, 10]; //Should add to only first column, second column should be filled with NaN
      final appendedDf = df.append(newRow, ignore_index: true);
      expect(appendedDf[0], equals([1, 3, 9, 10])); // Data in column A
      expect(appendedDf[1][0], equals(2)); // Data in column B
      expect(appendedDf[1][1], equals(4)); // Data in column B
      expect(appendedDf[1][2].isNaN, isTrue); // Data in column B
      expect(appendedDf[1][3].isNaN, isTrue); // Data in column B
      expect(appendedDf.index, equals([0, 1, 2, 3])); // Index reset with ignore_index
    });
    test('Append List of two Maps', () {
      final newRows = [{'A': 5, 'B': 6},{'A': 7, 'B': 8}];
      final appendedDf = df.append(newRows, ignore_index: true);
      expect(appendedDf[0][0], equals(1)); // Data in column 0
      expect(appendedDf[0][1], equals(3)); // Data in column 0
      expect(appendedDf[0][2].isNaN, isTrue); // Data in column 0
      expect(appendedDf[0][3].isNaN, isTrue); // Data in column 0
      expect(appendedDf['B'][0].isNaN, isTrue); // Data in column B
      expect(appendedDf['B'][1].isNaN, isTrue); // Data in column B
      expect(appendedDf['B'][2], equals(6)); // Data in column B
      expect(appendedDf['B'][3], equals(8)); // Data in column B
      expect(appendedDf.index, equals([0, 1, 2, 3])); // Index reset with ignore_index
    });
    test('Throws error for unsupported input type', () {
      expect(() => df.append(123), throwsException); // Input type not supported
    });
  });
  group('editRow() tests', () {
    late DataFrame df;
    setUp(() {
      df = DataFrame([[1, 4, 7],[2, 5, 8],[3, 6, 9], [10,11,12]],index: ['row1','row2','row3','row4'],columns: ['A', 'B', 'C']);
    });
    test('Edit a single value in a row', () {
      df.editRow[0][1] = 30.5; // Edit single value using integer row 0 and integer column 1 
      expect(df.iloc(row:0), equals([1,30.5,7]));  
      df.editRow['row2']['A'] = 'test';  // Edit single value using row name and column name
      expect(df.iloc(row:1), equals(['test',5.0,8]));
      df.editRow[2] = [88,77,66]; // Edit an entire row
      expect(df.iloc(row:2), equals([88,77.0,66]));
    });
  });
}
