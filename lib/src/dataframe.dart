import 'dart:collection';
import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'package:csv/csv.dart';
import 'data_core.dart';
import 'dart:math' as math;
import 'dfunctions.dart';
import 'math_utils.dart';
import 'timestamp.dart';
import 'series.dart';
part 'dataframe_math.dart';

/// The main DataFrame class.
class DataFrame {
  
  // * Fields *
  final DataFrameCore _dataCore = DataFrameCore();

  // * Constructors * 

  /// Creates a new DataFrame.
  ///  
  /// - Parameters: 
  ///   - inputData: The primary data for the DataFrame. It can be a `List` or `Map`. 
  ///     For a `List`, each element represents a row of data in a table. For a `Map`, the key will be
  ///     the column name and the value is a `List` that represents the column of data.
  ///   - columns: Optional. A list of column labels. If not provided, columns will be auto-generated.
  ///   - index: Optional. A list of row labels. If not provided, row indices will be auto-generated.
  /// 
  /// Example: var df = DataFrame([[1,2,3],[4,5,6],[7,8,9]], columns: ['a','b',0]); 
  DataFrame( var inputData, {List columns = const [], List index = const []}) {
    // This constructor processes different input types (List, Map, or Series) and normalizes them 
    // to ensure that the internal data (_dataCore) is structured correctly for further operations.
    // Handle empty data (with column names), List, and Map input
    if (inputData == null || 
        (inputData is Iterable && inputData.isEmpty) ||
        (inputData is List && inputData.every((element) => element is List && element.isEmpty))) {
      if (columns.isNotEmpty) { 
        _dataCore.indexer(columns, true);
      } 
    } else if (inputData is List) {  
        _processList(inputData, columns, index);
    } else if(inputData is Map) {     
        _processMap(inputData, columns, index);   
    } else if (inputData is Series) { 
        _processSeries(inputData, columns, index);
    } else {
        throw ArgumentError('Data must be either a Map, List or Series type');
    }
  }
  // Helper method for processing List input in the DataFrame constructor
  // Note: Data needs to be normalized (no missing data for rows/columns) first before indexer is called to determine correct max length 
  void _processList(var inputData, List columns, List index) {
    // 1. DATA PROCESSING 
    // 1.a. List type check: If List elements are primitives, encapsulate them in a List, then proceed as usual.
    if(inputData.every((element) => element is! List && element is! Series)){ 
      inputData = inputData.map( (e) => [e]).toList(growable:true);
    }
    if(inputData.every((element) => element is List && element.every((subElement) => subElement is! List))) {  // Ensure inputData is a 2D matrix (List<List>) or throw error
    //1.b. Normalize row lengths: If inputData rows are not the same length, fill it in with NaN
      int longestRow = 0; // Determine longest row length
      for (var row in inputData) {
        if (row is List && row.length > longestRow) {
          longestRow = row.length;
        }
      }
      // 1.c. Avoid explicit type inference issues in Dart (e.g., List<List<int>>) by copying the original lists with Object generic.
      if(inputData.any((innerList) => innerList.length != longestRow)){
        var newInput = inputData.map( (e) => List<Object>.from(e)).toList(growable:true);
        inputData = newInput;   
      }
      // 1.d. For the rows that are lesser than the longestRow, recreate it as List<Object> so you can fill it with double.nan for the missing spots. All rows equal length now.
      for(int row = 0; row <inputData.length; row++){
        if(inputData[row].length != longestRow){
          inputData[row] = List<Object>.from(inputData[row]);
        }
        while(inputData[row].length != longestRow){
          inputData[row].add(double.nan);
        }
      }

      //1.e. CONVERT ROWS TO COLUMNS + ADD TYPE INFO - transpose adds type information via checkType being true 
      var transposedData = _dataCore.transposeT(inputData, checkType: true);

      //1.f. ADD DATA
      _dataCore.data.addAll(transposedData);
      
      // 2. INITIALIZE COLUMNS
      // 2.a. Auto-generate column names if none were entered
      if (columns.isEmpty) {
        if (inputData.isNotEmpty) {
          // note: columns argument required for columnLastIndexVal: If no columns argument given, assign it auto-generated values
          columns = List.generate(inputData[0].length, (i) => i); //inputData rows all same length at this point
        }
      }
      // 2.b. Confirm that columns argument matches number of data columns (pd). Rows were normalized in 1.b.
      if (columns.isNotEmpty && columns.length != inputData[0].length) { throw ArgumentError('columns argument size does not fit'); }
      // 2.c. Add column indices via indexer
      _dataCore.indexer(columns, true);

      // 3. INITIALIZE ROWS
      // 3.a. If index was entered, check that it's given for all rows or throw error (pd)
      if(index.isNotEmpty) {
        if(index.length != inputData.length){
          throw Exception('Index must match number of rows entered');
        }
      }
      // 3.b. If index was not entered, auto-generate
      if(index.isEmpty){index = List.generate(inputData.length, (i) => i);}
      // 3.c. Add row indices via indexer
      _dataCore.indexer(index, false);
    }
    else{throw ArgumentError('Input not a valid type');}
  }
  // Helper method for processing Map input in the DataFrame constructor
  void _processMap(var inputData, List columns, List index) {
    // 1. INITIALIZE COLUMNS - Add column names from the Map keys. Map requires k/v, no need to check column names to number of columns.
    if(columns.isNotEmpty){
      _dataCore.indexer(columns, true);
    } else {
      _dataCore.indexer(inputData.keys, true);
    }
    // 2. DETERMINE ROW LENGTH
    // 2.a. Find the longest column (List length) from the Map values. This determines the number of rows for the DataFrame.
    for (var value in inputData.values) {
      if (value is List && (value.length - 1) > _dataCore.rowLastIndexVal) {
        _dataCore.rowLastIndexVal = value.length - 1;
      }
    }
    // 2.b. If no List values are found (only primitives), set row length to 0
    if (_dataCore.rowLastIndexVal == -1) _dataCore.rowLastIndexVal = 0;

    // 2.c. Validate that all Lists in Map values are of equal length. Throw an error if the lengths are inconsistent.
    for (var kv in inputData.entries) {
      if (kv.value is List && kv.value.length != _dataCore.rowLastIndexVal + 1) {
        throw ArgumentError('Column data entries must be the same size');
      } else if (kv.value is! List && _dataCore.rowLastIndexVal > 0) {
        throw ArgumentError('Column data entries must be the same size');
      }
    }
    
    // 3. ADD DATA TO MATRIX
    // 3.a. Populate _dataCore.data with the values from the Map. If a value is a List, add it directly. If primitives, wrap it in a List.
    // If columns parameter was passed an argument, match the labels with the keys; for each match add the data, if it doesn't match, add NaN.
    if(columns.isEmpty){
      for (var value in inputData.values) {
        if (value is List) {
          _dataCore.data.add(value);
        } else {
          List newColumn = createListFromType(value.runtimeType);
          newColumn.add(value);
          _dataCore.data.add(newColumn);
        }
      }
    } else {
      for(var name in columns){
          // If column name exists in the Map, add the corresponding column data
          if(inputData.containsKey(name)){
            var value = inputData[name];
            if (value is List) {
              _dataCore.data.add(value);
            } else {
              List newColumn = createListFromType(value.runtimeType);
              newColumn.add(value);
              _dataCore.data.add(newColumn);
            }
          } 
          // If the Map doesn't contain the column name, fill with NaN
          else{
            if(_dataCore.rowLastIndexVal == 0){
              _dataCore.data.add(<Object>[]);
            } else{
            List<double> nanColumn = List.generate(_dataCore.rowLastIndexVal+1, (_)=> double.nan, growable: true);
            _dataCore.data.add(nanColumn);
            }
          }
       }
    }
    // 4. DETERMINE COLUMN TYPES 
    // 4.a. Add type information for each column
    for (var column in _dataCore.data) {
      _dataCore.columnTypes.add(checkListType(column));
    }
    // 4.b. Convert columns of 'num' type to 'double'. Ensures numerical consistency across the DataFrame.
    int numIndex = _dataCore.columnTypes.indexOf(num);
    if (numIndex != -1) {
      // mark column as double-based (you might still keep track of nullability separately)
      _dataCore.columnTypes[numIndex] = double;
      final rawCol = _dataCore.data[numIndex];
      final temp   = <double?>[];
      for (var x in rawCol) {
        if (x is num) {
          // ints, doubles, and double.nan → toDouble() (double.nan stays double.nan)
          temp.add(x.toDouble());
        } else {
          // preserve actual nulls
          temp.add(null);
        }
      }
      _dataCore.data[numIndex] = temp;
    }
    // 5. INITIALIZE ROW INDICES
    // 5.a. Validate the index size matches the number of rows
    if (index.isNotEmpty && index.length != inputData.values.first.length) {
      throw ArgumentError('Index must match number of rows');
    }
    // 5.b. Auto-generate row indices if not provided
    if (index.isEmpty) {
      index = List.generate(_dataCore.data.first.length, (i) => i);
    }
    // 5.c. Add row indices to the matrix
    //      - Reset index to account for earlier increment.
    _dataCore.indexer(index, false, resetIndex: true);
  }
  // Helper method for processing Series input in the DataFrame constructor (Series is unused at the moment)
  void _processSeries(var inputData, List columns, List index) {
      //COLUMN NAMES must be entered 
      if(columns.isEmpty){ // Not pd behavior
        throw ArgumentError('Columns argument must be entered when using Series type');
      } 
      // Make sure number of column names entered is same as number of data columns entered
      if(columns.length != inputData.values.length){
        throw ArgumentError('Column names entered must match columns of data');
      }
      //1.a. Add column names and increment columnLastIndexVal
      _dataCore.indexer(columns, true);

      // 2. Add data
      if(inputData.values[0] is! List){
        for(int i = 0; i < inputData.values.length; i++){   
          var tempColumn = createListFromType(inputData.values[i].runtimeType);
          tempColumn.add(inputData.values[i]);
          _dataCore.data.add(tempColumn);
        }
      } else{
        _dataCore.data = inputData.values;
      }
      //2.d. Add type info
      for(var column = 0; column <_dataCore.data.length; column++){
        Type tempType = checkListType(_dataCore.data[column]);
  
        if(tempType == num){
          tempType = double;
          var newListType = astype(double,_dataCore.data[column]);
          _dataCore.data[column] = newListType;
        }
        _dataCore.columnTypes.add(tempType);
      }
      // 3. Add row index
      if(index.isNotEmpty && index.length != inputData.values.first.length) { throw ArgumentError('Index must match number of rows');}
      
      if(index.isEmpty){index = List.generate(_dataCore.data.first.length, (i) => i);} //Currently, index is always empty
      _dataCore.indexer(index, false, resetIndex: true); //resetIndex because it was incremented earlier
      
  }
  // Create a copy of a DataFrame
  DataFrame._copyDataframe(DataFrame df) {
    _dataCore.columnIndexMap = Map.from(df._dataCore.columnIndexMap).map((key, value) => MapEntry(key, List<int>.from(value)));
    _dataCore.rowIndexMap = Map.from(df._dataCore.rowIndexMap).map((key, value) => MapEntry(key, List<int>.from(value)));
    _dataCore.rowLastIndexVal = df._dataCore.rowLastIndexVal;
    _dataCore.columnLastIndexVal = df._dataCore.columnLastIndexVal;
    _dataCore.columnTypes = List.from(df._dataCore.columnTypes);
    var counter = 0;
    for(var type in _dataCore.columnTypes){
        if(type == int){
          _dataCore.data.add(List<int>.from(df._dataCore.data[counter]));
        } else if(type == double){
          _dataCore.data.add(List<double>.from(df._dataCore.data[counter]));
        } else if(type == String){
          _dataCore.data.add(List<String>.from(df._dataCore.data[counter]));
        } else if(type == bool){
          _dataCore.data.add(List<bool>.from(df._dataCore.data[counter]));
        } else {
          _dataCore.data.add(List<Object>.from(df._dataCore.data[counter]));
        }
        counter++;
    }
  }
  
  /// Creates an empty DataFrame.
  static final DataFrame empty = DataFrame._empty();
  DataFrame._empty();

// * DataFrame Methods *

  // ** Data Editing Methods **

  /// Drops specified rows or columns from the DataFrame.
  ///
  /// - Parameters:
  ///   - input: The name of the row or column to drop, or an index if applicable.
  ///   - axis: Determines whether to drop rows (0) or columns (1). Default is 0 (rows).
  ///   - inplace: If true, modifies the original DataFrame. If false, returns a new DataFrame.
  ///   - select: Optional. Specifies which occurrence to remove if there are multiple with the same name. 
  ///         If set to 0, all occurrences will be removed.
  ///
  /// - Returns: 
  ///   A new DataFrame if inplace is set to false. Otherwise, modifies the DataFrame in place.
  ///
  /// - Example:
  ///   ```dart
  ///   var df = DataFrame([[1, 2], [3, 4], [5, 6]], columns: ['A', 'B']);
  ///   var newDf = df.drop('A', axis: 1); // Drops column 'A' and returns a new DataFrame.
  ///   df.drop(0, axis: 0, inplace: true); // Drops the first row from the original DataFrame.
  ///   ```
  DataFrame drop(var input, {int axis = 0, bool inplace = false, int select = 0}) {
    DataFrame df = this;
    var dataCore = _dataCore;
    var indexMap = axis == 0 ? _dataCore.rowIndexMap : _dataCore.columnIndexMap;
    var data = _dataCore.data;
    var columnTypes = _dataCore.columnTypes;

    //Create copy of object if inplace parameter is false
    if(inplace == false) {
      df = DataFrame._copyDataframe(this);
      dataCore = df._dataCore;
      indexMap = axis == 0 ? df._dataCore.rowIndexMap : df._dataCore.columnIndexMap;
      data = df._dataCore.data;
      columnTypes = df._dataCore.columnTypes;
    }
    if(indexMap.containsKey(input)){ // Verify that column/row name exists
      if(select != 0){ // Check if 'select' was given an argument. If it was, remove only single entry
        var occurrences = indexMap[input].length;
        if(select > occurrences){
          throw ArgumentError('The select value cannot exceed number of occurrences of the row index');
        }
        int atCounter = 0;
        if(axis == 0){
          for(var valueRowIndex in indexMap[input]){ 
            ++atCounter;
            if(select == atCounter){
              for(int i = 0; i < data.length ; i ++){  
                data[i].removeAt(valueRowIndex);
              }
            }
          }
          dataCore.editIndices(indexName: input, isColumn: false, select: select);
          if(data[0].isEmpty){
            columnTypes.clear();
          }
        } 
        else if(axis == 1){
          for(var valueColIndex in indexMap[input]){ 
            ++atCounter;
            if(select == atCounter){
                data.removeAt(valueColIndex);
            }
          }
          dataCore.editIndices(indexName: input, isColumn: true, select: select);
        } else {
          throw ArgumentError('Not a valid axis value');
        }   
      } else if (select == 0){
        Iterable reversedIndex = indexMap[input].reversed; // Use reversed index to avoid changing element errors
        if (axis == 0) {
          for(var rowIndex in reversedIndex){ // Iterate backwards
            for(int i = 0; i < data.length ; i ++){  
              data[i].removeAt(rowIndex);
            }
          }
          dataCore.editIndices(indexName: input, isColumn: false);
          if(data[0].isEmpty){
            columnTypes.clear();
          }
        } else if (axis == 1) {
          for(var colIndex in reversedIndex){ // Iterate backwards
              data.removeAt(colIndex);    
          }
          dataCore.editIndices(indexName: input, isColumn: true);
        } else {
          throw ArgumentError('Not a valid axis value');
          }
        }
      } else{
        throw StateError('The index value entered cannot be found');
      }
      if(inplace == false) {
        return df;
      } 
      return DataFrame.empty;
  }
  
  /// Renames row or column indices in the DataFrame.
  ///
  /// - Parameters:
  ///   - index: A map where each key-value pair specifies the current row name and its new name.
  ///   - columns: A map where each key-value pair specifies the current column name and its new name.
  ///   - inplace: If true, modifies the original DataFrame. If false (default), returns a copy with changes.
  ///   - atIndex: Specifies which occurrence to rename if there are multiple rows with the same name. 
  ///              Default is 0, which changes all occurrences. If specified, only one row can be renamed.
  ///   - atCol: Specifies which occurrence to rename if there are multiple columns with the same name. 
  ///            Default is 0, which changes all occurrences.
  ///
  /// - Returns: 
  ///   A new DataFrame if inplace is set to false. Otherwise, modifies the DataFrame in place.
  ///
  /// - Example:
  ///   ```dart
  ///   var df = DataFrame([[1, 2], [3, 4], [5, 6]], columns: ['A', 'B']);
  ///   df.rename(index: {0: 'row2'}, columns: {1: 'col2'}, inplace: true); // Renames row '0' to 'row2' and column '1' to 'col2'.
  ///   ``` 
  DataFrame rename({Map index = const {}, Map columns = const {}, bool inplace=false, int atIndex = 0, int atCol = 0}) {
    // 1. INPUT VALIDATION
    // 1.a. Ensure that at least one of 'index' or 'columns' arguments is provided
    if (index.isEmpty && columns.isEmpty) {
      throw ArgumentError('Index and/or columns argument must be entered');
    }

    // 2. VARIABLE INITIALIZATION
    // 2.a. Retrieve the current row and column mappings
    var currentRowMap = _dataCore.rowIndexMap;
    var currentColMap = _dataCore.columnIndexMap;
    DataFrame df = empty;
    
    // 2.b. Create a copy of the DataFrame if 'inplace' is false
    //      - 'inplace=false' means changes are made on a new DataFrame, not the original.
    if (inplace == false) {
      df = DataFrame._copyDataframe(this);
      currentRowMap = df._dataCore.rowIndexMap;
      currentColMap = df._dataCore.columnIndexMap;
    }

    // 3. VALIDATE 'atIndex' PARAMETER FOR ROW RENAMING
    // 3.a. If 'atIndex' is specified, ensure only one row can be renamed
    if (atIndex != 0) {
      if (index.length != 1) {
        throw ArgumentError("When 'at' parameter specified, only one row can be renamed");
      }
      if (atIndex < 0) {
        throw ArgumentError("Invalid 'at' argument");
      }
    }

    // 4. RENAME INDEX
    // 4.a. Rename rows using the 'index' Map entries
    for (var entry in index.entries) {
      // 4.b. Ensure the row name to be changed exists in the current mapping
      if (!currentRowMap.containsKey(entry.key)) {
        throw ArgumentError('Invalid key selected for renaming');
      }
      
      // 4.c. If 'atIndex' is specified, but there are no multiple occurrences, reset 'atIndex' to 0
      if (currentRowMap[entry.key].length == 1) {
        atIndex = 0;
      }
      
      // 4.d. Skip renaming if the old name is the same as the new name
      if (entry.key == entry.value) {
        continue;
      }

      // 4.e. If the new row name already exists, merge entries, otherwise create a new entry
      if (currentRowMap.containsKey(entry.value)) {
        if (atIndex > 0) {
          // Rename a specific occurrence if 'atIndex' is specified
          var indexToRename = atIndex - 1;
          currentRowMap[entry.value].add(currentRowMap[entry.key][indexToRename]);
          currentRowMap[entry.value].sort();
          currentRowMap[entry.key].removeAt(indexToRename);
        } else {
          // Rename all occurrences
          currentRowMap[entry.value].addAll(currentRowMap[entry.key]);
          currentRowMap[entry.value].sort();
          currentRowMap.remove(entry.key);
        }
      } else {
        // Create a new entry for the renamed row
        if (atIndex > 0) {
          var indexToRename = atIndex - 1;
          currentRowMap[entry.value] = [currentRowMap[entry.key][indexToRename]];
          currentRowMap[entry.key].removeAt(indexToRename);
        } else {
          currentRowMap[entry.value] = currentRowMap[entry.key];
          currentRowMap.remove(entry.key);
        }
      }
    }

    // 5. RENAME COLUMNS
    // 5.a. Rename columns using the 'columns' Map entries
    for (var entry in columns.entries) {
      // 5.b. Ensure the column name to be changed exists in the current mapping
      if (!currentColMap.containsKey(entry.key)) {
        throw ArgumentError('Invalid key selected for renaming');
      }
      
      // 5.c. If 'atCol' is specified, but there are no multiple occurrences, reset 'atIndex' to 0
      if (currentColMap[entry.key].length == 1) {
        atCol = 0;
      }
      
      // 5.d. Skip renaming if the old name is the same as the new name
      if (entry.key == entry.value) {
        continue;
      }

      // 5.e. If the new column name already exists, merge entries, otherwise create a new entry
      if (currentColMap.containsKey(entry.value)) {
        if (atCol > 0) {
          // Rename a specific occurrence if 'atCol' is specified
          var columnToRename = atCol - 1;
          currentColMap[entry.value].add(currentColMap[entry.key][columnToRename]);
          currentColMap[entry.value].sort();
          currentColMap[entry.key].removeAt(columnToRename);
        } else {
          // Rename all occurrences
          currentColMap[entry.value].addAll(currentColMap[entry.key]);
          currentColMap[entry.value].sort();
          currentColMap.remove(entry.key);
        }
      } else {
        // Create a new entry for the renamed column
        if (atCol > 0) {
          var columnToRename = atCol - 1;
          currentColMap[entry.value] = [currentColMap[entry.key][columnToRename]];
          currentColMap[entry.key].removeAt(columnToRename);
        } else {
          currentColMap[entry.value] = currentColMap[entry.key];
          currentColMap.remove(entry.key);
        }
      }
    }

    // 6. RETURN RESULT
    // 6.a. Return the modified DataFrame (or original if 'inplace' is true)
    return df;
  }
  
  /// Reorders the rows of the DataFrame. Use [] to reorder entire row order via row names. 
  /// Use {} to move just a single row to another row position, with 'select' parameter for choosing occurrences
  ///
  /// - Parameters:
  ///   - data: If a `List`, reorders all rows according to the given row names. 
  ///            If a `Map`, moves a single row entered as the key to a position after the row entered as a value
  ///   - inplace: If true, modifies the original DataFrame. If false (default), returns a new DataFrame.
  ///   - select: Specifies which occurrence to move if there are multiple rows with the same name.
  ///             For example, if you want to move the third occurrence of a row, set the value to 3.
  ///
  /// - Returns:
  ///   A new DataFrame if inplace is false, or modifies the original DataFrame if inplace is true.
  ///
  /// - Example:
  ///   ```dart
  ///   var df = DataFrame([[1, 2], [3, 4], [5, 6]], columns: ['A', 'B']);
  ///   var newDf = df.reindex(['row1', 'row3', 'row2']); // Reorders rows by the given names.
  ///   df.reindex({'row2': 'row1'}, select: {1: 1}, inplace: true); // Moves 'row2' to the position of 'row1'.
  ///   ```
  reindex(var data, {bool inplace = false, Map<int, int> select = const {}}) {
    // 1. INITIALIZE VARIABLES
    // 1.a. Create a new DataFrame if 'inplace' is false; otherwise, use the original
    DataFrame df = empty;
    if (inplace == false) {
      df = DataFrame._copyDataframe(this);
    } else {
      df = this;
    }
    // 2. VALIDATE INPUT TYPE
    // 2.a. Ensure the input is either a Map or List; otherwise, throw an error
    if (data is! Map && data is! List) {
      throw ArgumentError('Invalid type, must be a List or Map');
    }
    // 3. HANDLE MAP INPUT (MOVING ROWS)
    // 3.a. If input is a Map, perform reindexing by moving rows to new positions
    if (data is Map) {
      // 3.b. Validate that only one row is specified in the input Map
      if (data.keys.length > 1) {
        throw ArgumentError('Can only move one row at a time with reindex');
      }
      // 3.c. If 'select' argument is not provided, default to {1:1}
      if (select.isEmpty) {
        select = {1: 1};
      }
      // 3.d. Get the index positions for the row to move and the new target position
      int indexPositionToMove = df._dataCore.rowIndexMap[data.keys.first][select.keys.first - 1];
      int newIndexPosition = df._dataCore.rowIndexMap[data.values.first][select.values.first - 1];
      // 3.e. Create a new matrix to hold the reordered data
      List newCore = [];
      // Create a list of indices representing the current order
      List<int> indices = List<int>.generate(df._dataCore.data[0].length, (i) => i);
      // Remove the index of the row to move
      indices.removeAt(indexPositionToMove);
      // Insert the index at the new position
      indices.insert(newIndexPosition, indexPositionToMove);

      for (int i = 0; i < df._dataCore.data.length; i++) {
        newCore.add(createListFromType(df._dataCore.columnTypes[i]));
        for (int idx in indices) {
          newCore[i].add(df._dataCore.data[i][idx]);
        }
      }
      // 3.f. Update the DataFrame's data with the new matrix
      df._dataCore.data = newCore;
      // 3.g. Adjust the row index accordingly
      df._dataCore.editIndices(indexName: data.keys.first,isColumn: false,select: select.keys.first,moveTo: newIndexPosition);

      // Return the modified DataFrame if 'inplace' is false
      if (inplace == false) {
        return df;
      }
    } else {
      // 4. HANDLE LIST INPUT (REORDERING ROWS)
      // 4.a. Verify that all elements in 'data' exist in the current row index map
      // 4.b. Create a new row index map with empty lists for each entry
      var newRowIndexMap = Map.fromEntries(
        df._dataCore.rowIndexMap.entries.map((entry) => MapEntry(entry.key, <int>[])),
      );
      // 4.c. Store the original lengths of each row's values
      List<int> listLengths1 = List<int>.from(df._dataCore.rowIndexMap.values.map((e) => e.length));
      // 4.d. Transpose the current data matrix for easier row reordering
      List dataT = df._dataCore.transposeT(df._dataCore.data);
      // 4.e. Populate the new row index map based on the input order
      int counter = 0;
      for (var rowName in data) {
        if (newRowIndexMap.containsKey(rowName)) {
          newRowIndexMap[rowName]?.add(counter);
          counter++;
        } else {
          throw ArgumentError('Invalid row name');
        }
      }
      // 4.f. Reorder the transposed data matrix according to the new row order
      List newDataT = List.generate(dataT.length, (_) => <dynamic>[]);
      counter = 0;
      for (var rowName in newRowIndexMap.keys) {
        for (int values = 0; values < listLengths1[counter]; values++) {
          newDataT[newRowIndexMap[rowName]![values]] =
              (dataT[df._dataCore.rowIndexMap[rowName]![values]]);
        }
        counter++;
      }
      // 4.g. Validate that the reordered list lengths match the original list lengths
      List<int> listLengths2 = List<int>.from(newRowIndexMap.values.map((e) => e.length));
      if (!areListsEqual(listLengths1, listLengths2)) {
        throw ArgumentError('Row lengths do not match after reindexing');
      }
      // 5. UPDATE DATAFRAME
      // 5.a. If 'inplace' is false, create a new DataFrame with the reordered data
      if (inplace == false) {
        df = DataFrame(newDataT, index: data, columns: columns);
        return df;
      } else {
        // 5.b. Update the original DataFrame with the new data and row index map
        df._dataCore.data = df._dataCore.transposeT(newDataT);
        df._dataCore.rowIndexMap = newRowIndexMap;
      }
    }
    // Return the modified DataFrame if 'inplace' is false
    if (inplace == false) {
      return df;
    }
    return;
  }

  /// Appends new rows to the DataFrame.
  ///
  /// - Parameters:
  ///   - newRow: The data to append, which can be a `Map` or `List`. If a `Map`, it can only add a single row.
  ///   - index: Optional list specifying the index for the new rows. 
  ///   - columns: Optional list specifying the column labels for the new rows.
  ///   - ignore_index: If true, original row indices are reset and given a new sequential index starting from 0, including the new rows. Default is false.
  ///   - inplace: If true, modifies the original DataFrame. If false, returns a new DataFrame with the appended rows. Default is false.
  ///
  /// - Returns:
  ///   The modified `DataFrame` with the new rows appended.
  ///
  /// - Notes:
  ///   - The `newRows` parameter must be a `Map`, `List` of lists/maps, or a `Series`.
  ///   - If `newRows` is a list of primitive values, each value is treated as a row.
  ///
  /// - Example:
  ///   ```dart
  ///   var df1 = df.append([[4, 2, 1]]); // Adds a new row [4, 2, 1] to the DataFrame.
  ///   var df2 = df.append([{ 'a': 4, 'b': 2 }, { 'a': 6, 'b': 1 }]); // Adds two new rows from maps.
  ///   var df3 = df.append([5, 6, 7], ignore_index: true); // Treats [5, 6, 7] as individual rows.
  ///   ```
  DataFrame append(var newRow, {List index = const [], List columns = const [], bool ignore_index = false, bool inplace = false,}) {
    DataFrame df1 = empty;
    if(inplace==false){
      //Make copy of instance _dataCore and use it instead so that original is not modified
      df1 = DataFrame._copyDataframe(this);
    } else{
      df1 = this;
    }
    // Map input
    if (newRow is Map) { 
      _mapAppend(newRow, df1, index: index, columns: columns, ignore_index: ignore_index, inplace: inplace);
    // List input.
    } else if (newRow is List) { 
        // elements are List type
        if(newRow.every( (e)=>e is List)){ 
          _listAppend(newRow, df1, index: index, columns: columns, ignore_index: ignore_index, inplace: inplace);
        // elements are Map type
      } else if(newRow.every( (e)=>e is Map)){ 
        for(var maps in newRow){
          _mapAppend(maps, df1, index: index, columns: columns, ignore_index: ignore_index, inplace: inplace);
        }
        // elements are primitives 
      } else if(newRow.every( (e)=> (e is num) || (e is String))){ 
        newRow = newRow.map( (e)=> [e]).toList();
        _listAppend(newRow, df1, index: index, columns: columns, ignore_index: ignore_index, inplace: inplace);
      }else {
        throw ArgumentError('Input must be a Map, List of Lists, or List of Maps');
      }
      // Series input
    } else if (newRow is Series){
        _seriesAppend(newRow, df1, index: index, columns: columns, ignore_index: ignore_index, inplace: inplace);
    } else {
      throw Exception('Input type is not supported. It must be a Map, List, or Series.');
    }
    return df1;
  }
  // Helper method for append(), processing List type.
  void _listAppend(List newRows, DataFrame df1, {List index = const [], List columns = const [], bool ignore_index = false, bool inplace = false,}) {
    if(newRows.every( (e)=>e is List)){ // List - List
      List dfcolumns = this.columns;
      for(var lists in newRows){             
        if(columns.isEmpty){ // Ensure that a column name is provided
          columns = [];
          for(int i =0; i< lists.length; i++){
            columns.add(i);
          }
        }
        if (dfcolumns.isNotEmpty && (columns.any((name) => !dfcolumns.contains(name))) ) {  
          throw Exception('A valid column name must be specified for List type');
        }
        lists = _dataCore.transposeT([lists]);
        // Add Data. Check if it's a List of primitives or a List of Lists.
        for(int i = 0; i < df1._dataCore.columnLastIndexVal+1; i++){
            var tempElement;
            try {
              tempElement = lists[i].first;
            } catch (e) {
              tempElement = double.nan;
            }
            df1._dataCore.addEditType(input: tempElement, colIndex: i);  
        }
      }
        df1._dataCore.indexer(List<int>.generate(newRows.length, (i) => i), false); // Update row index
        if(ignore_index == true){
          List newIndex = List.generate(df1._dataCore.rowLastIndexVal+1, (i) => i, growable: true); 
          df1._dataCore.indexer(newIndex, false, resetIndex: true);
        }
    }
  }
  // Helper method for append(), processing Map type.
  void _mapAppend(Map newRows, DataFrame df1, {List index = const [], List columns = const [], bool ignore_index = false, bool inplace = false,}) {
      if(ignore_index == false){
        throw ArgumentError('ignore_index must be set to true for Map input');
      }
      // Check for new keys (column labels)   
      var oldKeys = df1._dataCore.columnIndexMap.keys;
      var newKeys = newRows.keys.toSet();
      df1._dataCore.indexer([0], false);

      for(var key in oldKeys){
        for(int i =0; i<df1._dataCore.columnIndexMap[key].length; i++){
          var columnElement =  newKeys.contains(key) ? newRows[key] : double.nan;
          df1._dataCore.addEditType(input: columnElement, colIndex: df1._dataCore.columnIndexMap[key][i]);
        }
      }
      // For the keys that weren't contained, make a new column and add the new column name. add column with NaN
      List newUniqueKeys = newKeys.where( (key)=> !oldKeys.contains(key)).toList();
      if(newUniqueKeys.isNotEmpty){
        for(var key in newUniqueKeys){
          df1._dataCore.columnTypes = List.from(df1._dataCore.columnTypes, growable: true); // dart thinks columnTypes is fixed
          df1._dataCore.indexer([key], true);
          for(int i=0; i<df1._dataCore.rowLastIndexVal; i++){
            df1._dataCore.addEditType(input: double.nan, colIndex: df1._dataCore.columnLastIndexVal);
          }
          df1._dataCore.addEditType(input: newRows[key], colIndex: df1._dataCore.columnLastIndexVal);
        }      
      }
      if(ignore_index == true){
        List newIndex = List.generate(df1._dataCore.rowLastIndexVal+1, (i) => i);
        df1._dataCore.indexer(newIndex, false, resetIndex: true);
      }
  }
  // Helper method for append(), processing Series type.
  void _seriesAppend(Series newRows, DataFrame df1, {List index = const [], List columns = const [], bool ignore_index = false, bool inplace = false,}){
      // If Series is an input, ignore_index has to be 'true' unless the Series has been given a 'name' argument
      if(ignore_index == false && (newRows.name is String && newRows.name.isEmpty)){
        throw ArgumentError('ignore_index must be true for Series input');
      }
      // Check Series contains column names of dataframe it is being appended to
      bool columnsMatch = newRows.index.every((e) => this.columns.contains(e));
      if (!columnsMatch) throw ArgumentError('Column names must match');
      // Throw error if empty values
      if (newRows.values.isEmpty) throw ArgumentError('No data provided');
      // Check that values are Iterable type (this check might not be needed anymore due to Series single element being placed into List change)
      bool isFirstElementIterable = newRows.values.first is Iterable && newRows.values.first is! String;
      int expectedLength = isFirstElementIterable ? (newRows.values.first as Iterable).length : columns.length;
      // Validate the consistency of the data lengths and types
      bool isConsistent = newRows.values.every((element) =>
          (isFirstElementIterable 
            && element is Iterable 
            && element is! String 
            && element.length == expectedLength) || 
            (!isFirstElementIterable && element is! Iterable));
      if (!isConsistent) {
        throw ArgumentError('Inconsistent data lengths or mixed types.');
      }
      // Add data and new row index
        var indexNames = newRows.index;
        for(int i =0; i< newRows.values.length; i++){
          //newRows[indexNames[i]] = [newRows.values[i]]; // Encase primitive in a List
          df1._dataCore.addEditType(input: newRows[indexNames[i]], colIndex: i);
        }
        df1._dataCore.indexer([newRows.name], false);
      // Update the DataFrame index if ignore_index is true and Series 'name' parameter was not entered
      if (ignore_index == true && (newRows.name is String && newRows.name.isEmpty)) {
        List newIndex = List.generate(df1._dataCore.rowLastIndexVal + 1, (i) => i);
        df1._dataCore.indexer(newIndex, false, resetIndex: true);
      }
    }

  // ** CSV read and write methods **

  /// Reads a CSV file from a local file path or a remote data string and returns a DataFrame.
  /// 
  /// Usage example: var dfCsv = await pDataFrame.read_csv(path: 'lib/test.csv');
  /// 
  /// - Parameters:
  /// 
  ///   - path: (String) add directory of local file. e.g. 'path: lib/test.csv'
  ///   - remoteData: (String) parameter for csv data provided through http. Currently, standard output from http library is String 
  ///   - rows/columns: (List) Use to only pass specific rows or columns, use index integer and/or List for a range of indices.   
  ///                 e.g. columns: [0,2, [4,7]] will return columns 0,2,4,6,7.
  ///   - hasHeader: (bool) If set to false, will not use csv first row as column names and instead use as data
  ///   - hasIndex: (bool) If set to true, the first column (with the first element removed), will be used as the row indices. csv
  ///             format does include row indices as a format so this option might be unnecessary. 
  ///   - newHeader: (List) Replace csv header with a new one
  ///   - convertColumn: (Map) Will apply a function to all the elements in a column. The key is the column index, and the value is the function. e.g.:
  ///                  convertColumn: {2: num.parse}       Convert column 3 from String to num
  ///                  convertColumn: {5: (cell) => double.tryParse(cell.toString()) ?? 0.0}    Convert column 6 to double
  ///   - Other parameters from CsvToListConverter: fieldDelimiter, textDelimiter, textEndDelimiter, eol, convertNumeric, allowInvalid
  /// - Example:
  ///   ```dart
  ///   var dfCsv = await pDataFrame.read_csv(path: 'lib/test.csv');
  ///   ```
  static Future<DataFrame> read_csv(String? path,{
    bool pathIsData = false,
    List<dynamic>? rows,
    List<dynamic>? columns,
    bool hasHeader = true,
    bool hasIndex = false,
    List newHeader = const [],
    Map<int, Function>? convertColumn,
    // CsvToListConverter parameters
    String fieldDelimiter = defaultFieldDelimiter,
    String? textDelimiter = defaultTextDelimiter,
    String? eol = defaultEol,
    bool? convertNumeric = true,
    bool? allowInvalid,
  }) async {
    List<List<dynamic>> csvTable = [];
    List header = [];
    List rowIndex = [];
    String? remoteData;
    if(pathIsData == true){
      remoteData = path;
    }

    int currentRow = 0;
    List<int> selectedRows = expandIndices(rows);
    List<int> selectedCols = expandIndices(columns);

    if (path != null && pathIsData == false) {
      final input = File(path).openRead();
      final stream = input
          .transform(utf8.decoder)
          .transform(CsvToListConverter(
            fieldDelimiter: fieldDelimiter,
            textDelimiter: textDelimiter,
            eol: eol,
            shouldParseNumbers: convertNumeric,
            allowInvalid: false
          ));

      await for (final row in stream) {
        if (hasHeader && currentRow == 0) {
            header = [];
      for (int i = hasIndex ? 1 : 0; i < row.length; i++) {
        if (selectedCols.isEmpty || selectedCols.contains(i)) {
          header.add(row[i].toString().trim());   // Directly add the element without conversion
        }
      }
    } else if (selectedRows.isEmpty || selectedRows.contains(currentRow)) {
        if (hasIndex) {
          var indexValue = row[0];      
          // Preserve the type of the row index if it's int or double
          if (indexValue is! int && indexValue is! double && indexValue is! bool) {
            indexValue = indexValue.toString().trim();
          }
          rowIndex.add(indexValue);
        }
      List<dynamic> filteredRow = [];
      for (int i = hasIndex ? 1 : 0; i < row.length; i++) {
        if (selectedCols.isEmpty || selectedCols.contains(i)) {
          var value = row[i];

          // Convert value to String only if it is not int, double, or bool
          if (value is! int && value is! double && value is! bool) {
            value = value.toString().trim();  // Trim each element only if it's not a number or bool
          }

          if (convertColumn != null && convertColumn.containsKey(i)) {
            value = convertColumn[i]!(value);  // Apply the conversion function
          }
          filteredRow.add(value);
        }
      }
      csvTable.add(filteredRow);
            }
            currentRow++;
      }
    } else if (remoteData != null && pathIsData == true) {
      final stream = Stream.value(remoteData)
          .transform(CsvToListConverter(            
              fieldDelimiter: fieldDelimiter,
              textDelimiter: textDelimiter,
              eol: eol,
              shouldParseNumbers: convertNumeric,
              allowInvalid: false
            ));
      await for (final row in stream) {
        if (hasHeader && currentRow == 0) {
          header = [];
          for (int i = hasIndex ? 1 : 0; i < row.length; i++) {
            if (selectedCols.isEmpty || selectedCols.contains(i)) {
              header.add(row[i].toString().trim());  // Capture only the selected columns
            }
          }
        } else if (selectedRows.isEmpty || selectedRows.contains(currentRow)) {
          if (hasIndex) {
            rowIndex.add(row[0].toString().trim());  // Add the first column as the row index
          }
          List<dynamic> filteredRow = [];
          for (int i = hasIndex ? 1 : 0; i < row.length; i++) {
            if (selectedCols.isEmpty || selectedCols.contains(i)) {
              dynamic value = row[i].toString().trim();  // Trim each element
              if (convertColumn != null && convertColumn.containsKey(i)) {
                value = convertColumn[i]!(value);  // Apply the conversion function
              }
              filteredRow.add(value);
            }
          }
          csvTable.add(filteredRow);
        }
        currentRow++;
      }
    }

    if( newHeader.isNotEmpty && (newHeader.length != csvTable[0].length)){
      throw ArgumentError('The entered header length must match the number of columns');
    }

    // If hasHeader is false, pass null to use the DataFrame's default behavior
    return DataFrame(csvTable, columns: newHeader.isEmpty ? (hasHeader ? header : []) : newHeader, index: hasIndex ? rowIndex : []);
  }
  
  /// Exports the DataFrame to a CSV file.
  ///
  /// - Parameters:
  ///   - file: The path where the CSV file will be saved.
  ///   - index: If true, includes row indices as the first column in the CSV file. Default is false.
  ///
  /// - Returns:
  ///   A `Future<void>` that completes when the file has been written.
  ///
  /// - Usage example:
  ///   ```dart
  ///   df.to_csv(file: 'testCsv.csv'); // Saves the DataFrame to 'testCsv.csv' without row indices.
  ///   df.to_csv(file: 'lib/files/testCsv.csv', index: true); // Saves with row indices included.
  ///   ```
  Future<void> to_csv({
    required String file,
    bool index = false,
  }) async {
    final output = File(file).openWrite();
    List<List<String>> rows = [];
    // Add the header row, including the index column if specified
    if (index) {
      rows.add([''] + columns.map((e) => e.toString()).toList());  // Convert columns to List<String>
    } else {
      rows.add(columns.map((e) => e.toString()).toList());  // Ensure columns are List<String>
    }
    // Create the rows by combining the index and values
    for (int i = 0; i < values[0].length; i++) {
      List<String> row = [];
      if (index) {
        row.add(this.index[i].toString());  // Add the row index if specified
      }
      for (int j = 0; j < values.length; j++) {
        row.add(values[j][i].toString());
      }
      rows.add(row);
    }
    // Write the rows to the CSV file
    for (List<String> row in rows) {
      output.writeln(row.join(','));
    }
    await output.close();
  }

  // ** Sorting methods **

  /// Returns a new DataFrame sorted by a specified column.
  ///
  /// - Parameters:
  ///   - colName: The name of the column to sort by.
  ///   - inplace: If true, sorts the original DataFrame. If false, returns a new DataFrame. Default is true.
  ///   - nullIsFirst: If true, sorts null values to appear first. Default is true.
  ///   - comparator: A custom comparison function for sorting. If null, uses the default comparison for column values.
  ///   - select: When multiple columns have the same name, this parameter specifies which column to sort by, based on the integer index. Default is 1 (first occurrence).
  ///   - ascending: If true, sorts in ascending order (default). If false, sorts in descending order.
  ///
  /// - Returns:
  ///   A new DataFrame sorted by the specified column if inplace is false, or modifies the original DataFrame if inplace is true.
  ///
  /// - Notes:
  ///   - By default, rows are sorted using [Comparable.compare] on column values, with nulls handled based on the [nullIsFirst] parameter.
  ///   - If a custom comparator is provided, it takes precedence over the [nullIsFirst] parameter. Custom comparators must handle null values appropriately.
  ///
  /// - Example:
  ///   ```dart
  ///   var df = DataFrame([[1, 2, null], [3, 4, 5]], columns: ['A', 'B', 'C']);
  ///   var sortedDf = df.sort('B', inplace: false); // Sorts by column 'B' and returns a new DataFrame.
  ///   df.sort('C', comparator: (a, b) => a == null ? -1 : a.compareTo(b), inplace: true); // Sorts in-place by 'C', with custom handling for nulls.
  ///   ```
 DataFrame sort(
    var colName,
    {bool inplace = true,
    bool nullIsFirst = true,
    CustomComparator? comparator,
    int select = 1,
    bool ascending = true}) {

    // Initialize List that represents current index order
    List newIndexOrder = List.generate(_dataCore.data[0].length, (i) => i);

    // Determine the column index to use
    var colIndex = _dataCore.columnIndexMap[colName][select - 1]; // Index value of the column to be used
    
    // Custom comparator for sorting
    CustomComparator? customCompare = comparator;

    // Sort based on the provided comparator or default comparison
    if (customCompare == null) {
      // Sort indices using the custom sort parameter function
      newIndexOrder.sort((a, b) {
        int comparison = _customSortParameter(a, b, colIndex, nullIsFirst);
        return ascending ? comparison : -comparison;  // Adjust for ascending/descending
      });
    } else {
      newIndexOrder.sort((a, b) {
        int comparison = customCompare(this[colName][a], this[colName][b]);
        return ascending ? comparison : -comparison;  // Adjust for ascending/descending
      });
    }
    // Reorder each column of the matrix according to newIndexOrder 
    var newDf = iloc[newIndexOrder]; // iloc returns a df when input is list
    // Handle in-place sorting
    if (inplace) {
      _dataCore.data = newDf._dataCore.data;
      _dataCore.rowIndexMap = newDf._dataCore.rowIndexMap;
      _dataCore.columnIndexMap = newDf._dataCore.columnIndexMap;
      newDf = empty;  // Returns empty if accidentally assigned to sort() when inplace is true
    }

    return newDf;
  }
  // Default comparison function used by the sort() method
  int _customSortParameter(var numberA, var numberB, var colIndex, bool nullIsFirst) {
    var localColumn = _dataCore.data[colIndex];
    var columnValA = localColumn[numberA];
    var columnValB = localColumn[numberB];

    // Helper function to check for null or NaN values
    bool isNullOrNaN(var value) {
      return value == null || (value is double && value.isNaN);
    }

    bool aIsNullOrNaN = isNullOrNaN(columnValA);
    bool bIsNullOrNaN = isNullOrNaN(columnValB);

    // Handle null and NaN cases
    if (aIsNullOrNaN && bIsNullOrNaN) return 0;
    if (aIsNullOrNaN) return nullIsFirst ? -1 : 1;
    if (bIsNullOrNaN) return nullIsFirst ? 1 : -1;

    if (columnValA is Comparable && columnValB is Comparable) {
      // Handle String comparison
      if (columnValA is String && columnValB is String) {
        final firstCharA = columnValA.isNotEmpty ? columnValA[0].toUpperCase() : '';
        final firstCharB = columnValB.isNotEmpty ? columnValB[0].toUpperCase() : '';
        int charComparison = firstCharA.compareTo(firstCharB);
        if (charComparison != 0) return charComparison;
        return columnValA.compareTo(columnValB); // Full string comparison if first characters match
      }
      // Handle number types
      if (columnValA is String && columnValB is num) return -1;
      if (columnValA is num && columnValB is String) return 1;
      // Otherwise, use Comparable's compare method
      return Comparable.compare(columnValA, columnValB);
    } else {
      throw ArgumentError('Value in column cannot be compared');
    }
  }


  // ** DataFrame Utility Methods  **

  /// Resets the row index of the DataFrame to a default integer-based index.
  ///
  /// - Parameters:
  ///   - inplace: If true (default), modifies the original DataFrame. If false, returns a new DataFrame with the reset index.
  ///
  /// - Returns:
  ///   The original DataFrame with the reset index if inplace is true, or a new DataFrame with the reset index if inplace is false.
  ///
  /// - Example:
  ///   ```dart
  ///   df.reset_index(); // Resets the index of the original DataFrame.
  ///   var newDf = df.reset_index(inplace: false); // Returns a new DataFrame with the reset index.
  ///   ```
  reset_index({bool inplace=true}){
    if(inplace){
      _dataCore.reset_index();
      return empty;
    } else {
      DataFrame df = DataFrame._copyDataframe(this);
      df._dataCore.reset_index();
      return df;
    }
  }

  // Helper method when combining data from two DataFrames. Adjusts for type differences.
  void _combineColumnFromDf({required var df2, required int columnIndex1, required int columnIndex2,}){
    if(df2 is! DataFrame && df2 is! List){
      throw ArgumentError('df2 must be either a DataFrame or List');
    }
    if(df2 is! DataFrame && df2 is List){
      df2 = DataFrame(df2);
    }
    if(df2.values.isEmpty){
      return;
    }

    if( (columnIndex1+1 > _dataCore.data.length && columnIndex1 != 0) || _dataCore.data.isEmpty){ // For the simple case of an empty df1
      _dataCore.columnTypes.add(df2.dtypes[columnIndex2]); 
      _dataCore.data.add(df2.values[columnIndex2]); // Enclose in List because normally a List container would already exist.
    }
    else if(_dataCore.columnTypes[columnIndex1] == df2.dtypes[columnIndex2]){
      _dataCore.data[columnIndex1].addAll(df2.values[columnIndex2]);
    } else if(_dataCore.columnTypes[columnIndex1] == double && df2.dtypes[columnIndex2] == int){
      //List tempList = [];
      for (int e in df2.values[columnIndex2]) {
          //tempList.add(e.toDouble());
          _dataCore.data[columnIndex1].add(e.toDouble());
      }       
    } else if(_dataCore.columnTypes[columnIndex1] == int && df2.dtypes[columnIndex2] == double){
      List tempList = <double>[];
      for (int e in _dataCore.data[columnIndex1]) {
          tempList.add(e.toDouble());
      }
      _dataCore.data[columnIndex1] = tempList;
      _dataCore.columnTypes[columnIndex1] = double;
      _dataCore.data[columnIndex1].addAll(df2.values[columnIndex2]); 
    } else if(_dataCore.columnTypes[columnIndex1] == Object || _dataCore.columnTypes[columnIndex1] == dynamic){
      _dataCore.data[columnIndex1].addAll(df2.values[columnIndex2]);
    } else {  //Else, change the listed type, and create new column with the new generic. [This might not be needed, just a failsafe]
      _dataCore.columnTypes[columnIndex1] = Object;
      List newList = <Object>[];
      newList.addAll(_dataCore.data[columnIndex1]);
      _dataCore.data[columnIndex1] = newList;
      _dataCore.data[columnIndex1].addAll(df2.data[columnIndex2]); 
    }
  }

  /// Converts a list's elements to a specified type.
  /// - Parameters:
  ///   - type: The target type.
  ///   - list: The list to convert.
  List astype(Type type, List list) {
    var convertedList = createListFromType(type);
    if (type == double) {
      for (var element in list) {
        // Attempt to convert each element to double
        convertedList.add(element.toDouble());
      }
    } else {
      for (var element in list) {
        // Add elements without conversion
        convertedList.add(element);
      }
    }
    return convertedList;
  }

  // ** Data retrieval methods **

  /// Provides integer‐location based retrieval, slicing, and editing of DataFrame.
  ///
  /// - Parameters:
  ///   - row: (Required) The row index. Map and List provide two additional functions:
  ///          Map: specify and return a contiguous range of rows (start inclusive, end exclusive).
  ///          List: return a DataFrame with rows reordered according to the specified list of integer indices.
  ///   - col: (Optional) The column index. Map and List provide two additional functions:
  ///          Map: specify a contiguous range of columns (start inclusive, end exclusive).
  ///          List: return columns reordered according to the specified list of integer indices.
  ///          Defaults to all columns if not provided or null.
  ///
  /// - Examples:
  ///   ```dart
  ///   var row   = df.iloc[2];                   // Get row at index 2.
  ///   var cell  = df.iloc[1][3];                // Get cell at row 1, column 3.
  ///   df.iloc[1][3] = 42;                       // Set that cell.
  ///   df.iloc[2] = ['Alice', 123, true];        // Replace entire row 2.
  ///   var sub1  = df.iloc[[0, 2]];              // Rows 0 and 2, all columns.
  ///   var sub2 = df.iloc[{null:3}][{1:null}];   // Rows 0–2, columns 1 to last
  ///   var sub3  = df.iloc[{1:4}][[3, 0, 2]];    // Rows 1–3, cols 3,0,2.
  ///   ```
  IlocIndexer get iloc => IlocIndexer(this);

  /// Retrieves rows and columns based on specified names or ranges, with the option to edit data.
  ///
  /// - Parameters:
  ///   - row: (Required) The row name. Map and List provide two additional functions:
  ///          Map: Use to specify and return a range of rows (see ex.2). 
  ///          List: Returns a DataFrame with rows reordered according to the specified list of row names. Must contain all current row names.      
  ///   - col: (Optional) A map specifying the range of columns (NOTE: Includes the end column). Defaults to all columns if not provided or null.
  ///   - edit: (Optional) Value used to update the specified rows/columns.
  ///
  /// - Example:
  ///   ```dart
  ///   var row = df.loc(row: 'RowName'); // Retrieves the row with the name 'RowName'.
  ///   var rangeDf1 = df.loc(row: {'1': '3}, col: {'3': null}); // Assigns a DataFrame from df with rows 1 and 2, and all columns after and including 3.
  ///   var rangeDf2 = df.loc(row: {'1': '3}, col: {'2': '4'}); // Assigns a DataFrame from df with rows 1 and 2, and columns 2, 3, and 4.
  ///   df.loc(row: 'date', col: 2, edit: 'newValue'); // Updates the row with the index name 'date' and column 2 with 'newValue'.
  ///   df.loc(row: 'time', edit: 'newValue'); // Updates all values in the row with the index name 'time' with 'newValue'.
  ///   ```
  loc({required var row, var col, Object? edit}){
    if(row == null){
      throw ArgumentError('Index entered does not exist');
    }
    col ??= {null:null};
    // See if the number exists in index, if it doesn't, check if string version exists, if it doesn't throw error. 
    if(row is int){
      if(!_dataCore.rowIndexMap.containsKey(row)){
        if(!_dataCore.rowIndexMap.containsKey(row.toString())){
          throw ArgumentError('Index entered does not exist');
        }
      }
    }  
    // row is a List    
    else if (row is List) {
      // Ensure the newOrder contains the same row indices as the current DataFrame
      final currentIndices = _dataCore.rowIndexMap.keys.toList();

      if ( !setEquals(currentIndices.toSet(), row.toSet()) || _dataCore.rowLastIndexVal+1 != row.length) { 
        throw ArgumentError('The new row index order must contain the same row indices as the DataFrame');
      }
      // Create a new list to store the reordered data
      final List newData = [];
      final copyCurrentRowIndexMap = _dataCore.rowIndexMap.map(
        (key, value) => MapEntry(key, List.from(value)),
      );
      // Populate the new data and rowIndexMap based on the newOrder
      for (var rowName in row) {
        final List currentRowIndices = copyCurrentRowIndexMap[rowName]!;
          // Extract the corresponding row by iterating over columns
          final List<dynamic> tempRow = [];
          for (var column in _dataCore.data) {
            tempRow.add(column[currentRowIndices.first]);
          }
          // Add the constructed row to newData
          newData.add(tempRow);
          copyCurrentRowIndexMap[rowName]!.removeAt(0);
      }
      // Return a new DataFrame instance with the reordered data and updated rowIndexMap
      return DataFrame(newData, index: row, columns: columns,); 
    }
    // In case user accidentally enters an int index as a String, convert it to an int. Check if string contains only a number.
    else if( row is! Map && (!row.contains(RegExp('[\s\n]')) && int.tryParse(row) != null )){
      if(!_dataCore.rowIndexMap.containsKey(row)){
        final testInt = int.tryParse(row);
        if(_dataCore.rowIndexMap.containsKey(testInt)){
          row = testInt;
        } else{
          throw ArgumentError('Index entered does not exist');
        }
      }
    } 
    // `row` is Map input; need to do previous checks twice for key and value
    else if(row is Map){
      // Make sure the start row name exists
      Map integerIndexToRowName = _dataCore.reverseMap(_dataCore.rowIndexMap);
      Map integerIndexToColName = _dataCore.reverseMap(_dataCore.columnIndexMap);     
      Map lsRow = {}; 
      lsRow = Map.from(row);
      dynamic lsCol = {}; 

      if(row.isEmpty){
        lsRow = {null:null};
      }
      if(col is! Map){
        lsCol = {col:col};
      }
      else if(col.isEmpty){
        lsCol = {null:null};
      } else{
        lsCol = col;
      }
      var rowIndexName1 = lsRow.keys.first == null ?  integerIndexToRowName[0]:lsRow.keys.first; 
      var rowIndexName2 = lsRow.values.first == null ? integerIndexToRowName[_dataCore.rowLastIndexVal]:lsRow.values.first;       
      var colIndexName1 = (lsCol.isNotEmpty && lsCol.keys.first != null) ? lsCol.keys.first : integerIndexToColName[0];
      var colIndexName2 = (lsCol.isNotEmpty && lsCol.values.first != null) ? lsCol.values.first : integerIndexToColName[_dataCore.columnLastIndexVal];

      // Make sure the end row name exists
      if(rowIndexName1 is int){
        if(!_dataCore.rowIndexMap.containsKey(rowIndexName1)){
          if(!_dataCore.rowIndexMap.containsKey(rowIndexName1.toString())){
            throw ArgumentError('Index entered does not exist');
          }
        }
      } else if(!lsRow.keys.contains(RegExp('[\s\n]')) && int.tryParse(rowIndexName1) != null){
        // If entered row name isn't found, check if it is found when converted to an int
        if(!_dataCore.rowIndexMap.containsKey(rowIndexName1)){
          final testInt = int.tryParse(rowIndexName1);
          // if row was found to contain the name after it is converted, re-enter it as an int argument
            if(_dataCore.rowIndexMap.containsKey(testInt)){
              lsRow[testInt] = lsRow[rowIndexName1];
              lsRow.remove(rowIndexName1);
            } else{
              throw ArgumentError('Index entered does not exist');
            }
        }
      }
      if(rowIndexName2 is int){
        if(!_dataCore.rowIndexMap.containsKey(rowIndexName2)){
          if(!_dataCore.rowIndexMap.containsKey(rowIndexName2.toString())){
            throw ArgumentError('Index entered does not exist');
          }
        }
      } else if(!lsRow.values.contains(RegExp('[\s\n]')) && int.tryParse(rowIndexName2) != null){
        // If entered row name isn't found, check if it is found when converted to an int
        if(!_dataCore.rowIndexMap.containsKey(rowIndexName2)){
          final testInt = int.tryParse(rowIndexName2);
          // If row was found to contain the name after it is converted, re-enter it as an int argument
            if(_dataCore.rowIndexMap.containsKey(testInt)){
              lsRow[rowIndexName1] = testInt;
            } else{
              throw ArgumentError('Index entered does not exist');
            }
        }
      }
      if(col.isNotEmpty){ // Make sure the end col name exists
        if(colIndexName1 is int){
          if(!_dataCore.columnIndexMap.containsKey(colIndexName1)){
            if(!_dataCore.columnIndexMap.containsKey(colIndexName1.toString())){
              throw ArgumentError('Index entered does not exist');
            }
          }
        } else if(!lsCol.keys.contains(RegExp('[\s\n]')) && int.tryParse(colIndexName1) != null){
          // If entered col name isn't found, check if it is found when converted to an int
          if(!_dataCore.columnIndexMap.containsKey(colIndexName1)){
            final testInt = int.tryParse(colIndexName1);
            // If col was found to contain the name after it is converted, re-enter it as an int argument
              if(_dataCore.columnIndexMap.containsKey(testInt)){
                lsCol[testInt] = lsCol[colIndexName1];
                lsCol.remove(colIndexName1);
              } else{
                throw ArgumentError('Index entered does not exist');
              }
          }
        }
        if(colIndexName2 is int){
          if(!_dataCore.columnIndexMap.containsKey(colIndexName2)){
            if(!_dataCore.columnIndexMap.containsKey(colIndexName2.toString())){
              throw ArgumentError('Index entered does not exist');
            }
          }
        } else if(!lsCol.values.contains(RegExp('[\s\n]')) && int.tryParse(colIndexName2) != null){
          // If entered row name isn't found, check if it is found when converted to an int
          if(!_dataCore.columnIndexMap.containsKey(colIndexName2)){
            final testInt = int.tryParse(rowIndexName2);
            // If col was found to contain the name after it is converted, re-enter it as an int argument
              if(_dataCore.columnIndexMap.containsKey(testInt)){
                lsCol[colIndexName1] = testInt;
              } else{
                throw ArgumentError('Index entered does not exist');
              }
          }
        }
      } 
      var integerRow1 = lsRow.keys.first == null ? 0:_dataCore.rowIndexMap[lsRow.keys.first].first;
      var integerRow2 = lsRow.values.first == null ? _dataCore.rowLastIndexVal+1: _dataCore.rowIndexMap[lsRow.values.first].first;
      var integerCol1 = 0;
      var integerCol2 = _dataCore.data.length;
      if(col.isNotEmpty){
        integerCol1 = lsCol.keys.first == null ? 0 : _dataCore.columnIndexMap[lsCol.keys.first].first;
        integerCol2 = lsCol.values.first == null ? _dataCore.data.length : _dataCore.columnIndexMap[lsCol.values.first].first;
      }
      // Get the column range from sCol
      int colStart = lsCol.isNotEmpty ? integerCol1 : 0; // Default to the first column if sCol is empty
      int colEnd = lsCol.isNotEmpty ? integerCol2 : _dataCore.data.length - 1; // Select up to and including the given end column
      if(colEnd == _dataCore.columnLastIndexVal+1){ // Needed so that custom end column is included in loop below, but if's the end column is last column, there will be a range error 
        --colEnd; 
      }
      // If edit parameter entered, edit data with no return value.
      if(edit != null){
        for(int i = integerRow1; i < integerRow2; i++){
          for(int j = colStart; j <= colEnd; j++){ 
            _dataCore.addEditType(input: edit, colIndex: j, rowIndex: i);
          }
        }
        return;
      }
      // Create new row data and indices 
      List newRowIndex = [];
      List newColIndex = [];
      List newData = [];
      for(int i = integerRow1; i < integerRow2; i++){
        newRowIndex.add(integerIndexToRowName[i]);
        // Iterate through columns based on the specified range in sCol
        List newDataRow = [];
        for(int j = colStart; j <= colEnd; j++){
          newDataRow.add(_dataCore.data[j][i]);  // Accessing columns selectively within the specified range
        }
        newData.add(newDataRow);
      }
      // Get new column names
      if(col.isNotEmpty){
        for(int j = colStart; j <= colEnd; j++){
            newColIndex.add(integerIndexToColName[j]);
        }
      } else{
        newColIndex = columns;
      }
      return DataFrame(newData, index: newRowIndex, columns: newColIndex);
    }
    // Row is not a Map input
    if(edit != null){
      // If col is a single value
      if(col is! Map){
        for(int m in _dataCore.rowIndexMap[row]){
          for(int m2 in _dataCore.columnIndexMap[col]){
            _dataCore.addEditType(input: edit, colIndex: m2, rowIndex: m);
          }
        }
      } else{
        // If col is is empty, all columns must be edited
        if(col.isEmpty){
          for(int m in _dataCore.rowIndexMap[row]){
            for(int j = 0; j <= _dataCore.columnLastIndexVal; j++){ 
              _dataCore.addEditType(input: edit, colIndex: j, rowIndex: m);
            }
          }
        // If col has ranges
        } else { 
          var colStart = col.keys.first == null ? 0 : _dataCore.columnIndexMap[col.keys.first].first;
          var colEnd = col.values.first == null? _dataCore.columnLastIndexVal:_dataCore.columnIndexMap[col.values.first].first;
          for(int m in _dataCore.rowIndexMap[row]){
            for(int j = colStart; j <= colEnd; j++){ 
              _dataCore.addEditType(input: edit, colIndex: j, rowIndex: m);
            }
          }
        }
      }
      return;
    }
    var colStart = col.keys.first == null ? 0 : _dataCore.columnIndexMap[col.keys.first].first;
    var colEnd = col.values.first == null ? _dataCore.columnLastIndexVal:_dataCore.columnIndexMap[col.values.first].first;
    if(colEnd == _dataCore.columnLastIndexVal+1){ // Needed so that custom end column is included in loop below, but if's the end column is last column, there will be a range error 
        --colEnd; 
    }
    // Return value: Return a List for a single row, return DataFrame for multiple rows
    if( _dataCore.rowIndexMap[row].length > 1 ){
      List multiList = [];
      List newRowIndex = [];
      List newColumnIndex = columns.sublist(colStart, colEnd+1);
      for(var m in _dataCore.rowIndexMap[row]){
        var tempRow = [];
        for(int i = colStart; i <= colEnd; i++){
          tempRow.add(_dataCore.data[i][m]);
        }
        multiList.add(tempRow);
        newRowIndex.add(row);
      }
      return DataFrame(multiList, columns: newColumnIndex, index: newRowIndex );
    }
    List returnList = [];
    for(int i = colStart; i <= colEnd; i++){
      returnList.add(_dataCore.data[i][_dataCore.rowIndexMap[row].first]);
    }
    return returnList;
  }
  
  /// Returns the first N rows of the DataFrame.
  ///
  /// - Parameters:
  ///   - lines: The number of rows to return. Default is 5.
  /// 
  /// - Example:
  ///   ```dart
  ///   var firstRows = df.head(3); // Returns the first 3 rows of the DataFrame.
  ///   ```
  head([int lines = 5]) {
    var l = lines;
    if (length < lines) {
      l = length;
    }
    List dataT = _dataCore.transposeT(_dataCore.data);
    dataT = dataT.sublist(0,l);
    List newIndex = index.sublist(0,l);
    DataFrame df = DataFrame(dataT, index: newIndex, columns: columns);
    return df;
  }
  /// Prints to terminal and returns a List the following summary: column, including index, column name, non-null count, and data type.
  ///
  /// - Parameters
  ///   - verbose: When false, will not print to terminal.
  ///
  /// - Notes:
  ///   - The summary includes the index, column name, number of non-null entries, and the data type for each column.
  ///   - The printed output is formatted to align the columns for readability.
  ///
  /// - Example:
  ///   ```dart
  ///   var columnInfo = df.info(); // Displays a formatted summary of the DataFrame's columns.
  ///   ```
  List info({bool verbose = true}){
    List infoList = [];
    var columnNames = columns;
    for(int i = 0; i < _dataCore.data.length; i++){
      infoList.add([]); // add column List
      infoList[i].add(i); // Add integer index
      infoList[i].add(columnNames[i]); // Add column names
      infoList[i].add('${countNulls(columnNames[i])} non-null');
      infoList[i].add(_dataCore.columnTypes[i]);
    }
    if(verbose == true){
      int maxIndexWidth = 3; // Minimum starting width based on '---'
      int maxColumnNameWidth = 6; // Minimum starting width based on '------'
      int maxNonNullCountWidth = 14; // Minimum starting width based on 'Non-Null Count'

      // Determine the maximum width needed for the index, column name, and non-null count
      for (var column in infoList) {
        String indexString = column[0].toString();
        String columnName = column[1].toString();
        String nonNullCount = column[2].toString();
        if (indexString.length > maxIndexWidth) {
          maxIndexWidth = indexString.length;
        }
        if (columnName.length > maxColumnNameWidth) {
          maxColumnNameWidth = columnName.length;
        }
        if (nonNullCount.length > maxNonNullCountWidth) {
          maxNonNullCountWidth = nonNullCount.length;
        }
      }

      // Print the header
      print(' #${' ' * (maxIndexWidth - 1)}Column${' ' * (maxColumnNameWidth - 6)} Non-Null Count${' ' * (maxNonNullCountWidth - 14)} Dtype');
      print('${'-' * maxIndexWidth} ${'-' * maxColumnNameWidth} ${'-' * maxNonNullCountWidth} -----');

      // Iterate over each column's data and print
      for (var column in infoList) {
        // Extract the data from each column's list
        int index = column[0];
        String columnName = column[1].toString();
        String nonNullCount = column[2].toString();
        String dtype = column[3].toString();

        // Prepare formatted index, column name, and non-null count
        String formattedIndex = index.toString().padLeft((maxIndexWidth + index.toString().length) ~/ 2).padRight(maxIndexWidth);
        String formattedColumnName = columnName.padRight(maxColumnNameWidth);
        String formattedNonNullCount = nonNullCount.padRight(maxNonNullCountWidth);

        // Print the column data
        print('$formattedIndex $formattedColumnName $formattedNonNullCount $dtype');
      }
    }
    return infoList;
  }
  /// Returns the entire DataFrame as a list of rows.
  ///
  /// - Parameters:
  ///   - rowIndex: If true, includes the row index as the first column. Default is false.
  ///   - indexHeader: Custom header name for the row index column. Only used if `rowIndex` is true.
  ///
  /// - Example:
  ///   ```dart
  ///   var tableData = df.table(); // Returns the DataFrame as a list, excluding row index.
  ///   var tableWithIndex = df.table(rowIndex: true, indexHeader: 'Row ID'); // Returns the DataFrame with row index and custom header.
  ///   ```
  List table({bool rowIndex = false, String indexHeader = ''}){
    List table = [];
    List dataT = _dataCore.transposeT(_dataCore.data);
    if(rowIndex == false){
      table.add(columns);
      table.addAll(dataT);
    }
    if(rowIndex == true){
      List rowIndices = index;
      table.add([indexHeader, ...columns]);
      for(int i = 0; i < _dataCore.rowLastIndexVal+1; i++){
        table.add([rowIndices[i]] + dataT[i]);
      }
    }
    return table;
  }

  //* Getters
  
  /// Returns the row indices of the DataFrame.
  List get index => _dataCore.orderedEntries(_dataCore.rowIndexMap, false);
  /// Sets the row index labels for the DataFrame.
  set index(List newRowIndices){
    if(newRowIndices.length != _dataCore.rowLastIndexVal+1){
      throw ArgumentError('Row index does not much');
    }
    _dataCore.indexer(newRowIndices, false, resetIndex: true);
  }
  /// Returns a list of column names.
  List get columns => _dataCore.orderedEntries(_dataCore.columnIndexMap, true);
  /// Sets a new list of column names for the DataFrame. 
  /// Note: Use [rename()] when renaming a single column.
  set columns(List newColumnNames){ 
    if(newColumnNames.length != _dataCore.columnLastIndexVal+1){
      throw ArgumentError('Number of column names entered must match original');
    } else {
      _dataCore.indexer(newColumnNames, true, resetIndex: true);
    }
  }
  /// The raw data of the DataFrame.
  get values => _dataCore.data;

  /// The number of rows in the DataFrame.
  int get length => _dataCore.rowLastIndexVal+1;
  
  /// A List containing the type for each column.
  get dtypes => _dataCore.columnTypes;
  
  // * Overrides 

  /// Implements the [] operator for column access and column reordering.
  ///
  /// - []: If a single column name is provided, returns the data for that column.
  /// - [[]]: If multiple column names is provided inside of a List, returns a new DataFrame with the columns reordered.
  ///
  /// - Example:
  ///   ```dart
  ///   var columnData = df['ColumnName']; // Returns data for a single column.
  ///   var reorderedDf = df[['Col2', 'Col1']]; // Returns a new DataFrame with the columns reordered to Col2, Col1.
  ///   ```
  operator [](var columnName) {
    // If argument is a List of column names, return a new DataFrame that is a reordered by the List 
    if(columnName is List){
      Set columnNames = columnName.toSet();  // Get list of unique column names
      var column = _dataCore.columnIndexMap;
      if(columnNames.every( (e)=> column.containsKey(e))){ // Check that user entered in all the column names
        List newData = [];
        List newColumnName = []; 
        //copy row index
        List newIndex = index.map((i) => i).toList();
        for(var name in columnNames){ // Iterate through the new order of column names
          // Use the names to get the indices from columnIndexMap for the data
          var dataIndex = column[name];
          // Use indices and copy the data columns
          for(int index in dataIndex){
            if(_dataCore.data[index] is List<int>){  
              newData.add(List<int>.from(_dataCore.data[index]));
            } else if(_dataCore.data[index] is List<double>){  
              newData.add(List<double>.from(_dataCore.data[index]));
            } else if(_dataCore.data[index] is List<num>){  
              newData.add(List<num>.from(_dataCore.data[index]));
            } else if(_dataCore.data[index] is List<String>){  
              newData.add(List<String>.from(_dataCore.data[index]));
            } else if(_dataCore.data[index] is List<bool>){  
              newData.add(List<bool>.from(_dataCore.data[index]));
            } else if(_dataCore.data[index] is List<Object>){  
              newData.add(List<Object>.from(_dataCore.data[index]));
            } else{
              newData.add(_dataCore.data[index].map((i)=>i).toList());   
            }
            newColumnName.add(name);
          }
        }
        // convert newData to Map. This is done because Map input in df is faster (List input requires transposeT)
        Map newDataAsMap = Map.fromIterables(newColumnName, newData); 
        // create new Dataframe
        DataFrame df = DataFrame(newDataAsMap, index: newIndex);
        return df;
      } else{
        throw StateError('Invalid column name entered');
      } 
    } else {
      return _dataCore[columnName];
    }
  }

  /// Replaces an entire column of data. 
  /// 
  /// Note: If multiple columns share the same name, they will all be replaced with the same data.
  void operator []=(var columnName, List inputData) {
    _dataCore[columnName] = inputData;
  }

  /// Prints a formatted table of the DataFrame data to the terminal.
  @override
  String toString() {
    final buffer = StringBuffer('\n');

    // 1. DETERMINE MAXIMUM INDEX LENGTH
    // 1.a. Determine the maximum length of the row index for proper alignment. Default to 1 if no data is present.
    int maxIndexLength = _dataCore.rowIndexMap.isNotEmpty
        ? _dataCore.rowIndexMap.keys.map((e) => e.toString().length).fold(0, (max, e) => e > max ? e : max)
        : 1;

    // 2. STORE MAXIMUM COLUMN LENGTHS
    // 2.a. Start with the lengths of the column names
    List columnNames = columns;
    final columnMaxWidth = List<int>.generate(columnNames.length, (i) => columnNames[i].toString().length);

    // 2.b. Iterate through each column's data to find the maximum width for proper formatting
    int counter = -1;
    for (var column in _dataCore.data) {
      ++counter;
      // If column contains single values, compare its length to the stored max width
      if (column is! List && column is! Series) {
        final dataLength = column.toString().length;
        if (columnMaxWidth[counter] < dataLength) {
          columnMaxWidth[counter] = dataLength;
        }
      } else {
        // Iterate through the entire column (which is a List) and adjust the max width
        for (var i = 0; i < column.length; i++) {
          final dataLength = column[i]?.toString().length ?? 0;
          if (columnMaxWidth[counter] < dataLength) {
            columnMaxWidth[counter] = dataLength;
          }
        }
      }
    }

    // 3. CREATE CENTERED COLUMN ELEMENTS
    // 3.a. Function to center-align values based on column width
    String formatValueCentered(String value, int width) {
      int padding = (width - value.length) ~/ 2; // Calculate padding for both sides
      String paddedValue = ' ' * padding + value + ' ' * padding; // Add padding to both sides
      if (paddedValue.length < width) paddedValue += ' '; // Adjust for odd widths
      return paddedValue;
    }

    // 4. FIRST LINE: ADD COLUMN NAMES
    // 4.a. Add a blank space for the row index column with proper alignment
    buffer.write(formatValueCentered('', maxIndexLength) + ' | ');

    // 4.b. Add the column names with proper spacing and alignment
    String columnNamesLine = '';
    for (int i = 0; i < columns.length; i++) {
      if (i > 0) {
        columnNamesLine += ' | ';
      }
      columnNamesLine += formatValueCentered(columns[i].toString(), columnMaxWidth[i]);
    }
    // 4.c. Add the column names to the buffer
    buffer.writeln(columnNamesLine);

    // 5. SECOND LINE: ADD SEPARATOR LINE
    // 5.a. Create a separator line for formatting between headers and data rows
    String separatorLine = '-' * maxIndexLength + '-+-' + columnMaxWidth.map((width) => '-' * width).join('-+-') + '-';
    buffer.writeln(separatorLine);

    // 6. MULTIPLE LINES OF DATA (ROW-WISE)
    // 6.a. Determine the number of rows from the index
    int numRows = index.length;
    List rowIndex = index;

    // 6.b. If no rows are present, print a "No data available" message
    if (numRows == 0) {
      buffer.writeln('No data available');
    } else {
      // 6.c. Iterate over each row and format data for display
      for (int rows = 0; rows < numRows; rows++) {
        List<String> rowData = [];
        // 6.d. For each column in the matrix, retrieve and format data from each row
        for (var columnIndex = 0; columnIndex < _dataCore.data.length; columnIndex++) {
          var value = columnIndex < _dataCore.data.length && rows < _dataCore.data[columnIndex].length
              ? _dataCore.data[columnIndex][rows]?.toString() ?? ''
              : '';
          rowData.add(formatValueCentered(value, columnMaxWidth[columnIndex]));
        }
        // 6.e. Combine row data with row index and formatting
        final indexValue = formatValueCentered(rowIndex[rows].toString(), maxIndexLength);
        buffer.writeln(indexValue + ' | ' + rowData.join(' | '));
      }
    }

    // 7. RETURN THE FINAL FORMATTED STRING
    return buffer.toString();
  }

  // * Private helpers

  // Return a column index
  int _colIndex(Object col) {
    if (col is int) return col;
    if (col is String) return columns.indexOf(col);
    throw ArgumentError('col must be int or String');
  }
}

/// Concatenates multiple DataFrames into a single DataFrame.
/// 
/// - Parameters:
///   - input: (List<DataFrame>) A list of DataFrames to concatenate. The first DataFrame, `df1`, is the base to which subsequent DataFrames are added.
///   - axis: (0 or 1) Determines whether the concatenation is done vertically (by column names, axis = 0) or horizontally (by row index, axis = 1).
///   - join: ('outer' or 'inner') Specifies the join method:
///           - 'inner' returns only rows or columns that are present in all DataFrames.
///           - 'outer' returns all rows or columns, without filtering.
///   - ignore_index: (Optional) If true, ignores the existing index and generates a new one.
/// 
/// - Usage Notes:
///   - axis = 0, join = 'outer': `df1` cannot have non-unique column names unless `df2` has the exact same column names in the same order.
///                               If `df1` has unique column names, `df2` cannot have non-unique column names. Row names (index) can be non-unique.
///   - axis = 0, join = 'inner': `df1` column names must be unique, even if they match the column names in `df2`.
///   - axis = 1, join = 'outer': `df1` and `df2` cannot have non-unique row indices unless they are the exact same in both DataFrames, including the order. Column names can be non-unique.
/// 
/// - Example:
///   ```dart
///   var result = concat([df1, df2], axis: 0, join: 'outer'); // Concatenate vertically (axis = 0) with 'outer' join
///   var result = concat([df1, df2], axis: 1, join: 'inner'); // Concatenate horizontally (axis = 1) with 'inner' join
///   var result = concat([df1, df2, df3], axis: 0, ignore_index: true); // Concatenate multiple DataFrames and ignore the index
///   ```
DataFrame concat(List input, {int axis = 0, String join = 'outer', bool ignore_index = false}) {
  // 1. Validate input type: Ensure all elements in input are DataFrame objects
  if (input.every((e) => e is DataFrame)) {
    // 1.a. If only one DataFrame is provided, return it as-is
    if (input.length == 1) return input.first as DataFrame;
    // 2. Initialize new DataFrame by copying the first DataFrame
    DataFrame newDataFrame = DataFrame._copyDataframe(input.first);
    // 3. CONCATENATE DATA VERTICALLY (axis = 0)
    if (axis == 0) {
      Set joinColumnTracker = input.first._dataCore.columnIndexMap.keys.toSet();  // Track columns for joining
      List df1ColumnNames = input.first.columns;
      // 3.a. Inner join: only keep columns present in all DataFrames
      if (join == 'inner' && !(df1ColumnNames.length != df1ColumnNames.toSet().length)) {
        for (var df in input.skip(1)) {
          joinColumnTracker = joinColumnTracker.intersection(df._dataCore.columnIndexMap.keys.toSet());
        } 
        if (joinColumnTracker.isNotEmpty) {
          // Clear DataFrame while retaining some metadata
          newDataFrame._dataCore.clear(except: {'rowLastIndexVal', 'rowIndexMap', 'columnTypes'});
          // Add columns based on the inner join of common columns
          newDataFrame._dataCore.indexer(joinColumnTracker, true);
          // Temporarily store new column types
          List<Type> newTempColumnTypes = <Type>[];
          for (var columnName in joinColumnTracker) {
            int colIndex = input[0]._dataCore.columnIndexMap[columnName].first;
            newDataFrame._dataCore.data.add(input[0]._dataCore.data[colIndex]);
            newTempColumnTypes.add(input[0]._dataCore.columnTypes[colIndex]);
          }
          // Add data from subsequent DataFrames based on common columns
          for (var df in input.skip(1)) {
            newDataFrame._dataCore.indexer(df.index, false);
            for (var columnName in joinColumnTracker) {
              int colIndex1 = newDataFrame._dataCore.columnIndexMap[columnName].first;
              int colIndex2 = df._dataCore.columnIndexMap[columnName].first;
              newDataFrame._combineColumnFromDf(df2: df, columnIndex1: colIndex1, columnIndex2: colIndex2);
            }
          }
          // Update column types for the new DataFrame
          newDataFrame._dataCore.columnTypes = newTempColumnTypes;
        } else {
          // Clear the DataFrame if no columns match, add only the row index
          newDataFrame._dataCore.clear(except: {'rowLastIndexVal', 'rowIndexMap'});
          for(DataFrame e in input.skip(1)){
            newDataFrame._dataCore.indexer(e.index, false);
          }
        }

      } else {
        // 3.b. Standard operation: check for non-unique column names
        for (var df in input.skip(1)) {
          if (df1ColumnNames.length != df1ColumnNames.toSet().length) {
            if (join == 'inner') {
              throw ArgumentError("Column names cannot contain non-unique values when axis:0 and join:'inner' is used");
            }
            if (areListsEqual(input.first.columns, df.columns)) {
              newDataFrame._dataCore.indexer(df.index, false);
              for (int i = 0; i < df._dataCore.data.length; i++) {
                newDataFrame._combineColumnFromDf(df2: df, columnIndex1: i, columnIndex2: i);
              }
            } else {
              throw ArgumentError('Index cannot contain non-unique values for this operation');
            }
          } else if (df.columns.length != df.columns.toSet().length) {
            throw ArgumentError('Secondary DataFrame column names cannot contain non-unique values for this operation');
          } else {
            // 3.c. Standard operation: unique column names
            newDataFrame._dataCore.indexer(df.index, false);
            // Add data from subsequent DataFrames, filling in missing columns with NaN
            Set<Object> keysThatWereUsed = {};
            var dfColumnStartPoint = newDataFrame._dataCore.rowLastIndexVal;
            for (var key in newDataFrame._dataCore.columnIndexMap.keys) {
              var colIndex1 = newDataFrame._dataCore.columnIndexMap[key].first;
              if (df._dataCore.columnIndexMap.containsKey(key)) {
                var colIndex2 = df._dataCore.columnIndexMap[key].first;
                keysThatWereUsed.add(key);
                newDataFrame._combineColumnFromDf(df2: df, columnIndex1: colIndex1, columnIndex2: colIndex2);
              } else {
                for (int i = 0; i < df._dataCore.rowLastIndexVal + 1; i++) {
                  newDataFrame._dataCore.addEditType(input: double.nan, colIndex: colIndex1);
                }
              }
            }
            // Add new columns from df if not already in newDataFrame
            for (var key in df._dataCore.columnIndexMap.keys) {
              if (keysThatWereUsed.contains(key)) {
                continue;
              }
              var colIndex = df._dataCore.columnIndexMap[key].last;
              List columnToBeAdded;
              if (df._dataCore.columnTypes[colIndex] == int) {
                List tempColumn = <double>[];
                for (int e in df._dataCore.data[colIndex]) {
                  tempColumn.add(e.toDouble());
                }
                columnToBeAdded = <double>[];
                newDataFrame._dataCore.columnTypes.add(double);
              } else if (df._dataCore.columnTypes[colIndex] == double) {
                columnToBeAdded = <double>[];
                newDataFrame._dataCore.columnTypes.add(double);
              } else {
                columnToBeAdded = <Object>[];
                newDataFrame._dataCore.columnTypes.add(Object);
              }
              newDataFrame._dataCore.indexer([key], true);
              newDataFrame._dataCore.data.add(columnToBeAdded);
              var newColumnName = newDataFrame._dataCore.columnIndexMap[key].first;
              var dfColIndex = df._dataCore.columnIndexMap[key].first;
              dfColumnStartPoint = (newDataFrame._dataCore.rowLastIndexVal - df._dataCore.rowLastIndexVal) as int;
              for (int k = 0; k <= newDataFrame._dataCore.rowLastIndexVal; k++) {
                if (k < dfColumnStartPoint) {
                  newDataFrame._dataCore.data[newColumnName].add(double.nan);
                } else {
                  if (df._dataCore.columnTypes[dfColIndex] == int) {
                    newDataFrame._dataCore.data[newColumnName].add(df._dataCore.data[dfColIndex][k - dfColumnStartPoint].toDouble());
                  } else {
                    newDataFrame._dataCore.data[newColumnName].add(df._dataCore.data[dfColIndex][k - dfColumnStartPoint]);
                  }
                }
              }
            }
          }
        }
      }
    }
    // 4. CONCATENATE DATA HORIZONTALLY (axis = 1)
    else if (axis == 1) {
      Set joinRowsTracker = input.first._dataCore.rowIndexMap.keys.toSet();
      Map df1RowIndexMap = newDataFrame._dataCore.rowIndexMap;
      // 4.a. Inner join: keep only rows common to all DataFrames
      if (join == 'inner') {
        for (var df in input.skip(1)) {
          joinRowsTracker = joinRowsTracker.intersection(df._dataCore.rowIndexMap.keys.toSet());
        }
        if (joinRowsTracker.isNotEmpty) {
          newDataFrame._dataCore.clear();
          newDataFrame._dataCore.indexer(joinRowsTracker, false);
          for (var df in input) {
            newDataFrame._dataCore.indexer(df.columns, true);
            List tempData = [];
            for (int i = 0; i < df._dataCore.data.length; i++) {
              tempData.add(createListFromType(df._dataCore.columnTypes[i]));
            }
            for (var i in joinRowsTracker) {
              var rowIndex = df._dataCore.rowIndexMap[i].first;
              for (int j = 0; j < df._dataCore.data.length; j++) {
                tempData[j].add(df._dataCore.data[j][rowIndex]);
              }
            }
            newDataFrame._dataCore.data.addAll(tempData);
            newDataFrame._dataCore.columnTypes.addAll(df._dataCore.columnTypes);
          }
        } else {
          newDataFrame._dataCore.clear(except: {'columnLastIndexVal', 'columnIndexMap'});
        }
      } else {
        // 4.b. Outer join: Add df2's columns to df1, ensuring rows match as much as possible
        for (var df in input.skip(1)) {
          newDataFrame._dataCore.indexer(df.columns, true);
          var df1Index = newDataFrame._dataCore.rowIndexMap.keys;
          var df2Index = df._dataCore.rowIndexMap.keys;

          if (areIterablesEqualUnordered(df1Index, df2Index)) {
            newDataFrame._dataCore.data.addAll(df._dataCore.data);
            newDataFrame._dataCore.columnTypes.addAll(df._dataCore.columnTypes);
          } else {
            if (df == input[1]) {
              for (int i = 0; i < newDataFrame._dataCore.columnTypes.length; i++) {
                if (newDataFrame._dataCore.columnTypes[i] == int) {
                  List tempList = <double>[];
                  for (int e in newDataFrame._dataCore.data[i]) {
                    tempList.add(e.toDouble());
                  }
                  newDataFrame._dataCore.data[i] = tempList;
                  newDataFrame._dataCore.columnTypes[i] = double;
                } else if (newDataFrame._dataCore.columnTypes[i] != Object || newDataFrame._dataCore.columnTypes[i] != double) {
                  List tempList = <Object>[];
                  tempList.addAll(newDataFrame._dataCore.data[i]);
                  newDataFrame._dataCore.data[i] = tempList;
                  newDataFrame._dataCore.columnTypes[i] = Object;
                }
              }
            }
            for (int i = 0; i < df.columns.length; i++) {
              if (df._dataCore.columnTypes[i] == int || df._dataCore.columnTypes[i] == double) {
                newDataFrame._dataCore.data.add(List<double>.filled(newDataFrame.index.length, double.nan, growable: true));
                newDataFrame._dataCore.columnTypes.add(double);
              } else if (df._dataCore.columnTypes[i] != Object || df._dataCore.columnTypes[i] != double) {
                newDataFrame._dataCore.data.add(List<Object>.filled(newDataFrame.index.length, double.nan, growable: true));
                newDataFrame._dataCore.columnTypes.add(Object);
              }
            }
            List df2Index = df.index;
            var dfColStartPosition = (newDataFrame._dataCore.columnLastIndexVal - df._dataCore.columnLastIndexVal) as int;
            for (var key in df2Index) {
              if (df1RowIndexMap.containsKey(key)) {
                int df1rowIndex = df1RowIndexMap[key].first;
                for (int j = 0; j < df.columns.length; j++) {
                  if (df._dataCore.columnTypes[j] == int) {
                    newDataFrame._dataCore.data[dfColStartPosition + j][df1rowIndex] = df._dataCore.data[j][df._dataCore.rowIndexMap[key].first].toDouble();
                  } else {
                    newDataFrame._dataCore.data[dfColStartPosition + j][df1rowIndex] = df._dataCore.data[j][df._dataCore.rowIndexMap[key].first];
                  }
                }
              } else {
                newDataFrame._dataCore.indexer([key], false);
                for (int k = 0; k < newDataFrame._dataCore.columnLastIndexVal + 1; k++) {
                  if (k < dfColStartPosition) {
                    newDataFrame._dataCore.data[k].add(double.nan);
                  } else {
                    if (df._dataCore.columnTypes[k - dfColStartPosition] == int) {
                      newDataFrame._dataCore.data[k].add(df._dataCore.data[k - dfColStartPosition][df._dataCore.rowIndexMap[key].first].toDouble());
                    } else {
                      newDataFrame._dataCore.data[k].add(df._dataCore.data[k - dfColStartPosition][df._dataCore.rowIndexMap[key].first]);
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    // 5. Optionally reset index if ignore_index is true
    if (ignore_index == true) {
      newDataFrame.reset_index();
    }
    // 6. Return the concatenated DataFrame
    return newDataFrame;
  } else {
    throw ArgumentError('Invalid input type; must be a DataFrame');
  }
}

// A function that compares two objects for sorting. It will return -1 if a
// should be ordered before b, 0 if a and b are equal wrt to ordering, and 1
// if a should be ordered after b.
typedef CustomComparator = int Function(Object? a, Object? b);



// * iloc row view/edit proxies

class IlocIndexer {
  final DataFrame df;
  IlocIndexer(this.df);
  // df.iloc2[key]
  // - int             - IlocRow proxy for single‐row access
  // - List<int>       - DataFrame(rows=that list, all columns)
  // - Map<int?,int?>  - IlocSlice proxy for slicing
  dynamic operator [](dynamic key) {
    // 1) single-row proxy
    if (key is int) {
      return IlocRow(df, key);
    }

    // 2. list of row‐indices: DataFrame(rows=key, all cols)
    if (key is List && key.every((e) => e is int)) {
      final rev     = df._dataCore.reverseMap(df._dataCore.rowIndexMap);
      final newIdx  = <dynamic>[];
      final newData = <List<dynamic>>[];
      for (var r in key.cast<int>()) {
        newIdx.add(rev[r]);
        newData.add(df._dataCore.data.map((col) => col[r]).toList());
      }
      return DataFrame(newData, index: newIdx, columns: df.columns);
    }
    // 3. map‐based row slice: IlocSlice for columns next
    if (key is Map && key.length == 1) {
      final rawStart = key.keys.first  as int?;
      final rawEnd   = key.values.first as int?;
      final start = rawStart ?? 0;
      final end   = rawEnd   ?? (df._dataCore.rowLastIndexVal + 1);
      final total = df._dataCore.rowLastIndexVal + 1;
      if (start < 0 || end > total || start > end) {
        throw ArgumentError('Invalid row range: {$rawStart:$rawEnd}');
      }
      return IlocSlice(df, start, end);
    }

    throw ArgumentError('Invalid iloc2 key: $key');
  }

  // df.iloc[key] = value
  // - if key is int:  assign entire row (value must be List)
  // - if key is List<int>: assign multiple rows (value must be List<List>)
  void operator []=(dynamic key, dynamic value) {
    // a) multi‐row assignment
    if (key is List && key.every((e) => e is int) && value is List) {
      final rows    = key.cast<int>();
      final newRows = value as List;
      final nCols   = df._dataCore.data.length;
      if (rows.length != newRows.length) {
        throw ArgumentError(
          'Number of target rows (${rows.length}) must match number of newRows (${newRows.length})'
        );
      }
      for (var i = 0; i < rows.length; i++) {
        final r       = rows[i];
        final rowVals = newRows[i];
        if (rowVals is! List || rowVals.length != nCols) {
          throw ArgumentError('Each new row must be a List of length $nCols');
        }
        for (var c = 0; c < nCols; c++) {
          df._dataCore.data[c][r] = rowVals[c];
        }
      }
      return;
    }
    // b) single‐row assignment
    if (key is int && value is List) {
      final cols = df._dataCore.data;
      if (value.length != cols.length) {
        throw ArgumentError('New row must match column count');
      }
      for (var c = 0; c < cols.length; c++) {
        df[df.columns[c]][key] = value[c];
      }
      return;
    }
    throw ArgumentError('Invalid iloc2 assignment: $key');
  }
}

// Proxy used for re-ordering and slices, e.g. df.iloc[{r0:r1}][...]
class IlocSlice {
  final DataFrame df;
  final int rowStart, rowEnd; // end‐exclusive
  IlocSlice(this.df, this.rowStart, this.rowEnd);
  DataFrame operator [](dynamic colKey) {
    // 1. column slice via Map<int?,int?>
    if (colKey is Map && colKey.length == 1) {
      final rawC0 = colKey.keys.first  as int?;
      final rawC1 = colKey.values.first as int?;
      final c0 = rawC0 ?? 0;
      final c1 = rawC1 ?? (df._dataCore.columnLastIndexVal + 1);
      final nRows = df._dataCore.data[0].length;
      final nCols = df._dataCore.data.length;
      if (rowStart < 0 || rowEnd > nRows || c0 < 0 || c1 > nCols || c0 > c1) {
        throw ArgumentError('Invalid column range: {$rawC0:$rawC1}');
      }
      final out = <List<dynamic>>[];
      for (var r = rowStart; r < rowEnd; r++) {
        final buf = <dynamic>[];
        for (var c = c0; c < c1; c++) {
          buf.add(df._dataCore.data[c][r]);
        }
        out.add(buf);
      }
      final newIdx  = df.index.sublist(rowStart, rowEnd);
      final newCols = df.columns.sublist(c0, c1);
      return DataFrame(out, index: newIdx, columns: newCols);
    }

    // 2. reordered columns via List<int>
    if (colKey is List && colKey.every((e) => e is int)) {
      final colsList = colKey.cast<int>();
      final nCols    = df._dataCore.data.length;
      for (var c in colsList) {
        if (c < 0 || c >= nCols) {
          throw ArgumentError('Column index out of bounds: $c');
        }
      }
      final out = <List<dynamic>>[];
      for (var r = rowStart; r < rowEnd; r++) {
        final buf = <dynamic>[];
        for (var c in colsList) {
          buf.add(df._dataCore.data[c][r]);
        }
        out.add(buf);
      }
      final newIdx  = df.index.sublist(rowStart, rowEnd);
      final newCols = colsList.map((c) => df.columns[c]).toList();
      return DataFrame(out, index: newIdx, columns: newCols);
    }
    throw ArgumentError('Invalid column key: $colKey');
  }
}

// Proxy for cell read/write, e.g. df.iloc[3][5], df.iloc[3][5] = newValue
class IlocRow extends ListBase<dynamic> {
  final DataFrame df;
  final int row;
  IlocRow(this.df, this.row);

  @override int get length => df._dataCore.data.length;
  @override set length(int _) => throw UnsupportedError('Cannot resize iloc row');
  @override dynamic operator[](int col) => df._dataCore.data[col][row];
  @override void operator[]=(int col, dynamic value) {
    final colName = df.columns[col];
    if (value.runtimeType != df.dtypes[col]) {
      df._dataCore.addEditType(
        input: value,
        colIndex: col,
        rowIndex: row,
      );
    } else {
      df[colName][row] = value;
    }
  }
}