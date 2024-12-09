import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'package:csv/csv.dart';
import 'data_core.dart';
import 'series.dart';
import 'dfunctions.dart';

/// The main dataframe class
class DataFrame {
  
  // * Fields *
  final DataFrameCore _matrix = DataFrameCore();
  get matrix { return _matrix;}   /// edit this out
  // * Constructors * 

  /// Main constructor for the DataFrame class.
  ///  
  /// - Parameters: 
  ///   - inputData: The primary data for the DataFrame. It can be a `List` or `Map`. 
  ///     For a `List`, each element represents a row of data in a table. For a `Map`, the key will be
  ///     the column name and the value is a `List` that represents the column of data.
  ///   - columns: Optional. A list of column labels. If not provided, columns will be auto-generated.
  ///   - index: Optional. A list of row labels. If not provided, row indices will be auto-generated.
  /// 
  /// Example: var df = DataFrame([[1,2,3],[4,5,6],[7,8,9]], columns: ['a','b',0]); 
  // This constructor processes different input types (List, Map, or Series) and normalizes them 
  // to ensure that the internal matrix (_matrix) is structured correctly for further operations.
  DataFrame( var inputData, {List columns = const [], List index = const []}) {
    // Handle empty data (with column names), List, and Map input
    if (inputData == null || 
        (inputData is Iterable && inputData.isEmpty) ||
        (inputData is List && inputData.every((element) => element is List && element.isEmpty))) {
      if (columns.isNotEmpty) { 
        _matrix.indexer(columns, true);
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
  /// Helper method for processing List input in the DataFrame constructor
  //  Note: Data needs to be normalized (no missing data for rows/columns) first before indexer is called to determine correct max length 
  void _processList(var inputData, List columns, List index) {
    // 1. DATA PROCESSING 
    // 1.a. List type check: If List elements are primitives, encapsulate them in a List, then proceed as usual.
    if(inputData.every((element) => element is! List && element is! Series)){ 
      inputData = inputData.map( (e) => [e]).toList(growable:true);
    }
    if(inputData.every((element) => element is List && element.every((subElement) => subElement is! List))) {  //Ensure inputData is a 2D matrix (List<List>) or throw error
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
      var transposedData = _matrix.transposeT(inputData, checkType: true);

      //1.f. ADD DATA
      _matrix.data.addAll(transposedData);
      
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
      _matrix.indexer(columns, true);

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
      _matrix.indexer(index, false);
    }
    else{throw ArgumentError('Input not a valid type');}
  }
  /// Helper method for processing Map input in the DataFrame constructor
  void _processMap(var inputData, List columns, List index) {
    // 1. DATA VALIDATION
    // 1.a. Ensure columns argument is not specified when using a Map type. 
    //      When inputData is a Map, the keys are used as column names, so specifying columns is redundant.
    if (columns.isNotEmpty) {
      throw ArgumentError('Cannot specify column names when using Map type as keys are used in place');
    }

    // 2. INITIALIZE COLUMNS - Add column names from the Map keys. Map requires k/v, no need to check column names to number of columns.
    _matrix.indexer(inputData.keys, true);

    // 3. DETERMINE ROW LENGTH
    // 3.a. Find the longest column (List length) from the Map values. This determines the number of rows for the DataFrame.
    for (var value in inputData.values) {
      if (value is List && (value.length - 1) > _matrix.rowLastIndexVal) {
        _matrix.rowLastIndexVal = value.length - 1;
      }
    }
    // 3.b. If no List values are found (only primitives), set row length to 0
    if (_matrix.rowLastIndexVal == -1) _matrix.rowLastIndexVal = 0;

    // 3.c. Validate that all Lists in Map values are of equal length. Throw an error if the lengths are inconsistent.
    for (var kv in inputData.entries) {
      if (kv.value is List && kv.value.length != _matrix.rowLastIndexVal + 1) {
        throw ArgumentError('Column data entries must be the same size');
      } else if (kv.value is! List && _matrix.rowLastIndexVal > 0) {
        throw ArgumentError('Column data entries must be the same size');
      }
    }

    // 4. ADD DATA TO MATRIX
    // 4.a. Populate _matrix.data with the values from the Map. If a value is a List, add it directly. If primitives, wrap it in a List.
    for (var value in inputData.values) {
      if (value is List) {
        _matrix.data.add(value);
      } else {
        List p = createListFromType(value.runtimeType);
        p.add(value);
        _matrix.data.add(p);
      }
    }

    // 5. DETERMINE COLUMN TYPES - Add type information for each column
    for (var column in _matrix.data) {
      _matrix.columnTypes.add(checkListType(column));
    }

    // 5.b. Convert columns of 'num' type to 'double'. Ensures numerical consistency across the DataFrame.
    int numIndex = _matrix.columnTypes.indexOf(num);
    if (numIndex != -1) {
      _matrix.columnTypes[numIndex] = double;
      List<double> tempList = [];
      for (num e in _matrix.data[numIndex]) {
        tempList.add(e.toDouble());
      }
      _matrix.data[numIndex] = tempList;
    }

    // 6. INITIALIZE ROW INDICES
    // 6.a. Validate the index size matches the number of rows
    if (index.isNotEmpty && index.length != inputData.values.first.length) {
      throw ArgumentError('Index must match number of rows');
    }
    
    // 6.b. Auto-generate row indices if not provided
    if (index.isEmpty) {
      index = List.generate(_matrix.data.first.length, (i) => i);
    }

    // 6.c. Add row indices to the matrix
    //      - Reset index to account for earlier increment.
    _matrix.indexer(index, false, resetIndex: true);
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
      _matrix.indexer(columns, true);

      // 2. Add data
      if(inputData.values[0] is! List){
        for(int i = 0; i < inputData.values.length; i++){   
          var tempColumn = createListFromType(inputData.values[i].runtimeType);
          tempColumn.add(inputData.values[i]);
          _matrix.data.add(tempColumn);
        }
      } else{
        _matrix.data = inputData.values;
      }
      //2.d. Add type info
      for(var column = 0; column <_matrix.data.length; column++){
        Type tempType = checkListType(_matrix.data[column]);
  
        if(tempType == num){
          tempType = double;
          var newListType = astype(double,_matrix.data[column]);
          _matrix.data[column] = newListType;
        }
        _matrix.columnTypes.add(tempType);
      }
      // 3. Add row index
      if(index.isNotEmpty && index.length != inputData.values.first.length) { throw ArgumentError('Index must match number of rows');}
      
      if(index.isEmpty){index = List.generate(_matrix.data.first.length, (i) => i);} //Currently, index is always empty
      _matrix.indexer(index, false, resetIndex: true); //resetIndex because it was incremented earlier
      
  }
  /// Create a copy of a DataFrame
  DataFrame._copyDataframe(DataFrame df) {
    _matrix.columnIndexMap = Map.from(df._matrix.columnIndexMap).map((key, value) => MapEntry(key, List<int>.from(value)));
    _matrix.rowIndexMap = Map.from(df._matrix.rowIndexMap).map((key, value) => MapEntry(key, List<int>.from(value)));
    _matrix.rowLastIndexVal = df._matrix.rowLastIndexVal;
    _matrix.columnLastIndexVal = df._matrix.columnLastIndexVal;
    _matrix.columnTypes = List.from(df._matrix.columnTypes);
    var counter = 0;
    for(var type in _matrix.columnTypes){
        if(type == int){
          _matrix.data.add(List<int>.from(df._matrix.data[counter]));
        } else if(type == double){
          _matrix.data.add(List<double>.from(df._matrix.data[counter]));
        } else if(type == String){
          _matrix.data.add(List<String>.from(df._matrix.data[counter]));
        } else if(type == bool){
          _matrix.data.add(List<bool>.from(df._matrix.data[counter]));
        } else {
          _matrix.data.add(List<Object>.from(df._matrix.data[counter]));
        }
        counter++;
    }
  }
  
  /// Creates an empty DataFrame
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
    var matrix = _matrix;
    var indexMap = axis == 0 ? _matrix.rowIndexMap : _matrix.columnIndexMap;
    var data = _matrix.data;
    var columnTypes = _matrix.columnTypes;

    //Create copy of object if inplace parameter is false
    if(inplace == false) {
      df = DataFrame._copyDataframe(this);
      matrix = df._matrix;
      indexMap = axis == 0 ? df._matrix.rowIndexMap : df._matrix.columnIndexMap;
      data = df._matrix.data;
      columnTypes = df._matrix.columnTypes;
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
          matrix.editIndices(indexName: input, isColumn: false, select: select);
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
          matrix.editIndices(indexName: input, isColumn: true, select: select);
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
          matrix.editIndices(indexName: input, isColumn: false);
          if(data[0].isEmpty){
            columnTypes.clear();
          }
        } else if (axis == 1) {
          for(var colIndex in reversedIndex){ // Iterate backwards
              data.removeAt(colIndex);    
          }
          matrix.editIndices(indexName: input, isColumn: true);
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
    var currentRowMap = _matrix.rowIndexMap;
    var currentColMap = _matrix.columnIndexMap;
    DataFrame df = empty;
    
    // 2.b. Create a copy of the DataFrame if 'inplace' is false
    //      - 'inplace=false' means changes are made on a new DataFrame, not the original.
    if (inplace == false) {
      df = DataFrame._copyDataframe(this);
      currentRowMap = df._matrix.rowIndexMap;
      currentColMap = df._matrix.columnIndexMap;
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
      int indexPositionToMove = df._matrix.rowIndexMap[data.keys.first][select.keys.first - 1];
      int newIndexPosition = df._matrix.rowIndexMap[data.values.first][select.values.first - 1];
      // 3.e. Create a new matrix to hold the reordered data
      List newMatrix = [];
      // Create a list of indices representing the current order
      List<int> indices = List<int>.generate(df._matrix.data[0].length, (i) => i);
      // Remove the index of the row to move
      indices.removeAt(indexPositionToMove);
      // Insert the index at the new position
      indices.insert(newIndexPosition, indexPositionToMove);

      for (int i = 0; i < df._matrix.data.length; i++) {
        newMatrix.add(createListFromType(df._matrix.columnTypes[i]));
        for (int idx in indices) {
          newMatrix[i].add(df._matrix.data[i][idx]);
        }
      }
      // 3.f. Update the DataFrame's data with the new matrix
      df._matrix.data = newMatrix;
      // 3.g. Adjust the row index accordingly
      df._matrix.editIndices(indexName: data.keys.first,isColumn: false,select: select.keys.first,moveTo: newIndexPosition);

      // Return the modified DataFrame if 'inplace' is false
      if (inplace == false) {
        return df;
      }
    } else {
      // 4. HANDLE LIST INPUT (REORDERING ROWS)
      // 4.a. Verify that all elements in 'data' exist in the current row index map
      // 4.b. Create a new row index map with empty lists for each entry
      var newRowIndexMap = Map.fromEntries(
        df._matrix.rowIndexMap.entries.map((entry) => MapEntry(entry.key, <int>[])),
      );
      // 4.c. Store the original lengths of each row's values
      List<int> listLengths1 = List<int>.from(df._matrix.rowIndexMap.values.map((e) => e.length));
      // 4.d. Transpose the current data matrix for easier row reordering
      List dataT = df._matrix.transposeT(df._matrix.data);
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
              (dataT[df._matrix.rowIndexMap[rowName]![values]]);
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
        df._matrix.data = df._matrix.transposeT(newDataT);
        df._matrix.rowIndexMap = newRowIndexMap;
      }
    }
    // Return the modified DataFrame if 'inplace' is false
    if (inplace == false) {
      return df;
    }
    return;
  }

  /// Appends new rows to the DataFrame
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
      //Make copy of instance _matrix and use it instead so that original is not modified
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
  /// Helper method for append(), processing List type.
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
        lists = _matrix.transposeT([lists]);
        // Add Data. Check if it's a List of primitives or a List of Lists.
        for(int i = 0; i < df1._matrix.columnLastIndexVal+1; i++){
            var tempElement;
            try {
              tempElement = lists[i].first;
            } catch (e) {
              tempElement = double.nan;
            }
            df1._matrix.addEditType(input: tempElement, colIndex: i);  
        }
      }
        df1._matrix.indexer(List<int>.generate(newRows.length, (i) => i), false); // Update row index
        if(ignore_index == true){
          List newIndex = List.generate(df1._matrix.rowLastIndexVal+1, (i) => i, growable: true); 
          df1._matrix.indexer(newIndex, false, resetIndex: true);
        }
    }
  }
  /// Helper method for append(), processing Map type.
  void _mapAppend(Map newRows, DataFrame df1, {List index = const [], List columns = const [], bool ignore_index = false, bool inplace = false,}) {
      if(ignore_index == false){
        throw ArgumentError('ignore_index must be set to true for Map input');
      }
      // Check for new keys (column labels)   
      var oldKeys = df1._matrix.columnIndexMap.keys;
      var newKeys = newRows.keys.toSet();
      df1._matrix.indexer([0], false);

      for(var key in oldKeys){
        for(int i =0; i<df1._matrix.columnIndexMap[key].length; i++){
          var columnElement =  newKeys.contains(key) ? newRows[key] : double.nan;
          df1._matrix.addEditType(input: columnElement, colIndex: df1._matrix.columnIndexMap[key][i]);
        }
      }
      // For the keys that weren't contained, make a new column and add the new column name. add column with NaN
      List newUniqueKeys = newKeys.where( (key)=> !oldKeys.contains(key)).toList();
      if(newUniqueKeys.isNotEmpty){
        for(var key in newUniqueKeys){
          df1._matrix.columnTypes = List.from(df1._matrix.columnTypes, growable: true); // dart thinks columnTypes is fixed
          df1._matrix.indexer([key], true);
          for(int i=0; i<df1._matrix.rowLastIndexVal; i++){
            df1._matrix.addEditType(input: double.nan, colIndex: df1._matrix.columnLastIndexVal);
          }
          df1._matrix.addEditType(input: newRows[key], colIndex: df1._matrix.columnLastIndexVal);
        }      
      }
      if(ignore_index == true){
        List newIndex = List.generate(df1._matrix.rowLastIndexVal+1, (i) => i);
        df1._matrix.indexer(newIndex, false, resetIndex: true);
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
          df1._matrix.addEditType(input: newRows[indexNames[i]], colIndex: i);
        }
        df1._matrix.indexer([newRows.name], false);
      // Update the DataFrame index if ignore_index is true and Series 'name' parameter was not entered
      if (ignore_index == true && (newRows.name is String && newRows.name.isEmpty)) {
        List newIndex = List.generate(df1._matrix.rowLastIndexVal + 1, (i) => i);
        df1._matrix.indexer(newIndex, false, resetIndex: true);
      }
    }

  // ** CSV read and write methods **

  /// Reads a CSV file from a local file path or a remote data string, and returns a DataFrame.
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
    List newIndexOrder = List.generate(_matrix.data[0].length, (i) => i);

    // Determine the column index to use
    var colIndex = _matrix.columnIndexMap[colName][select - 1]; // Index value of the column to be used
    
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
    var newDf = iloc(row: newIndexOrder); // iloc returns a df when input is list

    // Handle in-place sorting
    if (inplace) {
      _matrix.data = newDf._matrix.data;
      _matrix.rowIndexMap = newDf._matrix.rowIndexMap;
      _matrix.columnIndexMap = newDf._matrix.columnIndexMap;
      newDf = empty;  // Returns empty if accidentally assigned to sort() when inplace is true
    }

    return newDf;
  }
  /// Default comparison function used by the sort() method
  int _customSortParameter(var numberA, var numberB, var colIndex, bool nullIsFirst) {
    var localColumn = _matrix.data[colIndex];
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

  /// Counts the number of null or NaN values in a specified column.
  ///
  /// - Parameters:
  ///   - colName: The name of the column to count null/NaN values.
  ///
  /// - Example:
  ///   ```dart
  ///   int nullCount = df.countNulls('ColumnName'); // Counts null/NaN values in the 'ColumnName' column.
  ///   ```
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
  /// 
  /// - Example:
  ///   ```dart
  ///   double totalSum = df.sumCol(0); // Sums the values in the first column.
  ///   ```
  double sumCol(int columnIndex) {
    return _matrix.filterNulls(columnIndex)
        .reduce((total, val) => total + val); // Sum non-null values in the column
  }

  /// Calculates the mean (average) of the values in a specified column.
  /// 
  /// - Example:
  ///   ```dart
  ///   double average = df.mean(0); // Calculates the mean of the first column.
  ///   ```
  double mean(int columnIndex) {
    return sumCol(columnIndex) / _matrix.filterNulls(columnIndex).length; // Divide the sum by the count of non-null values
  }

  /// Returns the maximum value in a specified column.
  ///
  /// - Example:
  ///   ```dart
  ///   double maxValue = df.max(0); // Gets the maximum value in the first column.
  ///   ```
  double max(int columnIndex) {
    return _matrix.filterNulls(columnIndex).reduce((a, b) => a > b ? a : b); // Find the maximum of the non-null values
  }

  /// Returns the minimum value in a specified column.
  ///
  /// - Example:
  ///   ```dart
  ///   double minValue = df.min(0); // Gets the minimum value in the first column.
  ///   ```
  double min(int columnIndex) {
    return _matrix.filterNulls(columnIndex).reduce((a, b) => a < b ? a : b);
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
      _matrix.reset_index();
      return empty;
    } else {
      DataFrame df = DataFrame._copyDataframe(this);
      df._matrix.reset_index();
      return df;
    }
  }

  /// Helper method when combining data from two dataframes. Adjusts for type differences.
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

    if( (columnIndex1+1 > _matrix.data.length && columnIndex1 != 0) || _matrix.data.isEmpty){ // For the simple case of an empty df1
      _matrix.columnTypes.add(df2.dtypes[columnIndex2]); 
      _matrix.data.add(df2.values[columnIndex2]); // Enclose in List because normally a List container would already exist.
    }
    else if(_matrix.columnTypes[columnIndex1] == df2.dtypes[columnIndex2]){
      _matrix.data[columnIndex1].addAll(df2.values[columnIndex2]);
    } else if(_matrix.columnTypes[columnIndex1] == double && df2.dtypes[columnIndex2] == int){
      //List tempList = [];
      for (int e in df2.values[columnIndex2]) {
          //tempList.add(e.toDouble());
          _matrix.data[columnIndex1].add(e.toDouble());
      }       
    } else if(_matrix.columnTypes[columnIndex1] == int && df2.dtypes[columnIndex2] == double){
      List tempList = <double>[];
      for (int e in _matrix.data[columnIndex1]) {
          tempList.add(e.toDouble());
      }
      _matrix.data[columnIndex1] = tempList;
      _matrix.columnTypes[columnIndex1] = double;
      _matrix.data[columnIndex1].addAll(df2.values[columnIndex2]); 
    } else if(_matrix.columnTypes[columnIndex1] == Object || _matrix.columnTypes[columnIndex1] == dynamic){
      _matrix.data[columnIndex1].addAll(df2.values[columnIndex2]);
    } else {  //Else, change the listed type, and create new column with the new generic. [This might not be needed, just a failsafe]
      _matrix.columnTypes[columnIndex1] = Object;
      List newList = <Object>[];
      newList.addAll(_matrix.data[columnIndex1]);
      _matrix.data[columnIndex1] = newList;
      _matrix.data[columnIndex1].addAll(df2.data[columnIndex2]); 
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

  /// Retrieves or edits rows and columns based on specified indices or ranges.
  /// Multiple rows can be returned by providing a list of indices or a range, e.g., [2,5] returns rows 3 through 6, and [2,null] returns all rows after row 2.
  ///
  /// - Parameters:
  ///   - row: (Required) The row index, list of indices, or map defining a range of rows to retrieve. Supports int, String, List, or Map.
  ///   - col: (Optional) The column index or range. If null, defaults to all columns.
  ///   - edit: (Optional) Value used to update the specified rows/columns.
  ///
  /// - Example:
  ///   ```dart
  ///   var row = df.iloc(row: 2); // Returns the row at index 2.
  ///   var rangeDf = df.iloc(row: {0: 2}, col: {1: null}); // Returns a DataFrame with rows 0 to 1 and all columns starting from 1.
  ///   df.iloc(row: 1, col: 2, edit: 'newValue'); // Updates row 1, column 2 with 'newValue'.
  ///   df.iloc(row: 3, edit: 'newValue'); // Updates all elements in row 3 with 'newValue'.
  ///   ```
  iloc({required row, var col, var edit}) {
    var matrix = _matrix.data;
    // 1. STANDARD OPERATION: ROW IS A NON-MAP VALUE
    // 1.a. If 'row' is a non-Map value (int or String), retrieve or edit a single row
    if ((row is int || row is String) && row is! Map) {
      int rowIndex = row is String ? int.parse(row) : row;
      // 1.b. If 'col' and 'edit' are not provided, return the entire row as a List
      if (col == null && edit == null) {
        List<dynamic> newRow = [];
        for (int i = 0; i < matrix.length; i++) {
          var element = matrix[i][rowIndex];
          Type columnType = dtypes[i];
          // Ensure type consistency for elements of 'Object' type
          if (columnType == Object && (element is int || element is double)) {
            newRow.add(element.runtimeType == int ? element.toInt() : element.toDouble());
          } else {
            newRow.add(element);
          }
        }
        return newRow;
      }
      // 1.c. If 'col' is specified, access or edit a specific cell in the DataFrame
      if (col is int) {
        if (edit != null) {
          // 1.c.i. Update the element if 'edit' is provided
          matrix[col][rowIndex] = edit;
          return '';
        } else {
          // 1.c.ii. Return the element at the specified row and column
          return matrix[col][rowIndex];
        }
      }
      // 1.d. If 'col' is not specified but 'edit' is, update the entire row
      if (col == null) {
        // 1.d.i. If 'edit' is not a List, apply the same value to all columns in the row
        if (edit != null && (edit is! List && edit is! Iterable)) {
          for (int i = 0; i < matrix.length; i++) {
            matrix[i][rowIndex] = edit;
          }
          return '';
        // 1.d.ii. If 'edit' is a List/Iterable, update the row with the new values
        } else if (edit != null && (edit is List || edit is Iterable)) {
          if (matrix.length != edit.length) {
            throw ArgumentError('Number of values in new data must equal the number of columns in the dataframe');
          }
          int counter = 0;
          for (var e in edit) {
            matrix[counter][rowIndex] = e;
            ++counter;
          }
          return '';
        }
      }
    }
    // 2. ROW IS A LIST: REORDER OR SELECT SPECIFIC ROWS
    // 2.a. If 'row' is a List, use reverse mapping to reorder or select rows by indices
    if (row is List) {
      Map reverseRowKV = _matrix.reverseMap(_matrix.rowIndexMap);
      List newData = [];
      List newRowIndices = [];
      for (var e in row) {
        if (e is int) {
          newRowIndices.add(reverseRowKV[e]);
          newData.add(matrix.map((column) => column[e]).toList());
        } else {
          throw ArgumentError('List elements must be integers');
        }
      }
      // 2.b. Return a new DataFrame with the selected rows
      return DataFrame(newData, index: newRowIndices, columns: columns);
    }
    // 3. ROW IS A MAP: SUBLIST SELECTION FOR ROWS AND COLUMNS
    // 3.a. If 'row' is a Map with a single key-value pair, get a sublist of rows and columns
    if (row is Map && row.length == 1) {
      // 3.b. Extract the row range (start and end)
      List<int> rowRange = [row.keys.first ?? 0, row.values.first ?? _matrix.rowLastIndexVal+1];
      // 3.c. Default to all columns if 'col' is null
      List<int> colRange;
      col ??= {0: _matrix.data[0].length};
      if (col is Map && col.length == 1) {
        colRange = [col.keys.first ?? 0, col.values.first ?? _matrix.columnLastIndexVal+1];
      } else {
        throw ArgumentError('Invalid column input. Must be a Map with one key-value pair.');
      }
      // 3.d. Validate the specified row and column ranges
      if (rowRange.any((r) => r < 0 || r >= _matrix.data.length + 1) ||
          colRange.any((c) => c < 0 || c >= _matrix.data[0].length + 1)) {
        throw ArgumentError('Invalid row or column range.');
      }
      // 3.e. Extract the specified submatrix based on the row and column ranges
      List<List> newData = [];
      for (int i = rowRange[0]; i <= rowRange[1] - 1; i++) {
        List rowToAdd = [];
        for (int j = colRange[0]; j <= colRange[1] - 1; j++) {
          rowToAdd.add(_matrix.data[j][i]);
        }
        newData.add(rowToAdd);
      }
      // 3.f. Retrieve the new row and column names based on the ranges
      List newRows = index.sublist(rowRange[0], rowRange[1]);
      List newColumns = columns.sublist(colRange[0], colRange[1]);
      // 3.g. Return a new DataFrame with the sublist of rows and columns
      return DataFrame(newData, index: newRows, columns: newColumns);
    }
    // 4. INVALID INPUT CASE: THROW ERROR - If no valid input type or range is matched, throw an error
    throw ArgumentError('Invalid input type or range.');
  }
  /// Edit data in a row using [][] operators
  /// - Example:
  ///   ```dart
  ///   df.editRow['City']['Temperature] = 20; // Edit the row City and the column Temperature to the value of 20
  ///   df.editRow['City'] = [20, 43, 'London']; // Edit the row data for 'City'
  ///   ```
  RowIndexer get editRow => RowIndexer(_matrix);

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
      if(!_matrix.rowIndexMap.containsKey(row)){
        if(!_matrix.rowIndexMap.containsKey(row.toString())){
          throw ArgumentError('Index entered does not exist');
        }
      }
    }  
    // row is a List    
    else if (row is List) {
      // Ensure the newOrder contains the same row indices as the current DataFrame
      final currentIndices = _matrix.rowIndexMap.keys.toList();

      if ( !setEquals(currentIndices.toSet(), row.toSet()) || _matrix.rowLastIndexVal+1 != row.length) { 
        throw ArgumentError('The new row index order must contain the same row indices as the DataFrame');
      }
      // Create a new list to store the reordered data
      final List newData = [];
      final copyCurrentRowIndexMap = _matrix.rowIndexMap.map(
        (key, value) => MapEntry(key, List.from(value)),
      );
      // Populate the new data and rowIndexMap based on the newOrder
      for (var rowName in row) {
        final List currentRowIndices = copyCurrentRowIndexMap[rowName]!;
          // Extract the corresponding row by iterating over columns
          final List<dynamic> tempRow = [];
          for (var column in _matrix.data) {
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
      if(!_matrix.rowIndexMap.containsKey(row)){
        final testInt = int.tryParse(row);
        if(_matrix.rowIndexMap.containsKey(testInt)){
          row = testInt;
        } else{
          throw ArgumentError('Index entered does not exist');
        }
      }
    } 
    // `row` is Map input; need to do previous checks twice for key and value
    else if(row is Map){
      // Make sure the start row name exists
      Map integerIndexToRowName = _matrix.reverseMap(_matrix.rowIndexMap);
      Map integerIndexToColName = _matrix.reverseMap(_matrix.columnIndexMap);     
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
      var rowIndexName2 = lsRow.values.first == null ? integerIndexToRowName[_matrix.rowLastIndexVal]:lsRow.values.first;       
      var colIndexName1 = (lsCol.isNotEmpty && lsCol.keys.first != null) ? lsCol.keys.first : integerIndexToColName[0];
      var colIndexName2 = (lsCol.isNotEmpty && lsCol.values.first != null) ? lsCol.values.first : integerIndexToColName[_matrix.columnLastIndexVal];

      // Make sure the end row name exists
      if(rowIndexName1 is int){
        if(!_matrix.rowIndexMap.containsKey(rowIndexName1)){
          if(!_matrix.rowIndexMap.containsKey(rowIndexName1.toString())){
            throw ArgumentError('Index entered does not exist');
          }
        }
      } else if(!lsRow.keys.contains(RegExp('[\s\n]')) && int.tryParse(rowIndexName1) != null){
        // If entered row name isn't found, check if it is found when converted to an int
        if(!_matrix.rowIndexMap.containsKey(rowIndexName1)){
          final testInt = int.tryParse(rowIndexName1);
          // if row was found to contain the name after it is converted, re-enter it as an int argument
            if(_matrix.rowIndexMap.containsKey(testInt)){
              lsRow[testInt] = lsRow[rowIndexName1];
              lsRow.remove(rowIndexName1);
            } else{
              throw ArgumentError('Index entered does not exist');
            }
        }
      }
      if(rowIndexName2 is int){
        if(!_matrix.rowIndexMap.containsKey(rowIndexName2)){
          if(!_matrix.rowIndexMap.containsKey(rowIndexName2.toString())){
            throw ArgumentError('Index entered does not exist');
          }
        }
      } else if(!lsRow.values.contains(RegExp('[\s\n]')) && int.tryParse(rowIndexName2) != null){
        // If entered row name isn't found, check if it is found when converted to an int
        if(!_matrix.rowIndexMap.containsKey(rowIndexName2)){
          final testInt = int.tryParse(rowIndexName2);
          // If row was found to contain the name after it is converted, re-enter it as an int argument
            if(_matrix.rowIndexMap.containsKey(testInt)){
              lsRow[rowIndexName1] = testInt;
            } else{
              throw ArgumentError('Index entered does not exist');
            }
        }
      }
      if(col.isNotEmpty){ // Make sure the end col name exists
        if(colIndexName1 is int){
          if(!_matrix.columnIndexMap.containsKey(colIndexName1)){
            if(!_matrix.columnIndexMap.containsKey(colIndexName1.toString())){
              throw ArgumentError('Index entered does not exist');
            }
          }
        } else if(!lsCol.keys.contains(RegExp('[\s\n]')) && int.tryParse(colIndexName1) != null){
          // If entered col name isn't found, check if it is found when converted to an int
          if(!_matrix.columnIndexMap.containsKey(colIndexName1)){
            final testInt = int.tryParse(colIndexName1);
            // If col was found to contain the name after it is converted, re-enter it as an int argument
              if(_matrix.columnIndexMap.containsKey(testInt)){
                lsCol[testInt] = lsCol[colIndexName1];
                lsCol.remove(colIndexName1);
              } else{
                throw ArgumentError('Index entered does not exist');
              }
          }
        }
        if(colIndexName2 is int){
          if(!_matrix.columnIndexMap.containsKey(colIndexName2)){
            if(!_matrix.columnIndexMap.containsKey(colIndexName2.toString())){
              throw ArgumentError('Index entered does not exist');
            }
          }
        } else if(!lsCol.values.contains(RegExp('[\s\n]')) && int.tryParse(colIndexName2) != null){
          // If entered row name isn't found, check if it is found when converted to an int
          if(!_matrix.columnIndexMap.containsKey(colIndexName2)){
            final testInt = int.tryParse(rowIndexName2);
            // If col was found to contain the name after it is converted, re-enter it as an int argument
              if(_matrix.columnIndexMap.containsKey(testInt)){
                lsCol[colIndexName1] = testInt;
              } else{
                throw ArgumentError('Index entered does not exist');
              }
          }
        }
      } 
      var integerRow1 = lsRow.keys.first == null ? 0:_matrix.rowIndexMap[lsRow.keys.first].first;
      var integerRow2 = lsRow.values.first == null ? _matrix.rowLastIndexVal+1: _matrix.rowIndexMap[lsRow.values.first].first;
      var integerCol1 = 0;
      var integerCol2 = _matrix.data.length;
      if(col.isNotEmpty){
        integerCol1 = lsCol.keys.first == null ? 0 : _matrix.columnIndexMap[lsCol.keys.first].first;
        integerCol2 = lsCol.values.first == null ? _matrix.data.length : _matrix.columnIndexMap[lsCol.values.first].first;
      }
      // Get the column range from sCol
      int colStart = lsCol.isNotEmpty ? integerCol1 : 0; // Default to the first column if sCol is empty
      int colEnd = lsCol.isNotEmpty ? integerCol2 : _matrix.data.length - 1; // Select up to and including the given end column
      if(colEnd == _matrix.columnLastIndexVal+1){ // Needed so that custom end column is included in loop below, but if's the end column is last column, there will be a range error 
        --colEnd; 
      }
      // If edit parameter entered, edit data with no return value.
      if(edit != null){
        for(int i = integerRow1; i < integerRow2; i++){
          for(int j = colStart; j <= colEnd; j++){ 
            _matrix.addEditType(input: edit, colIndex: j, rowIndex: i);
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
          newDataRow.add(_matrix.data[j][i]);  // Accessing columns selectively within the specified range
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
        for(int m in _matrix.rowIndexMap[row]){
          for(int m2 in _matrix.columnIndexMap[col]){
            _matrix.addEditType(input: edit, colIndex: m2, rowIndex: m);
          }
        }
      } else{
        // If col is is empty, all columns must be edited
        if(col.isEmpty){
          for(int m in _matrix.rowIndexMap[row]){
            for(int j = 0; j <= _matrix.columnLastIndexVal; j++){ 
              _matrix.addEditType(input: edit, colIndex: j, rowIndex: m);
            }
          }
        // If col has ranges
        } else { 
          var colStart = col.keys.first == null ? 0 : _matrix.columnIndexMap[col.keys.first].first;
          var colEnd = col.values.first == null? _matrix.columnLastIndexVal:_matrix.columnIndexMap[col.values.first].first;
          for(int m in _matrix.rowIndexMap[row]){
            for(int j = colStart; j <= colEnd; j++){ 
              _matrix.addEditType(input: edit, colIndex: j, rowIndex: m);
            }
          }
        }
      }
      return;
    }
    var colStart = col.keys.first == null ? 0 : _matrix.columnIndexMap[col.keys.first].first;
    var colEnd = col.values.first == null ? _matrix.columnLastIndexVal:_matrix.columnIndexMap[col.values.first].first;
    if(colEnd == _matrix.columnLastIndexVal+1){ // Needed so that custom end column is included in loop below, but if's the end column is last column, there will be a range error 
        --colEnd; 
    }
    // Return value: Return a List for a single row, return DataFrame for multiple rows
    if( _matrix.rowIndexMap[row].length > 1 ){
      List multiList = [];
      List newRowIndex = [];
      List newColumnIndex = columns.sublist(colStart, colEnd+1);
      for(var m in _matrix.rowIndexMap[row]){
        var tempRow = [];
        for(int i = colStart; i <= colEnd; i++){
          tempRow.add(_matrix.data[i][m]);
        }
        multiList.add(tempRow);
        newRowIndex.add(row);
      }
      return DataFrame(multiList, columns: newColumnIndex, index: newRowIndex );
    }
    List returnList = [];
    for(int i = colStart; i <= colEnd; i++){
      returnList.add(_matrix.data[i][_matrix.rowIndexMap[row].first]);
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
    List dataT = _matrix.transposeT(_matrix.data);
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
    for(int i = 0; i < _matrix.data.length; i++){
      infoList.add([]); // add column List
      infoList[i].add(i); // Add integer index
      infoList[i].add(columnNames[i]); // Add column names
      infoList[i].add('${countNulls(columnNames[i])} non-null');
      infoList[i].add(_matrix.columnTypes[i]);
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
    List dataT = _matrix.transposeT(_matrix.data);
    if(rowIndex == false){
      table.add(columns);
      table.addAll(dataT);
    }
    if(rowIndex == true){
      List rowIndices = index;
      table.add([indexHeader, ...columns]);
      for(int i = 0; i < _matrix.rowLastIndexVal+1; i++){
        table.add([rowIndices[i]] + dataT[i]);
      }
    }
    return table;
  }

//* Getters
  
  /// Gets the row indices of the DataFrame
  List get index => _matrix.orderedEntries(_matrix.rowIndexMap, false);
  /// Sets a new List of row indices for the DataFrame.
  set index(List newRowIndices){
    if(newRowIndices.length != _matrix.rowLastIndexVal+1){
      throw ArgumentError('Row index does not much');
    }
    _matrix.indexer(newRowIndices, false, resetIndex: true);
  }

  /// A List of all the column names
  List get columns => _matrix.orderedEntries(_matrix.columnIndexMap, true);
  /// Sets a new List of column names for the DataFrame. 
  /// Note: Use rename() when renaming an individual column.
  set columns(List newColumnNames){ 
    if(newColumnNames.length != _matrix.columnLastIndexVal+1){
      throw ArgumentError('Number of column names entered must match original');
    } else {
      _matrix.indexer(newColumnNames, true, resetIndex: true);
    }
  }
  /// The DataFrame data in a List (pd)
  get values => _matrix.data;

  /// The number of rows in the DataFrame
  int get length => _matrix.rowLastIndexVal+1;
  
  /// A List containing the Type for each column
  get dtypes => _matrix.columnTypes;
  
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
      var column = _matrix.columnIndexMap;
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
            if(_matrix.data[index] is List<int>){  
              newData.add(List<int>.from(_matrix.data[index]));
            } else if(_matrix.data[index] is List<double>){  
              newData.add(List<double>.from(_matrix.data[index]));
            } else if(_matrix.data[index] is List<num>){  
              newData.add(List<num>.from(_matrix.data[index]));
            } else if(_matrix.data[index] is List<String>){  
              newData.add(List<String>.from(_matrix.data[index]));
            } else if(_matrix.data[index] is List<bool>){  
              newData.add(List<bool>.from(_matrix.data[index]));
            } else if(_matrix.data[index] is List<Object>){  
              newData.add(List<Object>.from(_matrix.data[index]));
            } else{
              newData.add(_matrix.data[index].map((i)=>i).toList());   
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
      return _matrix[columnName];
    }
  }

  /// Replaces an entire column of data. If multiple columns share the same name, 
  /// they will all be replaced with the same List argument.
  void operator []=(var columnName, List inputData) {
    _matrix[columnName] = inputData;
  }

  /// Prints a formatted table of the DataFrame data to the terminal
  @override
  String toString() {
    final buffer = StringBuffer('\n');

    // 1. DETERMINE MAXIMUM INDEX LENGTH
    // 1.a. Determine the maximum length of the row index for proper alignment. Default to 1 if no data is present.
    int maxIndexLength = _matrix.rowIndexMap.isNotEmpty
        ? _matrix.rowIndexMap.keys.map((e) => e.toString().length).fold(0, (max, e) => e > max ? e : max)
        : 1;

    // 2. STORE MAXIMUM COLUMN LENGTHS
    // 2.a. Start with the lengths of the column names
    List columnNames = columns;
    final columnMaxWidth = List<int>.generate(columnNames.length, (i) => columnNames[i].toString().length);

    // 2.b. Iterate through each column's data to find the maximum width for proper formatting
    int counter = -1;
    for (var column in _matrix.data) {
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
        for (var columnIndex = 0; columnIndex < _matrix.data.length; columnIndex++) {
          var value = columnIndex < _matrix.data.length && rows < _matrix.data[columnIndex].length
              ? _matrix.data[columnIndex][rows]?.toString() ?? ''
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
/// - Usage Notes (pd3.9):
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
      Set joinColumnTracker = input.first._matrix.columnIndexMap.keys.toSet();  // Track columns for joining
      List df1ColumnNames = input.first.columns;
      // 3.a. Inner join: only keep columns present in all DataFrames
      if (join == 'inner' && !(df1ColumnNames.length != df1ColumnNames.toSet().length)) {
        for (var df in input.skip(1)) {
          joinColumnTracker = joinColumnTracker.intersection(df._matrix.columnIndexMap.keys.toSet());
        } 
        if (joinColumnTracker.isNotEmpty) {
          // Clear DataFrame while retaining some metadata
          newDataFrame._matrix.clear(except: {'rowLastIndexVal', 'rowIndexMap', 'columnTypes'});
          // Add columns based on the inner join of common columns
          newDataFrame._matrix.indexer(joinColumnTracker, true);
          // Temporarily store new column types
          List<Type> newTempColumnTypes = <Type>[];
          for (var columnName in joinColumnTracker) {
            int colIndex = input[0]._matrix.columnIndexMap[columnName].first;
            newDataFrame._matrix.data.add(input[0]._matrix.data[colIndex]);
            newTempColumnTypes.add(input[0]._matrix.columnTypes[colIndex]);
          }
          // Add data from subsequent DataFrames based on common columns
          for (var df in input.skip(1)) {
            newDataFrame._matrix.indexer(df.index, false);
            for (var columnName in joinColumnTracker) {
              int colIndex1 = newDataFrame._matrix.columnIndexMap[columnName].first;
              int colIndex2 = df._matrix.columnIndexMap[columnName].first;
              newDataFrame._combineColumnFromDf(df2: df, columnIndex1: colIndex1, columnIndex2: colIndex2);
            }
          }
          // Update column types for the new DataFrame
          newDataFrame._matrix.columnTypes = newTempColumnTypes;
        } else {
          // Clear the DataFrame if no columns match, add only the row index
          newDataFrame._matrix.clear(except: {'rowLastIndexVal', 'rowIndexMap'});
          for(DataFrame e in input.skip(1)){
            newDataFrame._matrix.indexer(e.index, false);
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
              newDataFrame._matrix.indexer(df.index, false);
              for (int i = 0; i < df._matrix.data.length; i++) {
                newDataFrame._combineColumnFromDf(df2: df, columnIndex1: i, columnIndex2: i);
              }
            } else {
              throw ArgumentError('Index cannot contain non-unique values for this operation');
            }
          } else if (df.columns.length != df.columns.toSet().length) {
            throw ArgumentError('Secondary DataFrame column names cannot contain non-unique values for this operation');
          } else {
            // 3.c. Standard operation: unique column names
            newDataFrame._matrix.indexer(df.index, false);
            // Add data from subsequent DataFrames, filling in missing columns with NaN
            Set<Object> keysThatWereUsed = {};
            var dfColumnStartPoint = newDataFrame._matrix.rowLastIndexVal;
            for (var key in newDataFrame._matrix.columnIndexMap.keys) {
              var colIndex1 = newDataFrame._matrix.columnIndexMap[key].first;
              if (df._matrix.columnIndexMap.containsKey(key)) {
                var colIndex2 = df._matrix.columnIndexMap[key].first;
                keysThatWereUsed.add(key);
                newDataFrame._combineColumnFromDf(df2: df, columnIndex1: colIndex1, columnIndex2: colIndex2);
              } else {
                for (int i = 0; i < df._matrix.rowLastIndexVal + 1; i++) {
                  newDataFrame._matrix.addEditType(input: double.nan, colIndex: colIndex1);
                }
              }
            }
            // Add new columns from df if not already in newDataFrame
            for (var key in df._matrix.columnIndexMap.keys) {
              if (keysThatWereUsed.contains(key)) {
                continue;
              }
              var colIndex = df._matrix.columnIndexMap[key].last;
              List columnToBeAdded;
              if (df._matrix.columnTypes[colIndex] == int) {
                List tempColumn = <double>[];
                for (int e in df._matrix.data[colIndex]) {
                  tempColumn.add(e.toDouble());
                }
                columnToBeAdded = <double>[];
                newDataFrame._matrix.columnTypes.add(double);
              } else if (df._matrix.columnTypes[colIndex] == double) {
                columnToBeAdded = <double>[];
                newDataFrame._matrix.columnTypes.add(double);
              } else {
                columnToBeAdded = <Object>[];
                newDataFrame._matrix.columnTypes.add(Object);
              }
              newDataFrame._matrix.indexer([key], true);
              newDataFrame._matrix.data.add(columnToBeAdded);
              var newColumnName = newDataFrame._matrix.columnIndexMap[key].first;
              var dfColIndex = df._matrix.columnIndexMap[key].first;
              dfColumnStartPoint = (newDataFrame._matrix.rowLastIndexVal - df._matrix.rowLastIndexVal) as int;
              for (int k = 0; k <= newDataFrame._matrix.rowLastIndexVal; k++) {
                if (k < dfColumnStartPoint) {
                  newDataFrame._matrix.data[newColumnName].add(double.nan);
                } else {
                  if (df._matrix.columnTypes[dfColIndex] == int) {
                    newDataFrame._matrix.data[newColumnName].add(df._matrix.data[dfColIndex][k - dfColumnStartPoint].toDouble());
                  } else {
                    newDataFrame._matrix.data[newColumnName].add(df._matrix.data[dfColIndex][k - dfColumnStartPoint]);
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
      Set joinRowsTracker = input.first._matrix.rowIndexMap.keys.toSet();
      Map df1RowIndexMap = newDataFrame._matrix.rowIndexMap;
      // 4.a. Inner join: keep only rows common to all DataFrames
      if (join == 'inner') {
        for (var df in input.skip(1)) {
          joinRowsTracker = joinRowsTracker.intersection(df._matrix.rowIndexMap.keys.toSet());
        }
        if (joinRowsTracker.isNotEmpty) {
          newDataFrame._matrix.clear();
          newDataFrame._matrix.indexer(joinRowsTracker, false);
          for (var df in input) {
            newDataFrame._matrix.indexer(df.columns, true);
            List tempData = [];
            for (int i = 0; i < df._matrix.data.length; i++) {
              tempData.add(createListFromType(df._matrix.columnTypes[i]));
            }
            for (var i in joinRowsTracker) {
              var rowIndex = df._matrix.rowIndexMap[i].first;
              for (int j = 0; j < df._matrix.data.length; j++) {
                tempData[j].add(df._matrix.data[j][rowIndex]);
              }
            }
            newDataFrame._matrix.data.addAll(tempData);
            newDataFrame._matrix.columnTypes.addAll(df._matrix.columnTypes);
          }
        } else {
          newDataFrame._matrix.clear(except: {'columnLastIndexVal', 'columnIndexMap'});
        }
      } else {
        // 4.b. Outer join: Add df2's columns to df1, ensuring rows match as much as possible
        for (var df in input.skip(1)) {
          newDataFrame._matrix.indexer(df.columns, true);
          var df1Index = newDataFrame._matrix.rowIndexMap.keys;
          var df2Index = df._matrix.rowIndexMap.keys;

          if (areIterablesEqualUnordered(df1Index, df2Index)) {
            newDataFrame._matrix.data.addAll(df._matrix.data);
            newDataFrame._matrix.columnTypes.addAll(df._matrix.columnTypes);
          } else {
            if (df == input[1]) {
              for (int i = 0; i < newDataFrame._matrix.columnTypes.length; i++) {
                if (newDataFrame._matrix.columnTypes[i] == int) {
                  List tempList = <double>[];
                  for (int e in newDataFrame._matrix.data[i]) {
                    tempList.add(e.toDouble());
                  }
                  newDataFrame._matrix.data[i] = tempList;
                  newDataFrame._matrix.columnTypes[i] = double;
                } else if (newDataFrame._matrix.columnTypes[i] != Object || newDataFrame._matrix.columnTypes[i] != double) {
                  List tempList = <Object>[];
                  tempList.addAll(newDataFrame._matrix.data[i]);
                  newDataFrame._matrix.data[i] = tempList;
                  newDataFrame._matrix.columnTypes[i] = Object;
                }
              }
            }
            for (int i = 0; i < df.columns.length; i++) {
              if (df._matrix.columnTypes[i] == int || df._matrix.columnTypes[i] == double) {
                newDataFrame._matrix.data.add(List<double>.filled(newDataFrame.index.length, double.nan, growable: true));
                newDataFrame._matrix.columnTypes.add(double);
              } else if (df._matrix.columnTypes[i] != Object || df._matrix.columnTypes[i] != double) {
                newDataFrame._matrix.data.add(List<Object>.filled(newDataFrame.index.length, double.nan, growable: true));
                newDataFrame._matrix.columnTypes.add(Object);
              }
            }
            List df2Index = df.index;
            var dfColStartPosition = (newDataFrame._matrix.columnLastIndexVal - df._matrix.columnLastIndexVal) as int;
            for (var key in df2Index) {
              if (df1RowIndexMap.containsKey(key)) {
                int df1rowIndex = df1RowIndexMap[key].first;
                for (int j = 0; j < df.columns.length; j++) {
                  if (df._matrix.columnTypes[j] == int) {
                    newDataFrame._matrix.data[dfColStartPosition + j][df1rowIndex] = df._matrix.data[j][df._matrix.rowIndexMap[key].first].toDouble();
                  } else {
                    newDataFrame._matrix.data[dfColStartPosition + j][df1rowIndex] = df._matrix.data[j][df._matrix.rowIndexMap[key].first];
                  }
                }
              } else {
                newDataFrame._matrix.indexer([key], false);
                for (int k = 0; k < newDataFrame._matrix.columnLastIndexVal + 1; k++) {
                  if (k < dfColStartPosition) {
                    newDataFrame._matrix.data[k].add(double.nan);
                  } else {
                    if (df._matrix.columnTypes[k - dfColStartPosition] == int) {
                      newDataFrame._matrix.data[k].add(df._matrix.data[k - dfColStartPosition][df._matrix.rowIndexMap[key].first].toDouble());
                    } else {
                      newDataFrame._matrix.data[k].add(df._matrix.data[k - dfColStartPosition][df._matrix.rowIndexMap[key].first]);
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

/// A function that compares two objects for sorting. It will return -1 if a
/// should be ordered before b, 0 if a and b are equal wrt to ordering, and 1
/// if a should be ordered after b.
typedef CustomComparator = int Function(Object? a, Object? b);

/// A proxy object setup used to edit column data structure as rows via [][] operator.
class RowIndexer<T> {
  DataFrameCore datacore;
  RowIndexer(this.datacore);

  RowView<T> operator [](final rowIndex) {
    if( rowIndex.runtimeType == int){
      return RowView<T>(rowIndex, datacore);
    } else{
      var rowIndexS = datacore.rowIndexMap[rowIndex].first;
      return RowView<T>(rowIndexS, datacore);
    } 
  }
  void operator []=(int rowIndex, List newRow) {
    int counter = 0;
    if( rowIndex.runtimeType == String){
      rowIndex = datacore.rowIndexMap[rowIndex].first;
    }
    for(var i = 0; i < datacore.data.length; i++){ 
      datacore.addEditType(input: newRow[counter], colIndex: counter, rowIndex: rowIndex);
      counter++;
    }
  }
}

class RowView<T> {
  final rowIndex;
  final DataFrameCore datacore;

  RowView(this.rowIndex, this.datacore);
  int get length => datacore.data.length;
  
  T operator [](var columnIndex) {  
    if( columnIndex.runtimeType == int){
      return datacore.data[columnIndex][rowIndex];
    } else{
      var columnIndexS = datacore.columnIndexMap[columnIndex].first;
      return datacore.data[columnIndexS][rowIndex];
    } 
    //return dfcore.data[columnIndex][rowIndex];
  }
  void operator []=(var columnIndex, var value) {
    if( columnIndex.runtimeType == int){
      //return datacore.data[columnIndex][rowIndex];
      datacore.addEditType(input: value, colIndex: columnIndex, rowIndex: rowIndex);
    } else{
      var columnIndexS = datacore.columnIndexMap[columnIndex].first;
      // return datacore.data[columnIndexS][rowIndex];
      datacore.addEditType(input: value, colIndex: columnIndexS, rowIndex: rowIndex);
    } 
    //datacore.addEditType(input: value, colIndex: columnIndex, rowIndex: rowIndex);
  }
  @override
  String toString() {
    return [for (var column in datacore.data) column[rowIndex]].toString(); 
  }
}