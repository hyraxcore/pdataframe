/// The DataFrameCore<T> class is the foundational data structure for DataFrame 
/// It handles data storage, column type management, and indexing mechanisms for both rows and columns. 

class DataFrameCore<T> {
  // * Fields *
  String name = '';
  List data = [];
  List<Type> columnTypes = <Type>[]; // columnTypes saves List generics because Dart does not store inner generics at runtime
  Map rowIndexMap = {};
  Map columnIndexMap = {};
  int rowLastIndexVal = -1;
  int columnLastIndexVal = -1;

  // * Data Manipulation Methods *

  /// Add or edit a value in the data matrix, updating the column type information accordingly.
  /// - Parameters:
  ///   - input: The new value to be added or used for editing.
  ///   - colIndex: The index of the data column being modified.
  ///   - rowIndex: Optional. Specifies the row index to edit. If value is -1, the input value is added to the end of the column.
  void addEditType({required var input, required int colIndex, int rowIndex = -1}) {
    Type eType = input.runtimeType;
    // Handle the case where a new column needs to be added
    if (colIndex >= data.length) {
      if (rowIndex != -1) {
        // Cannot edit in a non-existent column
        throw Exception("Cannot edit in a new column at index $colIndex");
      } else {
        // Adding a new column
        columnTypes.add(eType);
        data.add([input]);
        return;
      }
    }
    // Determine the target index for adding or editing
    int targetIndex = rowIndex;
    if (rowIndex == -1) {
      // Adding to the end of the column
      targetIndex = data[colIndex].length;
    } else if (rowIndex < 0 || rowIndex >= data[colIndex].length) {
      throw Exception("Invalid edit index $rowIndex for column $colIndex");
    }
    // Proceed with type checking and handling
    if (eType == columnTypes[colIndex]) {
      // Types match
      if (targetIndex == data[colIndex].length) {
        data[colIndex].add(input); // Adding
      } else {
        data[colIndex][targetIndex] = input; // Editing
      }
    } else if ((eType == double || eType == num) && columnTypes[colIndex] == int) {
      // Upgrade column type to double
      columnTypes[colIndex] = double;
      List<double> tempList = [];
      for (num e in data[colIndex]) {
        tempList.add(e.toDouble());
      }
      if (targetIndex == data[colIndex].length) {
        tempList.add((input as num).toDouble());
      } else {
        tempList[targetIndex] = (input as num).toDouble();
      }
      data[colIndex] = tempList;
    } else if (eType == int && columnTypes[colIndex] == double) {
      // Input is int, column expects double
      if (targetIndex == data[colIndex].length) {
        data[colIndex].add((input as int).toDouble());
      } else {
        data[colIndex][targetIndex] = (input as int).toDouble();
      }
    } else if (columnTypes[colIndex] == String && eType == num) {
      // Column expects String but input is numeric
      if (targetIndex == data[colIndex].length) {
        data[colIndex].add(input.toString());
      } else {
        data[colIndex][targetIndex] = input.toString();
      }
    } else if (columnTypes[colIndex] == Object || columnTypes[colIndex] == dynamic) {
      // Column type is Object or dynamic
      if (targetIndex == data[colIndex].length) {
        data[colIndex].add(input);
      } else {
        data[colIndex][targetIndex] = input;
      }
    } else {
      // Change column type to Object for mixed types
      columnTypes[colIndex] = Object;
      List newList = createListFromType(Object);
      for (var e in data[colIndex]) {
        newList.add(e);
      }
      if (targetIndex == data[colIndex].length) {
        newList.add(input);
      } else {
        newList[targetIndex] = input;
      }
      data[colIndex] = newList;
    }
  }

  /// Filters out null and NaN values from a specified column and returns a list of doubles.
  /// - Parameters:
  ///   - columnIndex: The index of the column to filter.
  ///   - skipNull: If true, skips null values; if false, replaces nulls with 0.0.
  List<double> filterNulls(var columnIndex, {bool skipNull = true}) {
    final rawData = List.from(data[columnIndex]);
    List<double> processedData;
    if (skipNull == true) {
      // Skip null values and convert to double
      processedData = rawData
          .whereType<num>()
          .where((e) => !e.isNaN)
          .map((e) => e.toDouble())
          .toList();
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

  // * Index Manipulation Methods *

  /// Method that handles adding names to row/column index maps.
  /// - Parameters:
  ///   - inputIndex: An iterable of new index names.
  ///   - isColumn: If true, adds to column index map; if false, to row index map.
  ///   - resetIndex: If true, clears existing indices before adding new ones.
  void indexer(Iterable inputIndex, bool isColumn, {bool resetIndex = false}) {
    Map indexed = isColumn ? columnIndexMap : rowIndexMap;
    int lastIndexVal = isColumn ? columnLastIndexVal : rowLastIndexVal;
    if (resetIndex == true) {
      lastIndexVal = -1;
      indexed = {};
    }
    if(inputIndex is Map || inputIndex is String){
      throw ArgumentError('Not a valid type');
    } else {
      if (inputIndex.isEmpty) {
        // Reset index maps if input is empty
        if (isColumn) {
          columnIndexMap = {};
          columnLastIndexVal = -1;
        } else {
          rowIndexMap = {};
          rowLastIndexVal = -1;
        }
      } else {
        for (var e in inputIndex) {
          indexed[e] ??= <int>[];
          indexed[e].add(++lastIndexVal);
        }
      }
      // Update index maps and last index values
      if (isColumn) {
        columnIndexMap = indexed;
        columnLastIndexVal = lastIndexVal;
      } else {
        rowIndexMap = indexed;
        rowLastIndexVal = lastIndexVal;
      }
    }
  }

  /// Edits or moves indices while adjusting index values and last index value variables.
  /// - Parameters:
  ///   - indexName: The name of the index to edit.
  ///   - isColumn: If true, edits column index map; if false, edits row index map.
  ///   - select: When 0, deletes all indices with indexName; otherwise, deletes the nth occurrence.
  ///   - moveTo: Requires select; moves deleted index to the specified position.
  void editIndices({
    required var indexName,
    required bool isColumn,
    int select = 0,
    int moveTo = -1,
  }) {
    Map inputMap = isColumn ? columnIndexMap : rowIndexMap;
    bool found = false;
    Set<int> deleteIndices = {}; // Tracks all indices to delete
    int length = isColumn ? columnLastIndexVal : rowLastIndexVal;
    // Enforce moveTo parameter to require select parameter
    if (moveTo > -1) {
      if (select == 0) {
        throw ArgumentError('Invalid select value for moveTo parameter');
      }
    }
    // Verify index name exists
    if (inputMap.containsKey(indexName)) {
      var valueList = inputMap[indexName];
      // Delete a single index occurrence
      if (select > 0) {
        --length;
        int valueIndexToDelete = select - 1;
        if (valueIndexToDelete < valueList.length) {
          deleteIndices.add(valueList[valueIndexToDelete]);
          valueList.removeAt(valueIndexToDelete);
          // Edit column types if a column is deleted
          if (isColumn == true) {
            List<Type> newColumnTypes = <Type>[];
            List<int> columnTypesLength =
                List<int>.generate(columnTypes.length, (i) => i);
            for (int i in columnTypesLength) {
              if (!(select == i)) {
                newColumnTypes.add(columnTypes[i]);
              }
            }
            columnTypes = newColumnTypes;
          }
          found = true;
        }
        // Delete all indices with the entered name
      } else {
        deleteIndices = Set<int>.from(valueList);
        if (isColumn == true) {
          List<Type> newColumnTypes = <Type>[];
          List<int> columnTypesLength =
              List<int>.generate(columnTypes.length, (i) => i);
          for (int i in columnTypesLength) {
            if (!deleteIndices.contains(i)) {
              newColumnTypes.add(columnTypes[i]);
            }
          }
          columnTypes = newColumnTypes;
        }
        inputMap.remove(indexName);
        found = true;
        length -= valueList.length as int;
      }
      if (valueList.isEmpty) {
        inputMap.remove(indexName);
      }
    } else {
      throw ArgumentError('Index value not found');
    }
    // Adjust indices
    if (found && deleteIndices.isNotEmpty) {
      // Moving an index, not deleting
      if (moveTo > -1) {
        if (!inputMap.containsKey(indexName)) {
          inputMap[indexName] = <int>[];
        }
        // Adjust indices for insertion
        inputMap.forEach((key, indexList) {
          List<int> adjustedIndices = [];
          int indexLength = indexList.length;
          for (int i = 0; i < indexLength; i++) {
            if (indexList[i] >= moveTo && indexList[i] < deleteIndices.first) {
              adjustedIndices.add(indexList[i] + 1);
            } else if ((indexList[i] <= moveTo) &&
                (indexList[i] > deleteIndices.first)) {
              adjustedIndices.add(indexList[i] - 1);
            } else {
              adjustedIndices.add(indexList[i]);
            }
          }
          if (key == indexName) {
            adjustedIndices.add(moveTo);
            adjustedIndices.sort();
          }
          inputMap[key] = adjustedIndices;
        });
      } else {
        inputMap.forEach((key, values) {
          List<int> adjustedIndices = [];
          int valuesLength = values.length;
          for (int i = 0; i < valuesLength; i++) {
            int decrement = deleteIndices.where((e) => e < values[i]).length;
            adjustedIndices.add(values[i] - decrement);
          }
          inputMap[key] = adjustedIndices;
        });
      }
    }
    if (moveTo > -1) {
      length++;
    }
    if (isColumn) {
      columnLastIndexVal = length;
    } else {
      rowLastIndexVal = length;
    }
  }

  // * Type Inference Methods *

  /// Scans a list and infers the types of columns, updating `columnTypes`.
  void scanTypes(var inputList) {
    // Assumes inputList is an iterable of lists (columns)
    if (inputList is List && inputList is! String) {
      int columnCounter = 0;
      for (var column in inputList) {
        Set<Type> typesInColumn = {};
        int elementCounter = 0;
        while (elementCounter < column.length) {
          typesInColumn.add(column[elementCounter].runtimeType);
          ++elementCounter;
        }
        // Determine column type based on collected types
        if (typesInColumn.contains(int) && typesInColumn.contains(double)) {
          columnTypes[columnCounter] = double;
        } else if (typesInColumn.every((e) => e == int)) {
          columnTypes[columnCounter] = int;
        } else if (typesInColumn.every((e) => e == double)) {
          columnTypes[columnCounter] = double;
        } else if (typesInColumn.every((e) => e == String)) {
          columnTypes[columnCounter] = String;
        } else {
          columnTypes[columnCounter] = Object;
        }
        ++columnCounter;
      }
    }
  }

  // * Utility Methods *

  /// Resets row indices to default integer values starting from zero.
  void reset_index() {
    var newList = List<dynamic>.generate(rowLastIndexVal + 1, (i) => i);
    indexer(newList, false, resetIndex: true);
  }

  /// Reverses a map by swapping its keys and values.
  Map reverseMap(Map map) {
    var reverseMap = {};
    map.forEach((key, values) {
      for (var value in values) {
        reverseMap[value] = key; // Assumes each integer is unique
      }
    });
    return reverseMap;
  }

  /// Returns in a List the ordered entries from an index map.
  /// - Parameters:
  ///   - inputIndex: The index map to process.
  ///   - isColumnIndex: If true, processes column index map; if false, row index map.
  List orderedEntries(Map inputIndex, bool isColumnIndex) {
    int length = isColumnIndex ? columnLastIndexVal + 1 : rowLastIndexVal + 1;
    List<dynamic> orderedList = List.filled(length, null, growable: false);
    for (var entry in inputIndex.entries) {
      for (var index in entry.value) {
        orderedList[index] = entry.key;
      }
    }
    return orderedList;
  }

  /// Clears the data of a DataFrame.
  /// - Parameters:
  ///   - except: Enter the properties that are to be kept.
  void clear({Set<String> except = const {}}) {
    if (!except.contains('data')) data = [];
    if (!except.contains('rowIndexMap')) rowIndexMap = {};
    if (!except.contains('rowLastIndexVal')) rowLastIndexVal = -1;
    if (!except.contains('columnIndexMap')) columnIndexMap = {};
    if (!except.contains('columnLastIndexVal')) columnLastIndexVal = -1;
    if (!except.contains('columnTypes')) columnTypes = <Type>[];
  }

  // * Computation Methods *

  /// Counts the occurrences of specified values in a column.
  /// - Parameters:
  ///   - columnIndex: The index of the column to search.
  ///   - values: The list of values to count.

  int countValues(int columnIndex, List values) {
    int n = 0;
    for (var e in values) {
      if (data[columnIndex].contains(e)) {
        ++n;
      }
    }
    return n;
  }
  /// Transposes a data matrix, swapping rows with columns.
  /// 
  /// - Parameters:
  ///   - matrix: The matrix to transpose, where each element is a row.
  ///   - checkType: If true, performs type inference on columns and initializes lists with specific types. Default is false.
  /// - Example:
  ///   ```dart
  ///   var transposedMatrix = transpose([[1, 2], [3, 4]]); // Returns [[1, 3], [2, 4]].
  ///   ```
  List transposeT(List matrix, {bool checkType = false}) {
    if (matrix.isEmpty) return [];
    if (matrix.isNotEmpty && matrix[0] is! List) {
      // Treat the input list as a single row
      return [matrix];
    }

    // Determine the length of the longest inner list
    int maxLength = matrix.fold(0, (int max, list) => max > list.length ? max : list.length);

    List transposed;

    if (checkType) {
      // *** Type collection done here
      // Ensure columnTypes is declared in the global scope before calling this function
      columnTypes = List.filled(maxLength, Object);

      // Infer the type for all the columns
      for (int j = 0; j < maxLength; j++) {
        Set<Type> typesInColumn = {}; // Holds the types in a single column
        // Iterate through the entire column j, adding the type found in each element
        for (int i = 0; i < matrix.length; i++) {
          if (matrix[i].length > j && matrix[i][j] != null) {
            typesInColumn.add(matrix[i][j].runtimeType);
          }
        }
        // Decision making - scan typesInColumn to determine what type should represent the entire column j
        if (typesInColumn.contains(int) && typesInColumn.contains(double) && typesInColumn.length == 2) {
          columnTypes[j] = double;  // Use double to cover both int and double
        } else if (typesInColumn.every((type) => type == int)) {
          columnTypes[j] = int;  // All are integers
        } else if (typesInColumn.every((type) => type == double)) {
          columnTypes[j] = double;  // All are doubles
        } else if (typesInColumn.every((type) => type == String)) {
          columnTypes[j] = String;  // All are strings
        } else if (typesInColumn.every((type) => type == bool)) {
          columnTypes[j] = bool;  // All are booleans
        } else {
          columnTypes[j] = Object;  // Mixed or other types, use Object
        }
      }

      // Create empty transposed matrix with type-specific lists
      transposed = [];
      for (int index = 0; index < maxLength; index++) {
        List<dynamic> column;
        if (columnTypes[index] == int) {
          column = List<int>.filled(matrix.length, 0, growable: true);
        } else if (columnTypes[index] == double) {
          column = List<double>.filled(matrix.length, 0.0, growable: true);
        } else if (columnTypes[index] == String) {
          column = List<String>.filled(matrix.length, '', growable: true);
        } else if (columnTypes[index] == bool) {
          column = List<bool>.filled(matrix.length, false, growable: true);
        } else {
          column = List<dynamic>.filled(matrix.length, null, growable: true);
        }
        transposed.add(column);
      }

      // Populate the transposed matrix and convert if necessary ints to doubles
      for (int i = 0; i < matrix.length; i++) {
        for (int j = 0; j < matrix[i].length; j++) {
          // Check and convert types if necessary
          if (columnTypes[j] == double && matrix[i][j] is int) {
            // Explicitly convert int to double if the column is expected to hold doubles
            transposed[j][i] = (matrix[i][j] as int).toDouble();
          } else {
            // Direct assignment when no type conversion is needed
            transposed[j][i] = matrix[i][j];
          }
        }
      }
    } else {
      // Create a list of lists with transposed dimensions without type checking
      transposed = List.generate(
        maxLength,
        (_) => List<dynamic>.filled(matrix.length, null, growable: true),
        growable: true,
      );

      // Populate the transposed matrix without type conversion
      for (int i = 0; i < matrix.length; i++) {
        for (int j = 0; j < matrix[i].length; j++) {
          transposed[j][i] = matrix[i][j];
        }
      }
    }
    return transposed;
  }
  /// Implements the [] operator for column access and column reordering.
  ///
  /// - []: If a single column name is provided, returns the data for that column.
  /// - [[]]: If multiple column names is provided inside of a List, returns a new DataFrame with the columns reordered.
  ///
  /// - Example:
  ///   ```dart
  ///   var columnData = df['ColumnName']; // Returns data for a single column.
  ///   ```
  operator [](var columnName) {
    // Get a List of all the column indices with name entered as the argument 'columnName'
      var columnIndices = columnIndexMap[columnName];
      // If column name key returns null, throw an error.
      if (columnIndices == null) {
        throw StateError('Column name not found');
      }
      final List<List> columnData = [];
      for(int index in columnIndices) { //Iterate through column indices of input name
        columnData.add(data[index]);
      }
      // If column name is unique, return only a single list, otherise it's a List of Lists of all columns of that name.
      return columnIndices.length == 1 ? columnData.first : columnData;
  }

  /// Replaces an entire column of data. If multiple columns share the same name, 
  /// they will all be replaced with the same List argument.
  void operator []=(var columnName, List inputData) {
    // Check that the input data length matches the number of rows
    if (inputData.length != rowLastIndexVal + 1) {
      throw ArgumentError('Input data must match number of rows');
    }
    
    Type newType;
    // Check if inputData is explicitly typed
    bool isExplicitlyTyped = inputData is List<int> ||
                            inputData is List<double> ||
                            inputData is List<String> ||
                            inputData is List<bool> ||
                            inputData is List<num>;
    // Case 1: Generic is an explicit type 
    List explicitList = [];
    if (isExplicitlyTyped) {
      // Use checkListType for explicitly typed lists
      newType = checkListType(inputData);
      explicitList = inputData;
    } else { 
    // Case 2: Generic is undetermined; shows as List<dynamic>. Determine the actual generic
      // Get actual List type. Use manualCheckListType for List<dynamic>
      newType = manualCheckListType(inputData);
      // Create an explicit list of the inferred type
      
      if(newType == num){
        explicitList = <double>[];
        newType = double;
        for(num number in inputData){
          if(number is int){
            explicitList.add(number.toDouble());
          } else {
            explicitList.add(number);
          }
        }
      } else {
        explicitList = createListFromType(newType);
        for(var e in inputData){
          explicitList.add(e);
        }        
      }
    }
    // Replace data and update column types
    if (columnIndexMap.containsKey(columnName)) {
      List columnIndices = columnIndexMap[columnName];
      for (int index in columnIndices) {
        data[index] = explicitList;
        columnTypes[index] = newType;
      }
    } else {
      // Add new column
      indexer([columnName], true);
      data.add(explicitList);
      columnTypes.add(newType);
    }
  }
}

// * Utility Functions *

/// Returns the runtime type of a List's generic type if it is explicit.
/// - Parameter column: The list to check.
/// - Returns: The determined type of the list.
Type checkListType(List column) {
  if (column is List<int> || column is List<int?>) {
    return int;
  } else if (column is List<double> || column is List<double?>) {
    return double;
  } else if (column is List<num> || column is List<num?>) {
    return num;
  } else if (column is List<String> || column is List<String?>) {
    return String;
  } else if (column is List<bool>) {
    return bool;
  } else {
    return Object;
  }
}

/// Determines a List's actual type by checking each element.
/// - Parameter column: The list to inspect.
/// - Returns: The inferred type of the list.
Type manualCheckListType(List column) {
  // Initially assume the most general type
  Type listType = Object;
  // Set to hold unique element types
  Set<Type> listTypes = {};
  // Collect runtime types of all elements
  for (var element in column) {
    listTypes.add(element.runtimeType);
  }
  // Determine the general type based on collected types
  if (listTypes.length == 1) {
    // All elements are of the same type
    listType = listTypes.first;
  } else if (listTypes.every((type) => type == int || type == double)) {
    // All types are int or double
    listType = num;
  } else {
    // Mixed types, default to Object
    listType = Object;
  }
  return listType;
}

/// Creates a list of a specific type.
/// - Parameter type: The type to create a list for.
/// - Returns: An empty list of the specified type.
List createListFromType(Type type) {
  if (identical(type, int)) {
    return <int>[];
  } else if (identical(type, double)) {
    return <double>[];
  } else if (identical(type, num)) {
    return <num>[];
  } else if (identical(type, String)) {
    return <String>[];
  } else if (identical(type, bool)) {
    return <bool>[];
  } else {
    return <Object>[];
  }
}
/// Expands a list of indices that contains List elements into a flat range of integers.
/// e.g. columnIndices = [0, [2, 4], 6] would return [0, 2, 3, 4, 6] with expandIndices
List<int> expandIndices(List<dynamic>? indices) {
    List<int> expanded = [];
    if (indices != null) {
      for (var index in indices) {
        if (index is int) {
          expanded.add(index);
        } else if (index is List && index.length == 2 && index[0] is int && index[1] is int) {
          expanded.addAll(List.generate(index[1] - index[0] + 1, (i) => index[0] + i));
        }
      }
    }
    return expanded;
}