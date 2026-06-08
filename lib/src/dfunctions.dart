import 'dart:typed_data';

import 'package:pdataframe/src/nlist.dart';

bool areListsEqual(List list1, List list2) {
  if (list1.length != list2.length) {
    return false;
  }
  for (int i = 0; i < list1.length; i++) {
    if (list1[i] != list2[i]) {
      return false;
    }
  }
  return true;
}

bool areIterablesEqual(Iterable iterable1, Iterable iterable2) {
  Iterator iterator1 = iterable1.iterator;
  Iterator iterator2 = iterable2.iterator;

  while (iterator1.moveNext() && iterator2.moveNext()) {
    if (iterator1.current != iterator2.current) {
      return false;
    }
  }
  return !iterator1.moveNext() && !iterator2.moveNext();
}

bool areIterablesEqualUnordered(Iterable iterable1, Iterable iterable2) {
  return Set.from(iterable1).containsAll(Set.from(iterable2)) &&
         Set.from(iterable2).containsAll(Set.from(iterable1));
}

// Takes a dataset and checks if it contains any of the values in searchValues.
int countainsValues(Iterable dataset, Iterable searchValues){
  if(searchValues is Map || dataset is Map){
    throw ArgumentError('Invalid collection type');
  }
  Set searchSet = searchValues.toSet();
  int counter = 0;
  for(var e in dataset){
    if(searchSet.contains(e)){
      ++counter;
    }
  }
  return counter;
}

// Utility function to compare sets
bool setEquals<T>(Set<T> a, Set<T> b) {
  if (a.length != b.length) return false;
  for (var element in a) {
    if (!b.contains(element)) return false;
  }
  return true;
}

Stream<List<T>> splitData<T>(List<T> list, int n) async* {
  for (var i = 0; i < list.length; i += n) {
    yield list.sublist(i, i + n < list.length ? i + n : list.length);
  }
}

Stream<List<T>> splitDataI<T>(Iterable<T> iterable, int n) async* {
  var iterator = iterable.iterator;
  while (true) {
    var chunk = <T>[];
    for (var i = 0; i < n && iterator.moveNext(); i++) {
      chunk.add(iterator.current);
    }
    if (chunk.isEmpty) break;
    yield chunk;
  }
}

/// Creates a list of a specific type.
/// - Parameter type: The type to create a list for.
/// - Returns: An empty list of the specified type.
List createListFromType(Type type) {
  if (identical(type, int)) {
    return <int>[];
  } else if (identical(type, double)) {
    return <double>[];
  } else if (identical(type, NList)) {
    return NList([], type: Int32List);
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
// * Utility Functions *

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

/// Returns the runtime type of a List's generic type if it is explicit.
/// - Parameter column: The list to check.
/// - Returns: The determined type of the list.
Type checkListGenericType(List column) {
  if (column is NList) {
    if (column.dtype == Int32List) {
      return Int32List;
    } else if (column.dtype == Float32List) {
      // ADDED
      return Float32List;
    } else if (column.dtype == Float64List) {
      return Float64List;
    } else {
      return Object;
    }
  } else if (column is Float32List) {
    // ADDED
    return Float32List;
  } else if (column is List<int> || column is List<int?>) {
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
/// Determines if a List is of the general type `num` or `Object` by checking each element.
/// - Parameter column: The list to inspect.
/// - Returns: The inferred type of the list.
Type manualCheckListType(List data) {
  // Initially assume the most general type
  Type listType = Object;
  // Set to hold unique element types
  Set<Type> listTypes = {};
  // Collect runtime types of all elements
  for (var element in data) {
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