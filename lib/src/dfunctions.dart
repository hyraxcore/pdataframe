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