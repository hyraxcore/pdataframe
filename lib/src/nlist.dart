import 'dart:typed_data';
import 'dart:collection';
import 'package:pdataframe/src/dfunctions.dart';

/// A List adapter for [NumberList].
final class NList extends ListBase<Object?> {
  final NumberList _numberList;

  /// Accepts a normal [List] and builds the backing [NumberList].
  NList(List input, {Type type = dynamic}) : _numberList = NumberList(input, type: type);
  
  /// Wrap an existing backing store without copying.
  NList._fromBacking(this._numberList);
  
  @override
  /// The number of stored elements.
  int get length => _numberList.length;
  
  @override
  set length(int newLength) => _numberList.length = newLength;
  
  @override
  /// Returns the element at the given [index].
  Object? operator [](int index) => _numberList[index];
  
  @override
  /// Assigns [newData] to the element at the given [index].
  void operator []=(int index, Object? value) => _numberList[index] = value;
  
  @override
  /// Adds a single [value] to the end of this list.
  ///
  /// ```dart
  /// final example = Nlist([0, 1]);
  /// example.add(2);
  /// print(example); // [0, 1, 2]
  /// ```
  void add(Object? element) => _numberList.add(element);
  
  @override
  /// Adds all elements from [backing] to the end of this list.
  ///
  /// ```dart
  /// final example = [0, 1];
  /// example.addAll([2, 3]);
  /// print(example); // [0, 1, 2, 3]
  /// ```
  void addAll(Iterable<Object?> iterable) => _numberList.addAll(iterable);
  
  @override
  /// Returns an iterator over the stored elements.
  ///
  /// ```dart
  /// final example = NList([1, 2, 3]);
  /// for (final value in example.iterator) {
  ///   print(value); // 1, 2, 3
  /// }
  /// ```
  Iterator<Object?> get iterator => _numberList.iterator;
  
  @override
  /// Returns a string representation of the stored elements.
  ///
  /// ```dart
  /// final example = NList([1, 2, 3]);
  /// print(example.toString()); // [1, 2, 3]
  /// ```
  String toString() => _numberList.toString();
  /// Returns the data type of the list.
  Type get dtype => _numberList.dtype;
  //get backing => _numberList.backing; // For Testing
}

// Notes:
//
// This class is a wrapper specifically for Int32List and Float64List.
// If the data cannot be stored in a typed list the implementation
// falls back to a regular growable List.
//
// Null handling: Typed lists cannot store null, so any incoming null value is
// converted to double.nan. If the current storage is Int32List, it must be promoted
// to Float64List before storing double.nan. Otherwise, users who require true
// nullability should use List<int?> directly.
//
// Possible changes: 
//  
// Null conversion: 
// If null-conversion policy is ever revised, the only locations
// that perform null -> double.nan conversion are:
//   - the constructor (which does not accept nullable input lists),
//   - the index assignment operator ([]=),
//   - add() and addAll().
//
// List<Object?> promotion: 
// Instead of storage type promoting to List<Object?> when non-numerical data
// is added via add(), addAll(), and []=, alternative isto just throw an error and enforce
// only numerical addition, since there is no benefit to using NumberList unless utilizing typed lists. 
//
// Operation: 
// When the size is maxed, _length == _allocatedLength must be checked before computeGrowth is called. computeGrowth takes in the current capacity and
// whether each element is 4 or 8 bits (depends if it's Int32List or Float64List)

class NumberList {
  
  // * Fields *
 
  dynamic _data;  // Storage type will be either Int32List, Float64List, or a generic List
  late Type _colType;   // Only int, double, or Object
  int _length;             // number of elements
  int _allocatedLength;   // length of elements+buffer, this value is always private

  // * Getters + Setters *
  //get backing => _data; // For Testing
  Type get dtype => _colType;
  int get length => _length;

  set length(int value) {
    // 1. Value is lesser than 0
    if (value < 0) {
      throw RangeError('length must be non-negative');
    }
    final int oldLength = _length;
    // 2. Value is the same; no change
    if (value == oldLength) return;
    // 3. Value is smaller: shrink
    if (value < oldLength) {
      // Optional: clear truncated values for numeric lists
      if (_colType == int) {
        final a = _data as Int32List;
        for (var i = value; i < oldLength; i++) {
          a[i] = 0;
        }
      } else if (_colType == double) {
        final a = _data as Float64List;
        for (var i = value; i < oldLength; i++) {
          a[i] = double.nan;
        }
      }
      _length = value;
      return;
    }

    // 4. Value is greater; grow
    // 4.a. List is int: must promote first because new values are NaN
    if (_colType == Int32List) { 
      _promoteInt32ToFloat64();
    }
    // 4.b. List is double
    if (_colType == Float64List) {
      while (_allocatedLength < value) {
        _growNumericBuffer(_colType);
      }
      final a = _data as Float64List;
      for (var i = oldLength; i < value; i++) {
        a[i] = double.nan;
      }
    } else {  
    // 4.c. Object list: treat as normal List
      final a = _data as List;
      a.length = value;
    }
    _length = value;
  }

  // * Constructors *
  
  // Notes: if `type` is provided (int/double/Object), skip infer check.
  // If `type` is dynamic, infer while copying: int -> double -> Object.
  NumberList(List input, {type = dynamic})
      : _colType = type,
        _data = input,
        _allocatedLength = input.length,
        _length = input.length {
    // Helper: fallback to plain List<Object?> backing (never throw in constructor).
    void fallbackToObject() {
      final temp = List<Object?>.filled(_length, null, growable: true);
      for (var i = 0; i < _length; i++) {
        temp[i] = input[i];
      }
      _data = temp;
      _colType = Object;
      _allocatedLength = temp.length;
    }
    // Zero-length input: choose an empty backing based on requested type.
    if (_length == 0) {
      if (type == int) {
        _data = Int32List(0);
        _colType = Int32List;
      } else if (type == double || type == num) {
        _data = Float64List(0);
        _colType = Float64List;
      } else if (type == Float32List) {
        _data = Float32List(0);
        _colType = Float32List;
      } 
      else {
        _data = <Object?>[];
        _colType = Object;
      }
      _allocatedLength = 0;
      return;
    }
    // If user specifies int: try Int32List; on any incompatibility fallback to List<Object?>.
    if (type == int) {
      if (input is Int32List) {
        _data = input;
        _colType = Int32List;
        _allocatedLength = input.length;
        return;
      }
      final temp = _createEmptyList(_length, int) as Int32List;
      for (var i = 0; i < _length; i++) {
        final v = input[i];
        if (v is int && _fitsInt32(v)) {
          temp[i] = v;
        } else {
          fallbackToObject();
          return;
        }
      }
      _data = temp;
      _colType = Int32List;
      return;
    }
    // If user specifies double/num: try Float64List; allow null->NaN; else fallback List<Object?>.
    if (type == double || type == num) {
      if (input is Float64List) {
        _data = input;
        _colType = Float64List;
        _allocatedLength = input.length;
        return;
      }
      final temp = _createEmptyList(_length, double) as Float64List;
      for (var i = 0; i < _length; i++) {
        final v = input[i];
        if (v == null) {
          temp[i] = double.nan;
        } else if (v is num) {
          temp[i] = v.toDouble();
        } else {
          fallbackToObject();
          return;
        }
      }
      _data = temp;
      _colType = Float64List;
      return;
    }
    // If user specified some non-numeric type explicitly, just fallback immediately.
    if (type != dynamic) {
      fallbackToObject();
      return;
    }
    // Start as Int32List; promote to Float64List on first null or non-int num;
    // fallback to Object backing on first non-(num|null).
    Int32List i32 = _createEmptyList(_length, int) as Int32List;
    Float64List? f64;
    var mode = 0; // 0=Int32List, 1=Float64List

    for (var i = 0; i < _length; i++) {
      final v = input[i];

      if (mode == 0) { // Int32List
        if (v == null) { // null forces float64
          f64 = Float64List(_allocatedLength);
          for (var j = 0; j < i; j++) {
            f64[j] = i32[j].toDouble();
          }
          f64[i] = double.nan;
          mode = 1;
          continue;
        }
        if (v is int && _fitsInt32(v)) {
          i32[i] = v;
          continue;
        }
        if (v is num) {
          // any non-int num or int outside int32 range forces float64
          f64 = Float64List(_allocatedLength);
          for (var j = 0; j < i; j++) {
            f64[j] = i32[j].toDouble();
          }
          f64[i] = v.toDouble();
          mode = 1;
          continue;
        }
        // Non-numeric => object backing
        fallbackToObject();
        return;
      } else { // mode == 1 (Float64List) 
        if (v == null) {
          f64![i] = double.nan;
        } else if (v is num) {
          f64![i] = v.toDouble();
        } else {
          fallbackToObject();
          return;
        }
      }
    }
    // Finalize
    if (mode == 0) {
      _data = i32;
      _colType = Int32List;
    } else {
      _data = f64!;
      _colType = Float64List;
    }
  }

  // * Storage + Growth Policy *
  // initialCapacity() allocates initial size 
  // growNumericBuffer() handles adding data, growth (computeGrowth()), and type of container. computeGrowth() used to calculate growth 
  
  static const _eightMB = 8 * 1024 * 1024;
  static const _sixteenMB = 16 * 1024 * 1024;
  int _elementBytes(Type t) => (t == int) ? 4 : 8; // int32 vs float64

  // Used only during initialization
  int _initialCapacity(int inputLength, int elementByteSize) {
    if (inputLength <= 0) return 0;
    final thresholdElems = _eightMB ~/ elementByteSize; // 8 MiB in elements
    if (inputLength < thresholdElems) {
      final cap = (inputLength * 3 + 1) ~/ 2; // ceil(1.5×)
      _allocatedLength = cap;
      return cap;
    } else {
      _allocatedLength = inputLength;
      return inputLength; // exact for large inputs
    }
  }
  // Calculate the amount the list should grow by
  int _computeGrowth(int oldCap, int elementBytes, {int fixedIncrease = _sixteenMB}) {
    if (oldCap <= 0) {
      _allocatedLength = 8;
      return _allocatedLength;
    }
    final int halfOldBytes = (oldCap * elementBytes) >> 1;
    if (halfOldBytes <= fixedIncrease) {
      _allocatedLength = (oldCap * 3 + 1) ~/ 2;
      return _allocatedLength;
    } else {
      final int addElemsRaw = fixedIncrease ~/ elementBytes;
      final int addElems = addElemsRaw > 0 ? addElemsRaw : 1;
      _allocatedLength = oldCap + addElems;
      return _allocatedLength;
    }
  }
  // Grow list buffer; copies only the first `length` elements
  // Usage: This function requires you to know first if the data buffer is filled before calling (_length == _allocatedLength). If so, all it requires
  //        is the Type you want the new buffer to be; it can only be int or double. Any other type, the backing should be a regular List.
  // Note: Buffer space is filled with 0.
  void _growNumericBuffer(Type t) {
    final elemBytes = _elementBytes(t);
    final newCap = _computeGrowth(_allocatedLength, elemBytes);
    if (t == int) {
      final old = _data as Int32List;
      final next = _createEmptyList(newCap, int) as Int32List;
      next.setRange(0, _length, old); // copy logical length only
      _data = next;
    } else {
      final old = _data as Float64List;
      final next = _createEmptyList(newCap, double) as Float64List;
      next.setRange(0, _length, old); // copy logical length only
      _data = next;
    }
    _allocatedLength = newCap;
  }
  dynamic _createEmptyList(int rawListLength, Type colType) {
    final elemBytes = _elementBytes(colType);
    final realListLength = _initialCapacity(rawListLength, elemBytes);
    if (colType == int) {
      return Int32List(realListLength);
    } else if (colType == double) {
      return Float64List(realListLength);
    } else {
      return createListFromType(colType);
    }
  }

  // * Data Editing *

  /// Returns the element at the given [index].
  operator [](int index) {
    if (index < _length) {
      return _data[index];
    } else {
      throw RangeError(index);
    }
  }

  //  Logic for Data Editing:
  //  1. If existing data is type int: check if new data is int, if it is, check if size fits i32l, else promote to f64l.
  //     If new data is num (or int but out of range), promote to float64l. Otherwise, promote to Object.   
  //  2.  If existing type is double promote to float64l.
  //  3.  If existing type is neither int/double, that is a general list, just assign directly. 
  void operator []=(int index, Object? newData) {
    newData ??= double.nan; // current policy is to convert all null to double.nan
    if (index < 0 || index >= _length) {
      throw RangeError.index(index, this, 'index', 'Index out of range', _length);
    }
    // 1. Int32List 
    if (_colType == Int32List) {
      if (newData is int) {
        if (_fitsInt32(newData)) {
          (_data as Int32List)[index] = newData;
        } else {
          _promoteInt32ToFloat64();
          (_data as Float64List)[index] = newData.toDouble();
        }
        return;
      }
      if (newData is num) {
        _promoteInt32ToFloat64();
        (_data as Float64List)[index] = newData.toDouble();
        return;
      }
      _promoteToObjectList();
      (_data as List)[index] = newData;
      return;
    }
    // 2. Float64List 
    if (_colType == Float64List) {
      if (newData is num) {
        (_data as Float64List)[index] = newData.toDouble();
        return;
      }
      _promoteToObjectList();
      (_data as List)[index] = newData;
      return;
    }
    // 3. Standard List
    (_data as List)[index] = newData;
  }

  // * Data Editing Helpers *
  
  // Min and max values for Int32List
  static const int _int32Min = -2147483648;
  static const int _int32Max = 2147483647;
  bool _fitsInt32(int v) => v >= _int32Min && v <= _int32Max;

  // Promote from Int32List to Float64List depending on conditions
  void _promoteInt32ToFloat64() {
    if (_data is Float64List) {
      _colType = Float64List;
      return;
    }
    final Int32List old = _data as Int32List;
    final Float64List next = Float64List(_allocatedLength);
    for (var i = 0; i < _length; i++) {
      next[i] = old[i].toDouble();
    }
    _data = next;
    _colType = Float64List;
  }
  // Promote to List<Object> depending on conditions
  void _promoteToObjectList() { 
    final List<Object> newList = List<Object>.filled(_length, '', growable: true);
    if (_data is Int32List) {
      final Int32List old = _data as Int32List;
      for (var i = 0; i < _length; i++) {newList[i] = old[i];}
    } else if (_data is Float64List) {
      final Float64List old = _data as Float64List;
      for (var i = 0; i < _length; i++) {newList[i] = old[i];}
    } else {
      final List old = _data as List;
      for (var i = 0; i < _length; i++) {newList[i] = old[i];}
    }
    _data = newList;
    _colType = Object;
    _allocatedLength = newList.length; // generic lists are growable; track logical length
  }

  void add(Object? value) {
    // 1. int input
    if (_colType == Int32List) {
      if (value is int && _fitsInt32(value)) {
        if (_length == _allocatedLength) _growNumericBuffer(int);
        (_data as Int32List)[_length] = value;
        _length += 1;
        return;
      }
      value ??= double.nan;  // null to double.nan conversion
      if (value is num) {
        // promote to Float64 and append
        _promoteInt32ToFloat64();
        if (_length == _allocatedLength) _growNumericBuffer(double);
        (_data as Float64List)[_length] = value.toDouble();
        _length += 1;
        return;
      }
      // non-num: promote to Object list
      _promoteToObjectList();
      (_data as List).add(value);
      _length += 1;
      return;
    }
    // 2. double input
    if (_colType == Float64List) {
      value ??= double.nan;  // null to double.nan conversion
      if (value is num) {
        if (_length == _allocatedLength) _growNumericBuffer(double);
        (_data as Float64List)[_length] = value.toDouble();
        _length += 1;
        return;
      }
      // non-num: promote to Object list
      _promoteToObjectList();
      (_data as List).add(value);
      _length += 1;
      return;
    }
    // Standard List (Object/dynamic)
    // note: no null conversion; let user decide how to handle when List is not numeric (back to default behaviour)
    (_data as List).add(value);
    _length += 1;
  }

  void addAll(Iterable<Object?> values) {
    for (var v in values) {
      v ??= double.nan;
      if (_colType == Int32List) {
        if (v is int && _fitsInt32(v)) {
          _appendInt32(v);
          continue;
        }
        if (v is num) {
          _promoteInt32ToFloat64();
          _appendF64(v.toDouble());
          continue;
        }
        // null or non-numeric: promote to Object
        _promoteToObjectList();
        (_data as List).add(v);
        _length += 1;
        continue;
      }

      if (_colType == Float64List) {
        if (v is num) {
          _appendF64(v.toDouble());
          continue;
        }
        // null or non-numeric: promote to Object
        _promoteToObjectList();
        (_data as List).add(v);
        _length += 1;
        continue;
      }
      // Generic list (Object/dynamic): keep as List
      (_data as List).add(v);
      _length += 1;
    }
  }

  // * addAll() helpers *

  // Check capacity and append one Int32 value
  void _appendInt32(int v) {
    if (_length == _allocatedLength) {
      _growNumericBuffer(int);
    }
    (_data as Int32List)[_length] = v;
    _length += 1;
  }

  // Check capacity and append one Float64 value
  void _appendF64(double v) {
    if (_length == _allocatedLength) {
      _growNumericBuffer(double);
    }
    (_data as Float64List)[_length] = v;
    _length += 1;
  }

  // * Accessors and Views * 

  Iterator<Object?> get iterator {
    if (_colType == Int32List) {
      final a = _data as Int32List;
      return a.getRange(0, length).map<Object?>((e) => e).iterator;
    } else if (_colType == Float64List) {
      final a = _data as Float64List;
      return a.getRange(0, length).map<Object?>((e) => e).iterator;
    } else {
      final a = _data as List<Object?>;
      return a.getRange(0, length).iterator; 
    }
  }
  
  // * Overrides *
    
  @override
  String toString() {
    if (_colType == Int32List) {
      final col = _data as Int32List;
      final view = col.buffer.asInt32List(col.offsetInBytes, _length);
      return view.toString();
    } else if (_colType == Float64List) {
      final col = _data as Float64List;
      final view = col.buffer.asFloat64List(col.offsetInBytes, _length);
      return view.toString();
    } else {
      return _data.toString();
    }
  }
}