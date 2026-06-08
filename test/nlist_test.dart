import 'package:test/test.dart';
import 'package:pdataframe/src/nlist.dart';
import 'dart:typed_data';

void main() {
  group('NList constructor tests', () {
    test('List<int> infers Int32List storage and int dtype', () {
      final col = NList(<int>[1, 2, 3]);
      expect(col.length, equals(3));
      expect(col.dtype, equals(Int32List));
      expect(col.backing, isA<Int32List>());
      expect([for (var i = 0; i < col.length; i++) col[i]], equals([1, 2, 3]));
    });

    test('List<dynamic> of ints still infers Int32List and int dtype', () {
      final it2 = <dynamic>[2, 3, 4];
      final col = NList(it2);
      expect(col.length, equals(3));
      expect(col.dtype, equals(Int32List));
      expect(col.backing, isA<Int32List>());
      expect([for (var i = 0; i < col.length; i++) col[i]], equals([2, 3, 4]));
    });

    test('List<String> infers Object-backed column', () {
      final col = NList(<String>['hi', 'hello']);
      expect(col.length, equals(2));
      expect(col.dtype, equals(Object));
      expect(col.backing, isA<List>());
      List<Object?> vals = [];
      for (var it = col.iterator; it.moveNext();) {
        vals.add(it.current);
      }
      expect(vals, equals(['hi', 'hello']));
    });

    test('Mixed numeric (int + double) infers Float64List and double dtype', () {
      final col = NList([2, 5.3]);
      expect(col.length, equals(2));
      expect(col.dtype, equals(Float64List));
      expect(col.backing, isA<Float64List>());
      expect([for (var i = 0; i < col.length; i++) col[i]], equals([2.0, 5.3]));
    });

    test('type: Float32List creates Float32List-backed column', () {
      final col = NList([1.0, 2.0, 3.0], type: Float32List);
      expect(col.length, equals(3));
      expect(col.dtype, equals(Float32List));
      expect(col.backing, isA<Float32List>());
      expect([for (var i = 0; i < col.length; i++) col[i]], equals([1.0, 2.0, 3.0]));
    });

    test('Empty NList with type: Float32List has Float32List backing', () {
      final col = NList([], type: Float32List);
      expect(col.length, equals(0));
      expect(col.dtype, equals(Float32List));
      expect(col.backing, isA<Float32List>());
    });
  });

  group('NList add() tests', () {
    test('add(int) on int column appends without promotion', () {
      final col = NList(<int>[1, 2, 3]);
      col.add(4); col.add(5); col.add(6);
      expect(col.dtype, equals(Int32List));
      expect(col.backing, isA<Int32List>());
      expect(col.length, equals(6));
      expect([for (var i = 0; i < col.length; i++) col[i]], equals([1, 2, 3, 4, 5, 6]));
    });

    test('add(double) promotes int column to double', () {
      final col = NList(<int>[1, 2, 3]);
      col.add(6.2);
      expect(col.dtype, equals(Float64List));
      expect(col.backing, isA<Float64List>());
      expect(col.length, equals(4));
      expect([for (var i = 0; i < col.length; i++) col[i]], equals([1.0, 2.0, 3.0, 6.2]));
    });

    test('add(null) on numeric column converts to double.nan', () {
      final col = NList(<int>[1, 2, 3]);
      col.add(null);
      expect(col.dtype, equals(Float64List));
      expect(col.backing, isA<Float64List>());
      expect(col.length, equals(4));
      final last = col[col.length - 1] as double;
      expect(last.isNaN, isTrue);
    });

    test('add(non-numeric) on double column promotes to Object', () {
      final col = NList([1, 2.5]);
      col.add('hi');
      expect(col.dtype, equals(Object));
      expect(col.backing, isA<List>());
      expect(col.length, equals(3));
      expect(col[0], equals(1.0));
      expect(col[1], equals(2.5));
      expect(col[2], equals('hi'));
    });

    test('add(num) on Float32List column appends as float', () {
      final col = NList([1.0, 2.0], type: Float32List);
      col.add(3.0);
      expect(col.dtype, equals(Float32List));
      expect(col.backing, isA<Float32List>());
      expect(col.length, equals(3));
    });

    test('add(null) on Float32List column converts to double.nan', () {
      final col = NList([1.0, 2.0], type: Float32List);
      col.add(null);
      expect(col.dtype, equals(Float32List));
      expect(col.length, equals(3));
      final last = col[col.length - 1] as double;
      expect(last.isNaN, isTrue);
    });

    test('add(non-numeric) on Float32List column promotes to Object', () {
      final col = NList([1.0, 2.0], type: Float32List);
      col.add('hi');
      expect(col.dtype, equals(Object));
      expect(col.backing, isA<List>());
      expect(col.length, equals(3));
      expect(col[2], equals('hi'));
    });
  });

  group('NList addAll() tests', () {
    test('addAll(ints) on int column appends values', () {
      final col = NList([1, 2, 3]);
      final toAdd = [5, 6, 7, 8, 9, 10, 11, 12, 13, 14];
      col.addAll(toAdd);
      expect(col.dtype, equals(Int32List));
      expect(col.backing, isA<Int32List>());
      expect(col.length, equals(3 + toAdd.length));
      expect([for (var i = 0; i < col.length; i++) col[i]],
          equals([1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]));
    });

    test('addAll with null and non-numeric on double column promotes to Object', () {
      final col = NList([1, 2.0]);
      col.addAll([3.5, null, 'x']);
      expect(col.length, equals(5));
      expect(col.dtype, equals(Object));
      expect(col.backing, isA<List>());
      expect(col[0], equals(1.0));
      expect(col[1], equals(2.0));
      expect(col[2], equals(3.5));
      final nanVal = col[3] as double;
      expect(nanVal.isNaN, isTrue);
      expect(col[4], equals('x'));
    });

    test('addAll on empty list', () {
      final col = NList(<int>[], type: int);
      col.addAll(<Object?>[]);
      expect(col.length, equals(0));
      expect(col.dtype, equals(Int32List));
    });

    test('addAll(nums) on Float32List column appends values', () {
      final col = NList([1.0, 2.0], type: Float32List);
      col.addAll([3.0, 4.0, 5.0]);
      expect(col.dtype, equals(Float32List));
      expect(col.backing, isA<Float32List>());
      expect(col.length, equals(5));
    });

    test('addAll with non-numeric on Float32List column promotes to Object', () {
      final col = NList([1.0, 2.0], type: Float32List);
      col.addAll([3.0, 'x']);
      expect(col.dtype, equals(Object));
      expect(col.backing, isA<List>());
      expect(col.length, equals(4));
      expect(col[3], equals('x'));
    });
  });

  group('NList index operator tests', () {
    test('[] returns correct values inside range', () {
      final col = NList([10, 20, 30]);
      expect(col[0], equals(10));
      expect(col[1], equals(20));
      expect(col[2], equals(30));
    });

    test('[] throws RangeError when index is out of range', () {
      final col = NList([10, 20, 30]);
      expect(() => col[3], throwsRangeError);
      expect(() => col[-1], throwsRangeError);
    });

    test('[]= with int inside range keeps int dtype', () {
      final col = NList(<int>[1, 2, 3]);
      col[2] = 100;
      expect(col.dtype, equals(Int32List));
      expect(col.backing, isA<Int32List>());
      expect([for (var i = 0; i < col.length; i++) col[i]], equals([1, 2, 100]));
    });

    test('[]= with double promotes int column to double', () {
      final col = NList(<int>[1, 2, 3]);
      col[1] = 2.5;
      expect(col.dtype, equals(Float64List));
      expect(col.backing, isA<Float64List>());
      expect([for (var i = 0; i < col.length; i++) col[i]], equals([1.0, 2.5, 3.0]));
    });

    test('[]= with null on int column stores double.nan and promotes to double', () {
      final col = NList(<int>[1, 2, 3]);
      col[1] = null;
      expect(col.dtype, equals(Float64List));
      expect(col.backing, isA<Float64List>());
      final v = col[1] as double;
      expect(v.isNaN, isTrue);
    });

    test('[]= with non-numeric promotes numeric column to Object', () {
      final col = NList(<int>[1, 2, 3]);
      col[1] = 'hi';
      expect(col.dtype, equals(Object));
      expect(col.backing, isA<List>());
      expect([for (var i = 0; i < col.length; i++) col[i]], equals([1, 'hi', 3]));
    });

    test('[]= with num on Float32List column stores value', () {
      final col = NList([1.0, 2.0, 3.0], type: Float32List);
      col[1] = 9.0;
      expect(col.dtype, equals(Float32List));
      expect(col.backing, isA<Float32List>());
      expect(col[1], equals(9.0));
    });

    test('[]= with non-numeric on Float32List column promotes to Object', () {
      final col = NList([1.0, 2.0, 3.0], type: Float32List);
      col[1] = 'hi';
      expect(col.dtype, equals(Object));
      expect(col.backing, isA<List>());
      expect(col[1], equals('hi'));
    });
  });

  group('NList iterator and toString tests', () {
    test('iterator yields only logical elements', () {
      final col = NList(<int>[1, 2, 3]);
      var it = col.iterator;
      List vals = [];
      while (it.moveNext()) { vals.add(it.current); }
      expect(vals, equals([1, 2, 3]));
    });

    test('toString() prints only logical elements', () {
      final col = NList(<int>[1, 2, 3]);
      expect(col.toString(), equals('[1, 2, 3]'));
    });

    test('iterator on Float32List yields correct values', () {
      final col = NList([1.0, 2.0, 3.0], type: Float32List);
      var it = col.iterator;
      List vals = [];
      while (it.moveNext()) { vals.add(it.current); }
      expect(vals, equals([1.0, 2.0, 3.0]));
    });

    test('toString() on Float32List prints only logical elements', () {
      final col = NList([1.0, 2.0, 3.0], type: Float32List);
      expect(col.toString(), equals('[1.0, 2.0, 3.0]'));
    });
  });

  group('NList .length assignment tests', () {
    test('new length is lesser', () {
      final ltest = NList(<int>[1, 2, 3]);
      ltest.length = 2;
      expect(ltest, equals([1, 2]));
      expect(ltest.length, equals(2));
    });

    test('new length is greater', () {
      final ltest = NList(<int>[1, 2]);
      ltest.length = 4;
      expect(ltest, equals([1.0, 2.0, isNaN, isNaN]));
      expect(ltest.length, equals(4));
    });

    test('new length is lesser on Float32List', () {
      final ltest = NList([1.0, 2.0, 3.0], type: Float32List);
      ltest.length = 2;
      expect(ltest.length, equals(2));
      expect(ltest[0], equals(1.0));
      expect(ltest[1], equals(2.0));
    });

    test('new length is greater on Float32List fills with nan', () {
      final ltest = NList([1.0, 2.0], type: Float32List);
      ltest.length = 4;
      expect(ltest.length, equals(4));
      expect(ltest[0], equals(1.0));
      expect(ltest[1], equals(2.0));
      final v2 = ltest[2] as double;
      final v3 = ltest[3] as double;
      expect(v2.isNaN, isTrue);
      expect(v3.isNaN, isTrue);
    });
  });

  group('NList backing getter tests', () {
    test('backing returns Int32List for int column', () {
      final col = NList([1, 2, 3]);
      expect(col.backing, isA<Int32List>());
    });

    test('backing returns Float64List for double column', () {
      final col = NList([1.0, 2.0, 3.0]);
      expect(col.backing, isA<Float64List>());
    });

    test('backing returns Float32List for Float32List column', () {
      final col = NList([1.0, 2.0, 3.0], type: Float32List);
      expect(col.backing, isA<Float32List>());
    });

    test('backing returns List for Object column', () {
      final col = NList(['a', 'b', 'c']);
      expect(col.backing, isA<List>());
    });

    test('backing reflects promotion from int to double', () {
      final col = NList([1, 2, 3]);
      expect(col.backing, isA<Int32List>());
      col.add(1.5);
      expect(col.backing, isA<Float64List>());
    });

    test('backing reflects promotion from numeric to Object', () {
      final col = NList([1.0, 2.0]);
      expect(col.backing, isA<Float64List>());
      col.add('x');
      expect(col.backing, isA<List>());
    });
  });

  // All tests below this line depend on the internal `allocated` getter.
  // You can comment out this entire group (and the getter) together if you
  // want to hide capacity details from the public API.
  // group('NList capacity and growth tests', () {
  //   test('Empty typed int column grows from zero capacity on first add', () {
  //     final col = NList(<int>[], type: int);
  //     expect(col.length, equals(0));
  //     expect(col.dtype, equals(Int32List));
  //     expect(col.allocated, equals(0));
  //     col.add(1);
  //     expect(col.length, equals(1));
  //     expect(col[0], equals(1));
  //     expect(col.allocated, greaterThan(0));
  //     expect(col.allocated, greaterThanOrEqualTo(col.length));
  //   });
  //   test('Small int column grows when length reaches capacity and preserves values', () {
  //     final col = NList(<int>[1, 2, 3]);
  //     final initialAllocated = col.allocated;
  //     expect(initialAllocated, greaterThanOrEqualTo(col.length));
  //     final remaining = initialAllocated - col.length;
  //     for (var i = 0; i < remaining; i++) { col.add(100 + i); }
  //     expect(col.length, equals(initialAllocated));
  //     expect(col.allocated, equals(initialAllocated));
  //     final oldAllocated = col.allocated;
  //     col.add(999);
  //     expect(col.length, equals(oldAllocated + 1));
  //     expect(col.allocated, greaterThan(oldAllocated));
  //     final values = [for (var i = 0; i < col.length; i++) col[i]];
  //     expect(values.sublist(0, 3), equals([1, 2, 3]));
  //     expect(values.last, equals(999));
  //   });
  //   test('List grows when length reaches capacity and preserves values', () {
  //     final col = NList([1, 2.5]);
  //     expect(col.dtype, equals(Float64List));
  //     final initialAllocated = col.allocated;
  //     expect(initialAllocated, greaterThanOrEqualTo(col.length));
  //     final remaining = initialAllocated - col.length;
  //     for (var i = 0; i < remaining; i++) { col.add(200.0 + i); }
  //     expect(col.length, equals(initialAllocated));
  //     expect(col.allocated, equals(initialAllocated));
  //     expect(col.dtype, equals(Float64List));
  //     final oldAllocated = col.allocated;
  //     col.add(999.9);
  //     expect(col.length, equals(oldAllocated + 1));
  //     expect(col.allocated, greaterThan(oldAllocated));
  //     expect(col.dtype, equals(Float64List));
  //     final values = [for (var i = 0; i < col.length; i++) col[i]];
  //     expect(values[0], equals(1.0));
  //     expect(values[1], equals(2.5));
  //     expect(values.last, equals(999.9));
  //   });
  //   test('addAll on int column can trigger growth and preserves values', () {
  //     final col = NList(<int>[1, 2, 3]);
  //     final initialAllocated = col.allocated;
  //     final toAdd = <int>[];
  //     for (var i = 0; i < initialAllocated * 2; i++) { toAdd.add(1000 + i); }
  //     col.addAll(toAdd);
  //     expect(col.dtype, equals(Int32List));
  //     expect(col.length, equals(3 + toAdd.length));
  //     expect(col.allocated, greaterThan(initialAllocated));
  //     final values = [for (var i = 0; i < col.length; i++) col[i]];
  //     expect(values.sublist(0, 3), equals([1, 2, 3]));
  //     expect(values.sublist(3), equals(toAdd));
  //   });
  // });
}
