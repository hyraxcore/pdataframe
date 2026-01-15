import 'package:test/test.dart';
import 'package:pdataframe/src/nlist.dart';
import 'dart:typed_data';

void main() {
  group('NList constructor tests', () {
    test('List<int> infers Int32List storage and int dtype', () {
      final col = NList(<int>[1, 2, 3]);
      expect(col.length, equals(3));
      expect(col.dtype, equals(Int32List));
      expect([for (var i = 0; i < col.length; i++) col[i]], equals([1, 2, 3]),);
      //expect(col.backing, isA<Int32List>());
    });

    test('List<dynamic> of ints still infers Int32List and int dtype', () {
      final it2 = <dynamic>[2, 3, 4];
      final col = NList(it2);
      expect(col.length, equals(3));
      expect(col.dtype, equals(Int32List));
      expect([for (var i = 0; i < col.length; i++) col[i]], equals([2, 3, 4]),);
      //expect(col.backing, isA<Int32List>());
    });

    test('List<String> infers Object-backed column', () {
      final col = NList(<String>['hi', 'hello']);
      expect(col.length, equals(2));
      expect(col.dtype, equals(Object));
      List<Object?> vals = [];
      for (var it = col.iterator; it.moveNext();) {
        vals.add(it.current);
      }
      expect(vals, equals(['hi', 'hello']));
      //expect(col.backing, isA<List>());
    });

    test('Mixed numeric (int + double) infers Float64List and double dtype', () {
      final col = NList([2, 5.3]);
      expect(col.length, equals(2));
      expect(col.dtype, equals(Float64List));
      expect([for (var i = 0; i < col.length; i++) col[i]],equals([2.0, 5.3]),);
      //expect(col.backing, isA<Float64List>());
    });
  });

  group('NList add() tests', () {
    test('add(int) on int column appends without promotion', () {
      final col = NList(<int>[1, 2, 3]);
      col.add(4); col.add(5); col.add(6);
      expect(col.dtype, equals(Int32List));
      expect(col.length, equals(6));
      expect([for (var i = 0; i < col.length; i++) col[i]], equals([1, 2, 3, 4, 5, 6]),);
      //expect(col.backing, isA<Int32List>());
    });

    test('add(double) promotes int column to double', () {
      final col = NList(<int>[1, 2, 3]);
      col.add(6.2);
      expect(col.dtype, equals(Float64List));
      expect(col.length, equals(4));
      expect([for (var i = 0; i < col.length; i++) col[i]],equals([1.0, 2.0, 3.0, 6.2]),);
      //expect(col.backing, isA<Float64List>());
    });

    test('add(null) on numeric column converts to double.nan', () {
      final col = NList(<int>[1, 2, 3]);
      col.add(null);
      expect(col.dtype, equals(Float64List));
      expect(col.length, equals(4));
      final last = col[col.length - 1] as double;
      expect(last.isNaN, isTrue);
      //expect(col.backing, isA<Float64List>());
    });

    test('add(non-numeric) on double column promotes to Object', () {
      final col = NList([1, 2.5]); // mixed: double
      col.add('hi');
      expect(col.dtype, equals(Object));
      //expect(col.backing, isA<List>());
      expect(col.length, equals(3));
      expect(col[0], equals(1.0));
      expect(col[1], equals(2.5));
      expect(col[2], equals('hi'));
    });
  });

  group('NList addAll() tests', () {
    test('addAll(ints) on int column appends values', () {
      final col = NList([1, 2, 3]);
      final toAdd = [5, 6, 7, 8, 9, 10, 11, 12, 13, 14];
      col.addAll(toAdd);
      expect(col.dtype, equals(Int32List));
      //expect(col.backing, isA<Int32List>());
      expect(col.length, equals(3 + toAdd.length));
      expect([for (var i = 0; i < col.length; i++) col[i]],equals([1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]),);
    });

    test('addAll with null and non-numeric on double column promotes to Object', () {
      final col = NList([1, 2.0]); // → double
      col.addAll([3.5, null, 'x']);
      expect(col.length, equals(5));
      expect(col.dtype, equals(Object));
      //expect(col.backing, isA<List>());
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
      //expect(col.backing, isA<Int32List>());
      expect(
        [for (var i = 0; i < col.length; i++) col[i]],
        equals([1, 2, 100]),
      );
    });

    test('[]= with double promotes int column to double', () {
      final col = NList(<int>[1, 2, 3]);
      col[1] = 2.5;
      expect(col.dtype, equals(Float64List));
      //expect(col.backing, isA<Float64List>());
      expect(
        [for (var i = 0; i < col.length; i++) col[i]],
        equals([1.0, 2.5, 3.0]),
      );
    });

    test('[]= with null on int column stores double.nan and promotes to double', () {
      final col = NList(<int>[1, 2, 3]);
      col[1] = null;
      expect(col.dtype, equals(Float64List));
      //expect(col.backing, isA<Float64List>());
      final v = col[1] as double;
      expect(v.isNaN, isTrue);
    });

    test('[]= with non-numeric promotes numeric column to Object', () {
      final col = NList(<int>[1, 2, 3]);
      col[1] = 'hi';
      expect(col.dtype, equals(Object));
      //expect(col.backing, isA<List>());
      expect(
        [for (var i = 0; i < col.length; i++) col[i]],
        equals([1, 'hi', 3]),
      );
    });
  });

  group('NList iterator and toString tests', () {
    test('iterator yields only logical elements', () {
      final col = NList(<int>[1, 2, 3]);
      var it = col.iterator;
      List vals = [];
      while (it.moveNext()){vals.add(it.current);}
      expect(vals, equals([1, 2, 3])); // Iterator should yield exactly the logical elements [1, 2, 3].
    });

    test('toString() prints only logical elements', () {
      final col = NList(<int>[1, 2, 3]);
      expect(col.toString(), equals('[1, 2, 3]')); // Internal capacity can exceed length; string must show only [1, 2, 3].
    });
  });
  group('.length assignment test', () {
    test('new length is lesser', () {
      final ltest = NList(<int>[1, 2, 3]);
      ltest.length = 2;
      expect(ltest, equals([1,2]));
      expect(ltest.length, equals(2));
    });
    test('new length is greater', () {
      final ltest = NList(<int>[1, 2]);
      ltest.length = 4;
      expect(ltest, equals([1.0, 2.0, isNaN, isNaN]));
      expect(ltest.length, equals(4));
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
  //     // Fill up to capacity without triggering growth.
  //     final remaining = initialAllocated - col.length;
  //     for (var i = 0; i < remaining; i++) {
  //       col.add(100 + i);
  //     }
  //     expect(col.length, equals(initialAllocated));
  //     expect(col.allocated, equals(initialAllocated));

  //     // This add should trigger a growth.
  //     final oldAllocated = col.allocated;
  //     col.add(999);

  //     expect(col.length, equals(oldAllocated + 1));
  //     expect(col.allocated, greaterThan(oldAllocated));

  //     // Check that original + filler + last value are all preserved.
  //     final values = [for (var i = 0; i < col.length; i++) col[i]];
  //     expect(values.sublist(0, 3), equals([1, 2, 3]));
  //     expect(values.last, equals(999));
  //   });

  //   test('List grows when length reaches capacity and preserves values', () {
  //     final col = NList([1, 2.5]); // inferred double

  //     expect(col.dtype, equals(Float64List));

  //     final initialAllocated = col.allocated;
  //     expect(initialAllocated, greaterThanOrEqualTo(col.length));

  //     // Fill up to capacity without triggering growth.
  //     final remaining = initialAllocated - col.length;
  //     for (var i = 0; i < remaining; i++) {
  //       col.add(200.0 + i);
  //     }

  //     expect(col.length, equals(initialAllocated));
  //     expect(col.allocated, equals(initialAllocated));
  //     expect(col.dtype, equals(Float64List));

  //     // This add should trigger a growth for the Float64List.
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
  //     // Construct a list large enough to exceed current capacity.
  //     final toAdd = <int>[];
  //     for (var i = 0; i < initialAllocated * 2; i++) {
  //       toAdd.add(1000 + i);
  //     }

  //     col.addAll(toAdd);
  //     expect(col.dtype, equals(Int32List));
  //     expect(col.length, equals(3 + toAdd.length));
  //     expect(col.allocated, greaterThan(initialAllocated));
  //     final values = [for (var i = 0; i < col.length; i++) col[i]];
  //     expect(values.sublist(0, 3), equals([1, 2, 3]));
  //     expect(values.sublist(3), equals(toAdd));
  //   });
  //});
}