import 'package:pdataframe/pdataframe.dart';
import 'package:test/test.dart';

void main() {
  group('Timestamp tests',(){
    var ts1 = Timestamp("1999-01-01");
    test('Timestamp constructor parsing', () {
      expect(Timestamp("1999-02-09").toString(), equals('1999-02-09 00:00:00.000')); // YYYY-MM-DD (ISO 8601) 
      expect(Timestamp("1999/02/09").toString(), equals('1999-02-09 00:00:00.000')); // YYYY/MM/DD
      expect(Timestamp("09-02-1999",).toString(), equals('1999-09-02 00:00:00.000')); // Cannot tell if DD-MM or MM-DD, default to MM-DD
      expect(Timestamp("09-02-1999", dayFirst: true).toString(), equals('1999-02-09 00:00:00.000')); // DD-MM-YYYY using dayFirst
      expect(Timestamp("02-09-1999").toString(), equals('1999-02-09 00:00:00.000')); // MM-DD-YYYY (if unambiguous)
      expect(Timestamp("09 Feb 1999").toString(), equals('1999-02-09 00:00:00.000')); // DD Mon YYYY
      expect(Timestamp("February 9, 1999").toString(), equals('1999-02-09 00:00:00.000')); // Month D, YTTT
      expect(Timestamp("Feb-09-99").toString(), equals('1999-02-09 00:00:00.000')); // Mon-DD-YY 
      expect(Timestamp("Feb 9, 1999, 14:30").toString(), equals('1999-02-09 14:30:00.000')); // Mon D, YYYY, HH:MM
      expect(Timestamp("19990209").toString(), equals('1999-02-09 00:00:00.000')); // Compact YYYYMMDD     
      expect(Timestamp("09.02.1999").toString(), equals('1999-02-09 00:00:00.000')); // MM.DD.YYYY
      expect(Timestamp("1999-02-09 14:30:45").toString(), equals('1999-02-09 14:30:45.000')); // YYYY-MM-DD HH:MM:SS
      expect(Timestamp("1999/02/09 2:30 PM").toString(), equals('1999-02-09 02:30:00.000')); // 12-hour format
      expect(Timestamp("1999/02/09 2:30aM").toString(), equals('1999-02-09 02:30:00.000')); // 12-hour format, space/caps adjusted
      expect(Timestamp("1999-02-09T14:30").toString(), equals('1999-02-09 14:30:00.000')); // ISO 8601 with 'T' 
      expect(Timestamp("1999-02-09T14:30Z").toString(), equals('1999-02-09 14:30:00.000Z')); // UTC time with 'Z'
      expect(Timestamp(916722000, unit:"s").toString(), equals('1999-01-19 05:00:00.000Z')); //Unix timestamp in Seconds (UTC/Z default)
      expect(Timestamp(916704000000, unit: "ms").toString(), equals('1999-01-19 00:00:00.000Z')); // Unix timestamp in Milliseconds
    });
    test('values constructor', () {
      expect(Timestamp.values( // Check all parameters
        year:1999, month: 12, day: 20, hour: 3, minute: 32, second: 14, millisecond: 123,microsecond: 323).toString(), 
        equals('1999-12-20 03:32:14.123323')); 
      expect(() => Timestamp.values(year: 1999, month: 12), throwsA(isA<ArgumentError>())); // Requires YMD else throw error
    });
    test('Operators', () {
      Timedelta td1 = Timedelta(days: 1, hours: 23);
      Timestamp ts2 = Timestamp(1999-02-01);
      expect((ts1-td1).toString(), equals('1998-12-30 01:00:00.000'));
      expect((ts1+td1).toString(), equals('1999-01-02 23:00:00.000'));
      expect((ts1>ts2), equals(false));
      expect((ts1<ts2), equals(true));
    });
    test('Date and calendar members', () {    
      Timestamp ts2 = Timestamp('2024-12-01', nanosecond: 234);
      Timestamp ts3 = Timestamp('2024-12-31');
      List members = [ts2.toString(), ts2.nanosecond, ts2.dayofweek, ts2.day_name(), ts2.isLeapYear, ts2.quarter, ts2.is_month_start];
      expect(members, equals(['2024-12-01 00:00:00.000000234',  234, 7, 'Sunday', true, 4, true]));
      expect(ts3.is_month_end, equals(true));
    });
    test('String format: .strftime()', () {
      expect(ts1.strftime('%d-%b-%Y'), equals ('01-Jan-1999'));
    });
    test('Period conversion: to_period()', () {
      Timestamp ts = Timestamp(DateTime(2023, 12, 17, 14, 45, 30));
      List fiscalQuarter = [ts.to_period("Q"), ts.to_period("q-jan"), ts.to_period("Q-JUN"), ts.to_period("Q-OCT"),ts.to_period("Q-DEC")];
      expect(fiscalQuarter, equals(['2023Q4','2024Q4','2024Q2','2024Q1','2023Q4'])); // Check that for Q-JAN, it rolls over to the next year.
      List fiscalYear = [ts.to_period("A"), ts.to_period("A-jan"), ts.to_period("A-APR"), ts.to_period("A-DEC")];
      expect(fiscalYear, equals(['2023','2024', '2024', '2023']));  // Check edge cases; Jan and Dec.
      List dateTruncation = [ts.to_period("M"), ts.to_period("D"), ts.to_period("H"), ts.to_period("T"), ts.to_period("S")];
      expect(dateTruncation, equals(['2023-12', '2023-12-17', '2023-12-17 14:00', '2023-12-17 14:45', '2023-12-17 14:45:30']));
    }); 
    test('Rounding methods: ceil()/floor()/round()', () {
      var ts2 = Timestamp('1999-02-06 08:46:39.500500', nanosecond: 499);
      final ceils = [ts2.ceil('D'),ts2.ceil('H'),ts2.ceil('T'),ts2.ceil('S'),ts2.ceil('L'),ts2.ceil('U'),ts2.ceil('N'),];
      final floors = [ts2.floor('D'),ts2.floor('H'),ts2.floor('T'),ts2.floor('S'),ts2.floor('L'),ts2.floor('U'),ts2.floor('N'),];
      final rounds = [ts2.round('D'),ts2.round('H'),ts2.round('T'),ts2.round('S'),ts2.round('L'),ts2.round('U'),ts2.round('N'),];

      expect(floors, 
        equals(['1999-02-06', '1999-02-06 08', '1999-02-06 08:46', '1999-02-06 08:46:39', 
        '1999-02-06 08:46:39.500', '1999-02-06 08:46:39.500500', '1999-02-06 08:46:39.500500499'])
      );
      expect(ceils, 
        equals(['1999-02-06', '1999-02-06 09', '1999-02-06 08:47', '1999-02-06 08:46:40', '1999-02-06 08:46:39.501', '1999-02-06 08:46:39.500501', '1999-02-06 08:46:39.500500499'])
      );
      expect(rounds, 
        equals(['1999-02-06', '1999-02-06 09', '1999-02-06 08:47', '1999-02-06 08:46:40', '1999-02-06 08:46:39.501', '1999-02-06 08:46:39.500500', '1999-02-06 08:46:39.500500499'])
      );
    }); 
  });
}