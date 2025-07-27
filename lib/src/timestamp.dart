class Timestamp {
  // Internally uses [DateTime].
  late DateTime _dateTime;

  // * Constructors *

  /// Create a `Timestamp` from a general input.
  ///
  /// Example:
  /// ```dart
  /// Timestamp('1999-01-21 14:30');           // from string
  /// Timestamp(916722000, unit: 's');         // from Unix timestamp (seconds)
  /// Timestamp(DateTime.utc(1999, 3, 25));    // from DateTime
  /// ```
  /// Supports:
  /// - Date-time formatted `String` values (flexible parsing).
  /// - Unix timestamps (requires the parameter `unit` to specify time scale).
  /// - `DateTime` objects directly.
  Timestamp(final input, {int nanosecond = 0, String unit = '', bool dayFirst = false}) {
    // Operation:
    // If `input` is a `DateTime`, store it directly.
    // If `input` is a `String`, try flexible parser `tryParseFlexible`.
    // Otherwise, use `DateTime.now()`.
    if (nanosecond < 0 || nanosecond >= 1000) {
      throw ArgumentError('Nanoseconds must be between 0 and 999.');
    }
    // 'unit' argument format should be as Unix timestamp
    if(unit != ''){
      _dateTime = _parseUnixTimestamp(input, unit);
      return;
    }
    if (input is DateTime) {
      _dateTime = input;
    } else if (input is String) {
      try {
        _dateTime = _tryParseFlexible(input, dayFirst: dayFirst);
      } catch (e) {
        _dateTime = DateTime.now();
      }
    } else {
      _dateTime = DateTime.now();
    }
    _nanosecond = nanosecond;
  }

  /// Creates a `Timestamp` using individual date and time components.
  ///
  /// Example:
  /// ```dart
  /// Timestamp.values(year: 1999, month: 1, day: 21);  
  /// ```
  /// Note: requires `year`, `month`, and `day`.
  /// - Parameters: `year`, `month`, `day`, `hour`, `minute`, `second`, `microsecond`, `nanosecond`.
  Timestamp.values({
    int? year,
    int? month,
    int? day,
    int hour = 0,
    int minute = 0,
    int second = 0,
    int millisecond = 0,
    int microsecond = 0,
    int nanosecond = 0,
  }) {
    if (year == null && month == null && day == null) {
      // If none are specified, just use "now"
      _dateTime = DateTime.now();
      _nanosecond = nanosecond;
    } else {
      // If any of year, month, day are specified, all must be specified
      if (year == null || month == null || day == null) {
        throw ArgumentError(
          'Year, month, and day must all be provided if any is specified.',
        );
      }
      _dateTime = DateTime(year,month,day,hour,minute,second,millisecond,microsecond);
      _nanosecond = nanosecond;
    }
  }
  // Nanosecond support
    // Extra nanoseconds (0-999).
    int _nanosecond = 0;
    // Setter for nanoseconds
    set nanosecond(int value) {
      if (value < 0 || value >= 1000) {
        throw ArgumentError('Nanoseconds must be between 0 and 999.');
      }
      _nanosecond = value;
    }
    Timestamp addNanoseconds(int nanos) {
      final totalNanos = _dateTime.microsecondsSinceEpoch * 1000 + _nanosecond + nanos;
      final newMicroseconds = totalNanos ~/ 1000;
      final newNanos = totalNanos % 1000;
      return Timestamp._(DateTime.fromMicrosecondsSinceEpoch(newMicroseconds),nanosecond: newNanos,);
    }
    Timestamp subtractNanoseconds(int nanos) {
      return addNanoseconds(-nanos);
    }

    // Private constructor for named factory.
    Timestamp._(this._dateTime, {int nanosecond = 0}) {
      _nanosecond = nanosecond;
    }

  /// Return a `Timestamp` with the current date-time.
  ///
  /// Example:
  /// ```dart
  /// Timestamp.now();                    // current UTC timestamp
  /// Timestamp.now(tz: 'America/New_York');    // current time in New York, America
  /// ```
  /// 
  /// If [tz] is provided, the DateTime is adjusted to the specified time zone.
  /// Supported time zone abbreviations and IANA time zone names:
  /// 'pst', 'est', 'cet', 'ist', 'MST', 'GMT', 'UTC', 
  /// 'America/New_York', 'America/Chicago', 'America/Denver', 'America/Los_Angeles', 
  /// 'America/Sao_Paulo', 'America/Buenos_Aires', 'Europe/London', 'Europe/Paris', 'Europe/Moscow', 
  /// 'Africa/Cairo', 'Africa/Johannesburg', 'Asia/Dubai', 'Asia/Kolkata', 'Asia/Shanghai', 
  /// 'Asia/Tokyo', 'Australia/Sydney', 'Pacific/Auckland', 'Pacific/Honolulu'
  factory Timestamp.now({int nanosecond = 0, String tz = 'utc'}) {
    DateTime now = DateTime.now();
    tz = tz.toLowerCase();
    if (tz == 'utc') {
      now = now.toUtc();
    } else {
      Map<String, Duration> tzOffsets = {
        'pst': Duration(hours: -8),
        'est': Duration(hours: -5),
        'cet': Duration(hours: 1),
        'ist': Duration(hours: 5, minutes: 30),
        'MST': Duration(hours: -7), 
        'GMT': Duration(hours: 0), 
        'UTC': Duration(hours: 0),
        'America/New_York': Duration(hours: -5),
        'America/Chicago': Duration(hours: -6),
        'America/Denver': Duration(hours: -7),
        'America/Los_Angeles': Duration(hours: -8),
        'America/Sao_Paulo': Duration(hours: -3),
        'America/Buenos_Aires': Duration(hours: -3),
        'Europe/London': Duration(hours: 0),
        'Europe/Paris': Duration(hours: 1),
        'Europe/Moscow': Duration(hours: 3),
        'Africa/Cairo': Duration(hours: 2),
        'Africa/Johannesburg': Duration(hours: 2),
        'Asia/Dubai': Duration(hours: 4),
        'Asia/Kolkata': Duration(hours: 5, minutes: 30),
        'Asia/Shanghai': Duration(hours: 8),
        'Asia/Tokyo': Duration(hours: 9),
        'Australia/Sydney': Duration(hours: 11),
        'Pacific/Auckland': Duration(hours: 13),
        'Pacific/Honolulu': Duration(hours: -10),
      };
      Duration? customOffset = tzOffsets[tz];
      if (customOffset != null) {
        now = now.toUtc().add(customOffset);
      } else {
        throw ArgumentError('Unsupported timezone: $tz');
      }
    }
    return Timestamp._(now, nanosecond: nanosecond);
  }

  // Constructor helper function that builds a DateTime object from various date and time components
  static DateTime _buildDateTime(
      int year,
      int? month,
      int? day,
      String? hour,
      String? min,
      String? sec,
      String? ampm
    ){
    var hh = (hour != null) ? int.parse(hour) : 0;
    var mm = (min  != null) ? int.parse(min)  : 0;
    var ss = (sec  != null) ? int.parse(sec)  : 0;
    // Handle AM/PM if present 
    var hr = hh;
    if (ampm != null) {
      final upper = ampm.toUpperCase();
      if (upper == 'PM' && hh < 12) hh += 12;
      if (upper == 'AM' && hh == 12) hh = 0;
    }
    return DateTime(year, month ?? 1, day ?? 1, hr, mm, ss);
  }
  
  // Constructor helper method for parsing String date inputs
  static DateTime _tryParseFlexible(String input, {bool dayFirst = false}) {
    // Trim input of spaces
    final trimmedInput = input.trim();
    // Force day-first if a dot is present.
    final useDayFirst = dayFirst || trimmedInput.contains('.');
    // Try built-in ISO parser first if dayFirst is false
    if (dayFirst == false) {
      try {
        return DateTime.parse(trimmedInput);
      } catch (_) {
      }
    }
    // Check if input contains letters
    final hasLetter = RegExp(r'[A-Za-z]').hasMatch(trimmedInput);
    // NUMBERS ONLY BRANCH
    if (!hasLetter) {
      final groups = trimmedInput
          .split(RegExp(r'\D+'))
          .where((s) => s.isNotEmpty)
          .toList();
      if (groups.isEmpty) {
        throw ArgumentError("Invalid date format: '$input'");
      }
      // Single-group: try fixed-length slicing
      if (groups.length == 1) {
        final numStr = groups[0];
        final len = numStr.length;
        if (len == 8) {
          // YYYYMMDD
          final year = int.parse(numStr.substring(0, 4));
          final month = int.parse(numStr.substring(4, 6));
          final day = int.parse(numStr.substring(6, 8));
          return _buildDateTime(year, month, day, null, null, null, null);
        } else if (len == 9 || len == 10) {
          // Unix timestamp in seconds
          final secs = int.parse(numStr);
          return DateTime.fromMillisecondsSinceEpoch(secs * 1000);
        } else if (len == 13) {
          // Unix timestamp in milliseconds.
          final ms = int.parse(numStr);
          return DateTime.fromMillisecondsSinceEpoch(ms);
        } else if (len == 12) {
          // YYYYMMDDHHMM
          final year = int.parse(numStr.substring(0, 4));
          final month = int.parse(numStr.substring(4, 6));
          final day = int.parse(numStr.substring(6, 8));
          final hourStr = numStr.substring(8, 10);
          final minStr = numStr.substring(10, 12);
          return _buildDateTime(year, month, day, hourStr, minStr, null, null);
        } else {
          // Fallback: let DateTime.parse try it
          return DateTime.parse(trimmedInput);
        }
      }
      // Multiple groups numeric branch
      int year = 0;
      int? month, day;
      int hour = 0, minute = 0, second = 0;
      int? yearIndex;
      for (int i = 0; i < groups.length; i++) {
        if (groups[i].length == 4) {
          yearIndex = i;
          break;
        }
      }
      if (yearIndex != null) {
        year = int.parse(groups[yearIndex]);
        final parts = List<String>.from(groups)..removeAt(yearIndex);
        if (parts.length >= 2) {
          final firstVal = int.parse(parts[0]);
          final secondVal = int.parse(parts[1]);
          if (useDayFirst || firstVal > 12) {
            day = firstVal;
            month = secondVal;
          } else {
            month = firstVal;
            day = secondVal;
          }
        } else if (parts.length == 1) {
          month = int.parse(parts[0]);
          day = 1;
        }
        if (parts.length >= 3) hour = int.parse(parts[2]);
        if (parts.length >= 4) minute = int.parse(parts[3]);
        if (parts.length >= 5) second = int.parse(parts[4]);
      } else {
        // No 4-digit year found; assume the last group is the year
        final last = groups.last;
        year = int.parse(last);
        if (last.length == 2) {
          year = (year < 30) ? 2000 + year : 1900 + year;
        }
        final parts = List<String>.from(groups)..removeLast();
        if (parts.length >= 2) {
          final firstVal = int.parse(parts[0]);
          final secondVal = int.parse(parts[1]);
          if (useDayFirst || firstVal > 12) {
            day = firstVal;
            month = secondVal;
          } else {
            month = firstVal;
            day = secondVal;
          }
        } else if (parts.length == 1) {
          month = int.parse(parts[0]);
          day = 1;
        }
        if (parts.length >= 3) hour = int.parse(parts[2]);
        if (parts.length >= 4) minute = int.parse(parts[3]);
        if (parts.length >= 5) second = int.parse(parts[4]);
      }
      return _buildDateTime(
        year,
        month,
        day,
        hour.toString(),
        minute.toString(),
        second.toString(),
        null,
      );
    }
    // LETTERS BRANCH
    // Only do the AM/PM “space insert” if we actually have letters
    final normalizedInput = trimmedInput.replaceAllMapped(
      RegExp(r'(\d)([APap][Mm])'),
      (match) => '${match.group(1)} ${match.group(2)}',
    );
    final text = normalizedInput.trim();
    // Split on slashes, hyphens, commas, or whitespace
    List<String> tokens = text
        .split(RegExp(r'[\/\-,\s]+'))
        .where((s) => s.isNotEmpty)
        .toList();
    int? year;
    int? month;
    int? day;
    int hour = 0, minute = 0, second = 0;
    String? ampm;
    // Remove AM/PM
    tokens.removeWhere((token) {
      final up = token.toUpperCase();
      if (up == 'AM' || up == 'PM') {
        ampm = up;
        return true;
      }
      return false;
    });
    // Month name mapping
    final Map<String, int> monthNames = {
      'jan': 1, 'january': 1,
      'feb': 2, 'february': 2,
      'mar': 3, 'march': 3,
      'apr': 4, 'april': 4,
      'may': 5,
      'jun': 6, 'june': 6,
      'jul': 7, 'july': 7,
      'aug': 8, 'august': 8,
      'sep': 9, 'sept': 9, 'september': 9,
      'oct': 10, 'october': 10,
      'nov': 11, 'november': 11,
      'dec': 12, 'december': 12,
    };
    // Identify month
    for (var token in List<String>.from(tokens)) {
      final lower = token.toLowerCase();
      if (monthNames.containsKey(lower)) {
        month = monthNames[lower];
        tokens.remove(token);
        break;
      }
    }
    // Separate time (contains ':') from date.
    List<String> timeTokens = [];
    List<String> dateTokens = [];
    for (var token in tokens) {
      if (token.contains(':')) {
        timeTokens.addAll(token.split(':'));
      } else {
        dateTokens.add(token);
      }
    }
    // Look for a 4-digit year in the date tokens
    for (var token in List<String>.from(dateTokens)) {
      if (token.length == 4) {
        year = int.parse(token);
        dateTokens.remove(token);
        break;
      }
    }
    // If no 4-digit year, assume the last token is the year
    if (year == null && dateTokens.isNotEmpty) {
      final token = dateTokens.removeLast();
      year = int.parse(token);
      if (token.length == 2) {
        year = (year < 30) ? 2000 + year : 1900 + year;
      }
    }
    // With remaining date tokens, assign month and day
    if (month == null && dateTokens.isNotEmpty) {
      if (dateTokens.length >= 2) {
        final firstVal = int.parse(dateTokens[0]);
        final secondVal = int.parse(dateTokens[1]);
        if (useDayFirst || firstVal > 12) {
          day = firstVal;
          month = secondVal;
        } else {
          month = firstVal;
          day = secondVal;
        }
      } else if (dateTokens.length == 1) {
        month = int.parse(dateTokens[0]);
      }
    } else if (month != null && dateTokens.isNotEmpty) {
      day = int.parse(dateTokens[0]);
    }
    // Process time tokens
    if (timeTokens.isNotEmpty) {
      if (timeTokens.length > 0) hour = int.parse(timeTokens[0]);
      if (timeTokens.length > 1) minute = int.parse(timeTokens[1]);
      if (timeTokens.length > 2) second = int.parse(timeTokens[2]);
    }
    return _buildDateTime(
      year ?? 1970,
      month ?? 1,
      day ?? 1,
      hour.toString(),
      minute.toString(),
      second.toString(),
      ampm,
    );
  }

  // Helper method that converts a Unix timestamp to a DateTime object
  // Default unit is seconds ('s'). Use 'unit' to specify milliseconds ('ms'), microseconds ('us'), or nanoseconds ('ns').
  DateTime _parseUnixTimestamp(int value, String? unit, {String tz = 'UTC'}) {
    bool tzUtc = true;
    tz.toUpperCase();
    if(tz != 'UTC'){
      tzUtc = false;
    }
    final u = unit?.toLowerCase();
    if (u == null || u == 's') return DateTime.fromMillisecondsSinceEpoch(value * 1000, isUtc: tzUtc);
    if (u == 'ms') return DateTime.fromMillisecondsSinceEpoch(value,isUtc: tzUtc);
    if (u == 'us') return DateTime.fromMillisecondsSinceEpoch(value ~/ 1000,isUtc: tzUtc);
    if (u == 'ns') return DateTime.fromMillisecondsSinceEpoch(value ~/ 1000000,isUtc: tzUtc);
    throw ArgumentError("Invalid value for the 'unit' parameter. Use only 's', 'ms', 'us', or 'ns'.");
  }

  // * Delegated methods from DateTime
  
  /// Returns the time difference between this and the input datetime.
  difference(var input) => _dateTime.difference(input);

  // * Date and time members
  
  /// Returns the year.
  get year => _dateTime.year;
  /// Returns the month.
  get month => _dateTime.month;
  /// Returns the week number.
  int get week {
    final DateTime firstDayOfYear = DateTime(year, 1, 1);
    final int daysSinceStartOfYear = difference(firstDayOfYear).inDays + 1; // +1 for 1-based day
    final int firstWeekday = firstDayOfYear.weekday; // Monday = 1, Sunday = 7
    // Calculate ISO week number
    final int adjustedDays = daysSinceStartOfYear + (firstWeekday - 1); // Align with ISO weeks
    return (adjustedDays / 7).ceil();
  }
  /// Returns microseconds since the Unix epoch.
  int get microsecondsSinceEpoch => _dateTime.microsecondsSinceEpoch;
  /// Returns the day of the month.
  int get day => _dateTime.day;
  /// Returns the hour.
  int get hour => _dateTime.hour;
  /// Returns the minute.
  int get minute => _dateTime.minute;
  /// Returns the second.
  int get second => _dateTime.second;
  /// Returns the millisecond.
  int get millisecond => _dateTime.millisecond;
  /// Returns the microsecond.
  int get microsecond => _dateTime.microsecond;
  /// Returns the nanosecond.
  int get nanosecond => _nanosecond;
  
  // * Calendar-based members

  /// Returns the weekday number.
  int get dayofweek =>  _dateTime.weekday;  
  /// Returns the name of the weekday.
  String day_name() { 
      List<String> dayNames = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday',];
      return dayNames[_dateTime.weekday-1];
  }
  /// Returns the day of the year.

  int get dayofyear {
    final startOfYear = DateTime(_dateTime.year, 1, 1);
    return _dateTime.difference(startOfYear).inDays + 1;
  }
  /// Returns the quarter of the year (1–4).
  int get quarter => ((_dateTime.month - 1) ~/ 3) + 1;
  /// Returns true if the year is a leap year.
  bool get isLeapYear{
    var year = _dateTime.year;
    return (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
  }
  /// Returns true if this date is the first day of the month.
  bool get is_month_start => _dateTime.day == 1;
  /// Returns true if this date is the last day of the month.
  bool get is_month_end => _dateTime.add(Duration(days: 1)).month != _dateTime.month;

  /// Returns a string representation of the Timestamp.
  @override
  String toString() { 
    if(_nanosecond > 0){
      if(microsecond == 0){
        return _dateTime.toString()+'000${_nanosecond.toString()}';
      } else {
        return _dateTime.toString()+'${_nanosecond.toString()}';
      }
    } else{
      return _dateTime.toString();
    }
  }

  // * Operator Overloads

  Timestamp operator -(Timedelta other) {
    final totalNanosThis = _dateTime.microsecondsSinceEpoch * 1000 + _nanosecond;
    final totalNanosOther = other.inMicroseconds * 1000 + other.nanoseconds;
    final diffNanos = totalNanosThis - totalNanosOther;

    final newMicroseconds = diffNanos ~/ 1000;
    final newNanoseconds = diffNanos % 1000;

    return Timestamp._(
      DateTime.fromMicrosecondsSinceEpoch(newMicroseconds),
      nanosecond: newNanoseconds,
    );
  }

  Timestamp operator +(Timedelta other) {
    final totalNanosThis = _dateTime.microsecondsSinceEpoch * 1000 + _nanosecond;
    final totalNanosOther = other.inMicroseconds * 1000 + other.nanoseconds;
    final sumNanos = totalNanosThis + totalNanosOther;

    final newMicroseconds = sumNanos ~/ 1000;
    final newNanoseconds = sumNanos % 1000;

    return Timestamp._(
      DateTime.fromMicrosecondsSinceEpoch(newMicroseconds),
      nanosecond: newNanoseconds,
    );
  }
  bool operator >(Timestamp other) {
    if (_dateTime.isAfter(other._dateTime)) return true;
    if (_dateTime.isAtSameMomentAs(other._dateTime)) {
      return _nanosecond > other._nanosecond;
    }
    return false;
  }
  bool operator <(Timestamp other) {
    if (_dateTime.isBefore(other._dateTime)) return true;
    if (_dateTime.isAtSameMomentAs(other._dateTime)) {
      return _nanosecond < other._nanosecond;
    }
    return false;
  }

// * Conversions
  
  /// Returns the ISO 8601 string representation of the datetime.
  String isoformat(){
    return _dateTime.toIso8601String();
  }
  /// Returns the local time as a string.
  String toLocal(){
    return _dateTime.toLocal().toString();
  }
  /// Returns the Timestamp as DateTime object.
  DateTime to_datetime(){
    return _dateTime;
  }
  /// Returns the time portion as a string in HH:MM:SS format.
  String time(){
    final timeString = '${_dateTime.hour}:${_dateTime.minute}:${_dateTime.second}';
    return timeString;
  }
  /// Returns the Unix timestamp in seconds.
  double timestamp(){
    double unixTimeInSeconds = _dateTime.millisecondsSinceEpoch / 1000;
    return unixTimeInSeconds;
  }

  /// Converts the `Timestamp` to a period-based representation based on the specified frequency alias.
  ///
  /// Frequency Aliases:
  /// - `'A'` (Annual): Truncates to the year, defaulting to year-end in December.
  ///   - Variants: `'A-JAN'` to `'A-DEC'` (year ends in specified month).
  /// - `'Q'` (Quarterly): Truncates to the quarter based on fiscal start month.
  ///   - Variants: `'Q-JAN'` to `'Q-DEC'` (quarterly periods adjusted accordingly).
  /// - `'M'` (Monthly): Truncates to the month.
  /// - `'D'` (Daily): Truncates the date to the day.
  /// - `'H'` (Hourly): Truncates the date to the hour.
  /// - `'T'` (Minute): Truncates the date to the minute.
  /// - `'S'` (Second): Truncates the date to the second.
  to_period(String freq) {
    freq = freq.toUpperCase();
    DateTime dt = _dateTime;
    final int year = dt.year;
    final int month = dt.month;
    final int day = dt.day;
    final int hour = dt.hour;
    final int minute = dt.minute;
    final int second = dt.second;
    final String yearStr = year.toString();
    final String yearPlusStr = (year + 1).toString();
  
    switch (freq) {
      // Annual frequencies:
      case 'A': return yearStr;
      case 'A-JAN': return (month >= 2 ? yearPlusStr : yearStr);
      case 'A-FEB': return (month > 2 ? yearPlusStr : yearStr);
      case 'A-MAR': return (month > 3 ? yearPlusStr : yearStr);
      case 'A-APR': return (month > 4 ? yearPlusStr : yearStr);
      case 'A-MAY': return (month > 5 ? yearPlusStr : yearStr);
      case 'A-JUN': return (month > 6 ? yearPlusStr : yearStr);
      case 'A-JUL': return (month > 7 ? yearPlusStr : yearStr);
      case 'A-AUG': return (month > 8 ? yearPlusStr : yearStr);
      case 'A-SEP': return (month > 9 ? yearPlusStr : yearStr);
      case 'A-OCT': return (month > 10 ? yearPlusStr : yearStr);
      case 'A-NOV': return (month > 11 ? yearPlusStr : yearStr);
      case 'A-DEC': return yearStr;
      // Quarterly frequencys:
      case 'Q': {  // Calendar Quarter
        final int quarter = ((month - 1) ~/ 3) + 1;
        return "$yearStr" "Q" + quarter.toString();
      }
      // Fiscal Quarters 
      case 'Q-JAN': { 
        final int adjYear = (month >= 2) ? (year + 1) : year;
        final int quarter = (((month + 10) % 12) ~/ 3) + 1;
        return "${adjYear}Q$quarter";
      }
      case 'Q-FEB': {
        final int adjYear = (month >= 3) ? (year + 1) : year;
        final int quarter = (((month + 9) % 12) ~/ 3) + 1;
        return "${adjYear}Q$quarter";
      }
      case 'Q-MAR': {
        final int adjYear = (month >= 4) ? (year + 1) : year;
        final int quarter = (((month + 8) % 12) ~/ 3) + 1;
        return "${adjYear}Q$quarter";
      }
      case 'Q-APR': {
        final int adjYear = (month >= 5) ? (year + 1) : year;
        final int quarter = (((month + 7) % 12) ~/ 3) + 1;
        return "${adjYear}Q$quarter";
      }
      case 'Q-MAY': {
        final int adjYear = (month >= 6) ? (year + 1) : year;
        final int quarter = (((month + 6) % 12) ~/ 3) + 1;
        return "${adjYear}Q$quarter";
      }
      case 'Q-JUN': {
        final int adjYear = (month >= 7) ? (year + 1) : year;
        final int quarter = (((month + 5) % 12) ~/ 3) + 1;
        return "${adjYear}Q$quarter";
      }
      case 'Q-JUL': {
        final int adjYear = (month >= 8) ? (year + 1) : year;
        final int quarter = (((month + 4) % 12) ~/ 3) + 1;
        return "${adjYear}Q$quarter";
      }
      case 'Q-AUG': {
        final int adjYear = (month >= 9) ? (year + 1) : year;
        final int quarter = (((month + 3) % 12) ~/ 3) + 1;
        return "${adjYear}Q$quarter";
      }
      case 'Q-SEP': {
        final int adjYear = (month >= 10) ? (year + 1) : year;
        final int quarter = (((month + 2) % 12) ~/ 3) + 1;
        return "${adjYear}Q$quarter";
      }
      case 'Q-OCT': {
        final int adjYear = (month >= 11) ? (year + 1) : year;
        final int quarter = (((month + 1) % 12) ~/ 3) + 1;
        return "${adjYear}Q$quarter";
      }
      case 'Q-NOV': {
        final int adjYear = (month >= 12) ? (year + 1) : year;
        final int quarter = (((month + 0) % 12) ~/ 3) + 1;
        return "${adjYear}Q$quarter";
      }
      case 'Q-DEC': {
        final int quarter = ((month - 1) ~/ 3) + 1;
        return "${year}Q$quarter";
      }
      // Month truncation
      case 'M': {
        final String mo = (month >= 10) ? month.toString() : "0$month";
        return "$yearStr-$mo";
      }
      // Day truncation
      case 'D': {
        final String mo = (month >= 10) ? month.toString() : "0$month";
        final String da = (day >= 10) ? day.toString() : "0$day";
        return "$yearStr-$mo-$da";
      }
      // Hour truncation
      case 'H': {
        final String mo = (month >= 10) ? month.toString() : "0$month";
        final String da = (day >= 10) ? day.toString() : "0$day";
        final String ho = (hour >= 10) ? hour.toString() : "0$hour";
        return "$yearStr-$mo-$da $ho:00";
      }  
      // Minute truncation
      case 'T': {
        final String mo = (month >= 10) ? month.toString() : "0$month";
        final String da = (day >= 10) ? day.toString() : "0$day";
        final String ho = (hour >= 10) ? hour.toString() : "0$hour";
        final String mi = (minute >= 10) ? minute.toString() : "0$minute";
        return "$yearStr-$mo-$da $ho:$mi";
      }
      // Second truncation
      case 'S': {
        final String mo = (month >= 10) ? month.toString() : "0$month";
        final String da = (day >= 10) ? day.toString() : "0$day";
        final String ho = (hour >= 10) ? hour.toString() : "0$hour";
        final String mi = (minute >= 10) ? minute.toString() : "0$minute";
        final String se = (second >= 10) ? second.toString() : "0$second";
        return "$yearStr-$mo-$da $ho:$mi:$se";
      }   
      default:
        throw ArgumentError("Unsupported frequency alias: $freq");
    }
  }

  /// Rounds down the timestamp to the specified frequency.
  ///
  /// Example:
  /// ```dart
  /// ts.floor('H');  // e.g. "1999-01-24 08:44:15" -> "1999-01-24 08"
  /// ```
  ///   - Frequency Aliases:
  ///       - 'D': Floor to day-level precision, formatted as "YYYY-MM-DD".
  ///       - 'H': Floor to hour-level precision, formatted as "YYYY-MM-DD HH".
  ///       - 'T': Floor to minute-level precision, formatted as "YYYY-MM-DD HH:MM".
  ///       - 'S': Floor to second-level precision, formatted as "YYYY-MM-DD HH:MM:SS".
  ///       - 'L': Floor to millisecond-level precision, formatted as "YYYY-MM-DD HH:MM:SS.sss".
  ///       - 'U': Floor to microsecond-level precision, formatted as "YYYY-MM-DD HH:MM:SS.sssuuu".
  String floor(String freq) {
    final freqUpper = freq.toUpperCase();
    switch (freqUpper) {
      case 'D':return '$year-${month.toString().padLeft(2, '0')}-${day.toString().padLeft(2, '0')}';
      case 'H':return '$year-${month.toString().padLeft(2, '0')}-${day.toString().padLeft(2, '0')} ${hour.toString().padLeft(2, '0')}';
      case 'T':return '$year-${month.toString().padLeft(2, '0')}-${day.toString().padLeft(2, '0')} ${hour.toString().padLeft(2, '0')}:${minute.toString().padLeft(2, '0')}';
      case 'S':return '$year-${month.toString().padLeft(2, '0')}-${day.toString().padLeft(2, '0')} ${hour.toString().padLeft(2, '0')}:${minute.toString().padLeft(2, '0')}:${second.toString().padLeft(2, '0')}';
      case 'L':return '$year-${month.toString().padLeft(2, '0')}-${day.toString().padLeft(2, '0')} ${hour.toString().padLeft(2, '0')}:${minute.toString().padLeft(2, '0')}:${second.toString().padLeft(2, '0')}.${millisecond.toString().padLeft(3, '0')}';
      case 'U':return '$year-${month.toString().padLeft(2, '0')}-${day.toString().padLeft(2, '0')} ${hour.toString().padLeft(2, '0')}:${minute.toString().padLeft(2, '0')}:${second.toString().padLeft(2, '0')}.${millisecond.toString().padLeft(3, '0')}${microsecond.toString().padLeft(3, '0')}';
      case 'N':return '$year-${month.toString().padLeft(2, '0')}-${day.toString().padLeft(2, '0')} ${hour.toString().padLeft(2, '0')}:${minute.toString().padLeft(2, '0')}:${second.toString().padLeft(2, '0')}.${millisecond.toString().padLeft(3, '0')}${microsecond.toString().padLeft(3, '0')}${_nanosecond.toString().padLeft(3, '0')}';
      default:throw ArgumentError("Unsupported frequency for floor: $freq");
    }
  }

  /// Rounds up the timestamp to the specified frequency.
  ///
  /// Example:
  /// ```dart
  /// ts.ceil('T');  // e.g. "1999-01-03 08:44:15" -> "1999-01-03 08:45"
  /// ```
  ///   - Frequency Alises:
  ///       - 'D': Ceil to day-level precision, formatted as "YYYY-MM-DD".
  ///       - 'H': Ceil to hour-level precision, formatted as "YYYY-MM-DD HH".
  ///       - 'T': Ceil to minute-level precision, formatted as "YYYY-MM-DD HH:MM".
  ///       - 'S': Ceil to second-level precision, formatted as "YYYY-MM-DD HH:MM:SS".
  ///       - 'L': Ceil to millisecond-level precision, formatted as "YYYY-MM-DD HH:MM:SS.sss".
  ///       - 'U': Ceil to microsecond-level precision, formatted as "YYYY-MM-DD HH:MM:SS.sssuuu".
  String ceil(String freq) {
    final freqUpper = freq.toUpperCase();
    // Convert our existing _dateTime plus _nanosecond to total nanoseconds from epoch
    var totalNanos = _dateTime.microsecondsSinceEpoch * 1000 + _nanosecond;
    // Determine the step size in nanoseconds
    int stepSize;
    switch (freqUpper) {
      case 'D':
        stepSize = 24 * 60 * 60 * 1000000000;
        break;
      case 'H':
        stepSize = 60 * 60 * 1000000000;
        break;
      case 'T':
        stepSize = 60 * 1000000000;
        break;
      case 'S':
        stepSize = 1000000000;
        break;
      case 'L':
        stepSize = 1000000;
        break;
      case 'U':
        stepSize = 1000;
        break;
      case 'N':
        stepSize = 1;
        break;
      default:
        throw ArgumentError("Unsupported frequency for ceil: $freq");
    }
    // If there's a remainder, add the difference to round up
    final remainder = totalNanos % stepSize;
    if (remainder != 0) {
      totalNanos += (stepSize - remainder);
    }
    // Convert back to a DateTime at microsecond precision
    final dtCeil = DateTime.fromMicrosecondsSinceEpoch(totalNanos ~/ 1000);
    final leftoverNanos = totalNanos % 1000;
    // Extract fields from dtCeil (plus leftoverNanos)
    final y  = dtCeil.year;
    final mo = dtCeil.month;
    final d  = dtCeil.day;
    final h  = dtCeil.hour;
    final m  = dtCeil.minute;
    final s  = dtCeil.second;
    final ms = dtCeil.millisecond;
    final us = dtCeil.microsecond; 
    final ns = leftoverNanos; // leftover fraction after microseconds
    // Return the formatted string in the same style as floor()
    switch (freqUpper) {
      case 'D':
        return '$y-${mo.toString().padLeft(2, '0')}-${d.toString().padLeft(2, '0')}';
      case 'H':
        return '$y-${mo.toString().padLeft(2, '0')}-${d.toString().padLeft(2, '0')} '
              '${h.toString().padLeft(2, '0')}';
      case 'T':
        return '$y-${mo.toString().padLeft(2, '0')}-${d.toString().padLeft(2, '0')} '
              '${h.toString().padLeft(2, '0')}:${m.toString().padLeft(2, '0')}';
      case 'S':
        return '$y-${mo.toString().padLeft(2, '0')}-${d.toString().padLeft(2, '0')} '
              '${h.toString().padLeft(2, '0')}:${m.toString().padLeft(2, '0')}:${s.toString().padLeft(2, '0')}';
      case 'L':
        return '$y-${mo.toString().padLeft(2, '0')}-${d.toString().padLeft(2, '0')} '
              '${h.toString().padLeft(2, '0')}:${m.toString().padLeft(2, '0')}:${s.toString().padLeft(2, '0')}.'
              '${ms.toString().padLeft(3, '0')}';
      case 'U':
        return '$y-${mo.toString().padLeft(2, '0')}-${d.toString().padLeft(2, '0')} '
              '${h.toString().padLeft(2, '0')}:${m.toString().padLeft(2, '0')}:${s.toString().padLeft(2, '0')}.'
              '${ms.toString().padLeft(3, '0')}${us.toString().padLeft(3, '0')}';
      case 'N':
        return '$y-${mo.toString().padLeft(2, '0')}-${d.toString().padLeft(2, '0')} '
              '${h.toString().padLeft(2, '0')}:${m.toString().padLeft(2, '0')}:${s.toString().padLeft(2, '0')}.'
              '${ms.toString().padLeft(3, '0')}${us.toString().padLeft(3, '0')}${ns.toString().padLeft(3, '0')}';
      default:
        throw ArgumentError("Unsupported frequency for ceil: $freq");
    }
  }

  /// Rounds the timestamp to the specified frequency (round half up).
  ///
  /// Example:
  /// ```dart
  /// ts.round('T');  // e.g. "1999-01-03 08:44:50" -> "1999-01-03 08:45"
  /// ```
  ///   - Frequency Aliases:
  ///       - 'D': Round to day-level precision, formatted as "YYYY-MM-DD".
  ///       - 'H': Round to hour-level precision, formatted as "YYYY-MM-DD HH".
  ///       - 'T': Round to minute-level precision, formatted as "YYYY-MM-DD HH:MM".
  ///       - 'S': Round to second-level precision, formatted as "YYYY-MM-DD HH:MM:SS".
  ///       - 'L': Round to millisecond-level precision, formatted as "YYYY-MM-DD HH:MM:SS.sss".
  ///       - 'U': Round to microsecond-level precision, formatted as "YYYY-MM-DD HH:MM:SS.sssuuu".
  String round(String freq) {
    final freqUpper = freq.toUpperCase();

    // Convert _dateTime + _nanosecond into total nanoseconds from epoch
    var totalNanos = _dateTime.microsecondsSinceEpoch * 1000 + _nanosecond;

    // Determine step size in nanoseconds
    int stepSize;
    switch (freqUpper) {
      case 'D':
        stepSize = 24 * 60 * 60 * 1000000000; // 1 day
        break;
      case 'H':
        stepSize = 60 * 60 * 1000000000;      // 1 hour
        break;
      case 'T':
        stepSize = 60 * 1000000000;           // 1 minute
        break;
      case 'S':
        stepSize = 1000000000;                // 1 second
        break;
      case 'L':
        stepSize = 1000000;                   // 1 millisecond
        break;
      case 'U':
        stepSize = 1000;                      // 1 microsecond
        break;
      case 'N':
        // Skip rounding if nanosecond precision
        stepSize = 1;
        break;
      default:
        throw ArgumentError("Unsupported frequency for round: $freq");
    }

    // If nanosecond precision, no rounding logic. Otherwise, do standard
    if (freqUpper != 'N') {
      final remainder = totalNanos % stepSize;
      final halfStep = stepSize ~/ 2; // integer half

      if (remainder >= halfStep) {
        // Round up
        totalNanos += (stepSize - remainder);
      } else {
        // Round down
        totalNanos -= remainder;
      }
    }

    // Convert back to a DateTime
    final dtRound = DateTime.fromMicrosecondsSinceEpoch(totalNanos ~/ 1000);
    final leftoverNanos = totalNanos % 1000; // sub-microsecond leftover

    // Extract fields from dtRound
    final y  = dtRound.year;
    final mo = dtRound.month;
    final d  = dtRound.day;
    final h  = dtRound.hour;
    final m  = dtRound.minute;
    final s  = dtRound.second;
    final ms = dtRound.millisecond;
    final us = dtRound.microsecond; 
    final ns = leftoverNanos; // leftover fraction after microseconds

    // Return the formatted string
    switch (freqUpper) {
      case 'D':
        return '$y-${mo.toString().padLeft(2, '0')}-${d.toString().padLeft(2, '0')}';
      case 'H':
        return '$y-${mo.toString().padLeft(2, '0')}-${d.toString().padLeft(2, '0')} '
              '${h.toString().padLeft(2, '0')}';
      case 'T':
        return '$y-${mo.toString().padLeft(2, '0')}-${d.toString().padLeft(2, '0')} '
              '${h.toString().padLeft(2, '0')}:${m.toString().padLeft(2, '0')}';
      case 'S':
        return '$y-${mo.toString().padLeft(2, '0')}-${d.toString().padLeft(2, '0')} '
              '${h.toString().padLeft(2, '0')}:${m.toString().padLeft(2, '0')}:${s.toString().padLeft(2, '0')}';
      case 'L':
        return '$y-${mo.toString().padLeft(2, '0')}-${d.toString().padLeft(2, '0')} '
              '${h.toString().padLeft(2, '0')}:${m.toString().padLeft(2, '0')}:${s.toString().padLeft(2, '0')}.'
              '${ms.toString().padLeft(3, '0')}';
      case 'U':
        return '$y-${mo.toString().padLeft(2, '0')}-${d.toString().padLeft(2, '0')} '
              '${h.toString().padLeft(2, '0')}:${m.toString().padLeft(2, '0')}:${s.toString().padLeft(2, '0')}.'
              '${ms.toString().padLeft(3, '0')}${us.toString().padLeft(3, '0')}';
      case 'N':
        // No rounding at nanosecond precision
        return '$y-${mo.toString().padLeft(2, '0')}-${d.toString().padLeft(2, '0')} '
              '${h.toString().padLeft(2, '0')}:${m.toString().padLeft(2, '0')}:${s.toString().padLeft(2, '0')}.'
              '${ms.toString().padLeft(3, '0')}${us.toString().padLeft(3, '0')}${ns.toString().padLeft(3, '0')}';
      default:
        throw ArgumentError("Unsupported frequency for round: $freq");
    }
  }
  /// Format the timestamp as a string using format codes.
  /// 
  /// Example:
  /// ```dart
  /// ts.strftime('%d-%b-%Y');  // 09-Feb-2025
  /// ```
  ///   - Format Codes: 
  ///       - %Y => 4-digit year, e.g. "2025"
  ///       - %y => 2-digit year, e.g. "25"
  ///       - %m => zero-padded month [01..12]
  ///       - %d => zero-padded day [01..31]
  ///       - %H => hour (24-hour) [00..23]
  ///       - %I => hour (12-hour) [01..12]
  ///       - %M => minute [00..59]
  ///       - %S => second [00..59]
  ///       - %p => "AM"/"PM"
  ///       - %a => abbreviated weekday name (Mon, Tue, ...)
  ///       - %A => full weekday name (Monday, Tuesday, ...)
  ///       - %b => abbreviated month name (Jan, Feb, ...)
  ///       - %B => full month name (January, February, ...)
  ///       - %z => timezone offset in +HHMM or -HHMM
  ///       - %% => literal '%'
  String strftime(String dateFormat) {
    final dt = _dateTime;
    final buffer = StringBuffer();

    // Helper to format a value with leading zeros
    String pad(int value, int width) => value.toString().padLeft(width, '0');

    for (int i = 0; i < dateFormat.length; i++) {
      if (dateFormat[i] == '%') {
        // If '%' is the last character, just append '%'
        if (i == dateFormat.length - 1) {
          buffer.write('%');
          break;
        }
        final nextChar = dateFormat[i + 1];
        i++; 

        switch (nextChar) {
          case '%':
            buffer.write('%');
            break;
          case 'Y': // 4-digit year
            buffer.write(dt.year.toString().padLeft(4, '0'));
            break;
          case 'y': // 2-digit year
            buffer.write(pad(dt.year % 100, 2));
            break;
          case 'm': // zero-padded month
            buffer.write(pad(dt.month, 2));
            break;
          case 'd': // zero-padded day
            buffer.write(pad(dt.day, 2));
            break;
          case 'H': // 24-hour
            buffer.write(pad(dt.hour, 2));
            break;
          case 'I': // 12-hour
            final hour12 = dt.hour == 0 ? 12 : (dt.hour > 12 ? dt.hour - 12 : dt.hour);
            buffer.write(pad(hour12, 2));
            break;
          case 'M': // minute
            buffer.write(pad(dt.minute, 2));
            break;
          case 'S': // second
            buffer.write(pad(dt.second, 2));
            break;
          case 'p': // AM/PM
            buffer.write(dt.hour < 12 ? 'AM' : 'PM');
            break;
          case 'a': // abbreviated weekday name
            buffer.write(_shortWeekday(dt.weekday));
            break;
          case 'A': // full weekday name
            buffer.write(_fullWeekday(dt.weekday));
            break;
          case 'b': // abbreviated month name
            buffer.write(_shortMonth(dt.month));
            break;
          case 'B': // full month name
            buffer.write(_fullMonth(dt.month));
            break;
          case 'z': // timezone offset
            buffer.write(_formatTimeZoneOffset(dt.timeZoneOffset));
            break;
          default:
            // If we don’t recognize the directive, just put it back
            buffer.write('%$nextChar');
            break;
        }
      } else {
        // Normal character
        buffer.write(dateFormat[i]);
      }
    }

    return buffer.toString();
  }
  // Abbreviated weekday name
  static String _shortWeekday(int wday) {
    const names = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
    return names[(wday - 1) % 7];
  }

  // Full weekday name
  static String _fullWeekday(int wday) {
    const names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'];
    return names[(wday - 1) % 7];
  }

  // Abbreviated month name
  static String _shortMonth(int month) {
    const names = [
      'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
      'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'
    ];
    return names[(month - 1) % 12];
  }

  // Full month name
  static String _fullMonth(int month) {
    const names = [
      'January', 'February', 'March', 'April', 'May', 'June',
      'July', 'August', 'September', 'October', 'November', 'December'
    ];
    return names[(month - 1) % 12];
  }

  // Format timezone offset as +HHMM or -HHMM
  static String _formatTimeZoneOffset(Duration offset) {
    final sign = offset.isNegative ? '-' : '+';
    final absOffset = offset.abs();
    final h = absOffset.inHours;
    final m = absOffset.inMinutes % 60;
    return '$sign${h.toString().padLeft(2, '0')}${m.toString().padLeft(2, '0')}';
  }
}


class Timedelta extends Duration {
  final int nanoseconds;

  Timedelta({
    super.days = 0,
    super.hours = 0,
    super.minutes = 0,
    super.seconds = 0,
    super.milliseconds = 0,
    super.microseconds = 0,
    this.nanoseconds = 0,
  });

  static final Timedelta zero = Timedelta();

  @override
  String toString() {
    final totalHours = inHours;
    final minutes = inMinutes.remainder(60);
    final seconds = inSeconds.remainder(60);
    final micro = inMicroseconds.remainder(1000000);
    final totalMicro = micro * 1000 + nanoseconds; // combine into full nanoseconds
    final fracStr = totalMicro.toString().padLeft(9, '0'); // 9 digits = nanoseconds

    return "$totalHours:${minutes.toString().padLeft(2, '0')}:${seconds.toString().padLeft(2, '0')}.$fracStr";
  }
}