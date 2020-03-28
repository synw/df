/// Get typed data from a value
dynamic castTypeForValue(dynamic value, Type colType) {
  dynamic v;
  switch (colType) {
    case int:
      v = value as int;
      break;
    case double:
      v = value as double;
      break;
    case DateTime:
      v = value as DateTime;
      break;
    default:
  }
  return v;
}

/// The format to use for timestamps
enum TimestampFormat {
  /// Timestamp in seconds from epoch
  seconds,

  /// Timestamp in milliseconds from epoch
  milliseconds,

  /// Timestamp in microseconds from epoch
  microseconds
}
