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

enum TimestampFormat { seconds, milliseconds, microseconds }
