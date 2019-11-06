/// An exception for type conversions errors
class TypeConversionException implements Exception {
  TypeConversionException(this.message);

  /// The error message
  final String message;
}

/// An exception for a column not found
class ColumnNotFoundException implements Exception {
  ColumnNotFoundException(this.message);

  /// The error message
  final String message;
}

/// An exception for a file not found
class FileNotFoundException implements Exception {
  FileNotFoundException(this.message);

  /// The error message
  final String message;
}
