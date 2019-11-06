import 'package:meta/meta.dart';

/// A data frame column
class DataFrameColumn {
  /// Provide a [name] and [type]
  DataFrameColumn({@required this.name, @required this.type});

  /// The column's name
  String name;

  /// The column's type
  Type type;

  @override
  String toString() {
    return "$name ($type)";
  }

  @override
  int get hashCode => name.hashCode;

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is DataFrameColumn &&
          runtimeType == other.runtimeType &&
          name == other.name;
}
