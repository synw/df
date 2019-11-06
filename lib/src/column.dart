import 'package:meta/meta.dart';

class DataFrameColumn {
  DataFrameColumn({@required this.name, @required this.type});

  String name;
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
