import 'package:df/df.dart';

Future<void> main() async {
  final df = await DataFrame.fromCsv("dataset/stocks.csv");
  df.show();
  print(df.columns);
}
