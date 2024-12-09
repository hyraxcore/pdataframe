import 'data_core.dart';

//NOTE: Series type is not currently necessary. Left for experimental purposes, will consider removing in future revisions. 

class Series<T> {
  Type inputType;
  final _matrix = DataFrameCore();
  var name;

  //Getters
  get dtypes{
    return _matrix.columnTypes;
  }
  get index{
    return _matrix.orderedEntries(_matrix.columnIndexMap, true);
  }
  get values{
    return _matrix.data;
  }

  // Default Constructor
  Series(var inputData, {List index = const [], this.name = ''}):inputType = T {

    //1.a. For List input
    if (inputData is List || inputData is List<List>) {

      //1.b. add data
      _matrix.data.addAll(inputData);
      //1.b.2. add type info
      for(var column in _matrix.data){ 
        if(column is! List){
          _matrix.columnTypes.add(column.runtimeType);
        } else {
          _matrix.columnTypes.add(checkListType(column));
        }
      }

      //1.c Create index
      if(index.isEmpty){
        if(inputData is List ){ index = List.generate(inputData.length, (i) => i);}
      }
      _matrix.indexer(index, true);
      //2.a. For Map input
    } else if (inputData is Map) {
      //pandas allows but throws all data out and fills with 'NaN'
      if(index.isNotEmpty){
        throw ArgumentError('Index not used with Map input');
      }
      inputData.forEach((key, value) {
      //2.b. Add data
        _matrix.data.add(value);
      });
      //2.c. add type data
      for(var column in _matrix.data){ 
        if(column is! List){
          _matrix.columnTypes.add(column.runtimeType);
        } else {
          _matrix.columnTypes.add(checkListType(column));
        }
      }
      //2.d. Add index
      _matrix.indexer(inputData.keys, true);
    } else {
      throw ArgumentError('Invalid input data format');
    }
  }

  //Access it like a List
  operator[](var key){
    if( key != null && _matrix.columnIndexMap.containsKey(key)){
      int listIndex = _matrix.columnIndexMap[key].first; // NOTE: since rows can have same index name, only works for first one. Use iloc/loc instead
      return _matrix.data[listIndex];
    } else {
      throw RangeError('Entered index is invalid');
    }
  }

  void operator []=(var key, var newListElement){
    if( key != null && _matrix.columnIndexMap.containsKey(key)){
      int listIndex = _matrix.columnIndexMap[key].first; // NOTE: since rows can have same index name, only works for first one. Use iloc/loc instead
      _matrix.data[listIndex] = newListElement;
    } else {
      throw RangeError('Entered index is invalid');
    }
  }

  @override
  String toString() {
    //Declare local variables
    int maxIndexLength = 2;
    List index = _matrix.orderedEntries(_matrix.columnIndexMap, true);// Minimum spacing for row index column
    // Calculate the maximum index length
    for (var rowName in _matrix.columnIndexMap.keys/*int i = 0; i < _matrix.columnIndexLength; i++*/) {
      final String rowIndexText = rowName.toString();
      if (rowIndexText.length > maxIndexLength) {
        maxIndexLength = rowIndexText.length;
      }
    }
    //Print output
    StringBuffer buffer = StringBuffer('\n');
    for (int i = 0; i < _matrix.data.length; i++) {
      if (_matrix.data[i] != null) {
        var rowDataText = _matrix.data[i];
        // Calculate custom spacing per row based on the maximum index length
        int spacing = maxIndexLength - (index[i].toString()).length;
        String spaces = '  ';
        for(int j = 0; j <= spacing; j++ ){
          spaces += ' ';
        }
        //Text output per row
        buffer.writeln('${index[i]}$spaces$rowDataText');
      }
    }
    return buffer.toString();
  }

  // Methods and properties for Series
  SeriesLocator get loc => SeriesLocator(this);
  SeriesIntegerLocator get iloc => SeriesIntegerLocator(this);

}

class SeriesLocator {
  final Series _series;
  SeriesLocator(this._series);

  operator [](String label) {
    var index = _series._matrix.columnIndexMap[label];
    if (index != -1) {
      return _series._matrix.data[index].first;
    } else {
      // Handle the case where the label is not found
      throw Exception('Label not found');
    }
  }
}

class SeriesIntegerLocator {
  final Series _series;
  SeriesIntegerLocator(this._series);

  operator [](int label) {
    return _series._matrix.data[label];
  }
}