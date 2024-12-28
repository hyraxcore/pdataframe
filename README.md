# pdataframe

A DataFrame library for storing, manipulating, and analyzing data, using an interface similar to Python’s pandas.

## Usage examples:

Create a DataFrame
```dart
// List input; each inner List is a row of data 
final dfList = DataFrame([[1,2,3],[4,5,6],[7,8,9]]); 

// Map input; keys are the column names and the value List's are the associated column of data
final dfMap = DataFrame({0:[1,4,7], 1:[2,5,8], 2:[3,6,9]});

// List input Using parameters to set column/row names
final df = DataFrame([[1,2,3.0],[4,5,6],[7,'hi',9]], index:['Dog','Dog','Cat'], columns:['a','b','c']); 

print(df);
//     | a |   b  |  c
// --------------------  
// Dog | 1 |   2  | 3.0   
// Dog | 4 |   5  |  6
// Cat | 7 | 'hi' |  9
```

Verify column types
```dart
print(df.dtypes); // [int, Object, Double]
```

Access a column of data
```dart
print(df['a']); // [1,4,7]
```

Retrieve a row of data
```dart
print(df2.iloc(row: 1)); // [1, 2, 3.0]
```

Edit data (multiple methods)
```dart
// Edit data via columns
df['a'][2] = [30];

// Edit row using iloc(); the row is called using it's integer index
df.iloc(row: 2, col:'a', edit: 30); 

// Edit row using loc(); the row is called using it's row name
df.iloc(row: 'Cat', col:'a', edit: 30); 

// Edit row using editRow(); the row data modified using [] operators
df.editRow['Cat']['a'] = 30; 
```

Add Data
```dart
// Add one DataFrame to another DataFrame using concat()
var newDf = concat([dfList, dfMap], axis:0); // 'axis:0' combines the columns 

// Append a new row to a DataFrame 
var newDf = df.append([[1,2,3]]); // Note: use parameter 'inplace:true' to modify current DataFrame
```

Delete Data
```dart
// Drop a row
var newDf = drop('Cat', axis:0); // 'axis:0' specifies a row operation ('axis:1' would be a column)  
```

Apply a math function to a column
```dart
// Apply '*2' to every value in column a
df.m('a',(a)=>a*2);
```

csv import/export support
```dart
// Import data from data.csv file
var file = await DataFrame.read_csv('lib/files/data.csv');

// Create csv file called dataOutput.csv and store df data in it
df.to_csv(file:'lib/files/dataOutput.csv', index: true);
```
### Miscellaneous Functions
These functions provide additional flexibility and allow for fine-tuning specific behaviors. 

- Basic Aggregation Methods: min(), max(), mean(),
- `reset_index()`: Resets the row index of the DataFrame to a default integer-based index.
- `sort()`: Sorts the DataFrame rows by a specified column.
- `head()`: Returns the first N rows of the DataFrame.
- `info()`: Prints and returns a summary of the DataFrame, including the column index, name, non-null count, and data type.
- `table()`: Converts the entire DataFrame to a list of rows, optionally including the row index as the first column.

For more details, refer to the full documentation or the code comments.