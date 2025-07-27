# pdataframe

A DataFrame library for storing, manipulating, and analyzing data, using an interface similar to Python’s pandas.
Designed for large datasets; key features include fast label-based access (`O(k)`) and support for duplicate labels. 

## Usage examples:

Create a DataFrame
```dart
// List input; each inner List is a row of data 
final dfList = DataFrame([[1,2,3],[4,5,6],[7,8,9]]); 

// Map input; keys are the column names and the value List's are the associated column of data
final dfMap = DataFrame({0:[1,4,7], 1:[2,5,8], 2:[3,6,9]});

// List input Using parameters to set column/row names
final df = DataFrame([[1,2,3.0],[4,5,6],[7,'hi',9]], index:['Dog','Dog','Cat'], columns:['M','W','F']); 

print(df);
//     | M |   W  |  F
// --------------------  
// Dog | 1 |   2  | 3.0   
// Dog | 4 |   5  |  6
// Cat | 7 | 'no' |  9
```

Verify column types
```dart
print(df.dtypes); // [int, Object, Double]
```
Note: For simplicity, `dtypes` does not distinguish between nullable and non-nullable types (e.g. int? shown as int). 

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
var newDf = df.append([[1,2,3]], columns:['M','W','F']); 
```
Note: Use parameter `inplace:true` to modify the current DataFrame

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

Timestamp (basic functionality)
```dart
// Create a Timestamp
var ts1 = Timestamp("February 9, 1999");
```

### Miscellaneous Functions
These functions provide additional flexibility and allow for fine-tuning specific behaviors. 

#### DataFrame Utilities
- `reset_index()` – Resets the row index of the DataFrame to a default integer-based index.
- `sort()` – Sorts the DataFrame rows by a specified column.
- `head()` – Returns the first N rows of the DataFrame.
- `info()` – Prints and returns a summary of the DataFrame, including the column index, name, non-null count, and data type.
- `table()` – Converts the entire DataFrame to a list of rows, optionally including the row index as the first column.

#### Data Cleaning & Filtering
`filterNulls()`, `countNulls()`, `countZeros()`, `interpolate()`, `knnImputer()`

#### Descriptive Statistics
`sumCol()`, `mean()`, `max()`, `min()`, `rollingMad()`, `weightedQuantile()`, `weightedHistogram()`

#### Rolling & Expanding Windows
`rollingSum()`, `rollingMean()`, `rollingStd()`, `rollingApply()`, `expandingMin()`, `expandingMax()`, `expandingMean()`, `expandingVar()`

#### Grouping & Reshaping
`groupBy()`, `pivotTable()`, `melt()`, `resample()`

#### Time Series Analysis
`autocorrelation()`, `partialAutocorrelation()`, `seasonalDecompose()`, `exponentialSmoothing()`

#### Statistical Tests
`tTest()`, `anova()`, `chiSquare()`, `bootstrap()`

#### Linear Algebra & Decomposition
`covarianceMatrix()`, `pca()`, `svd()`

#### Signal Processing
`fft()`, `convolve()`, `crossCorrelate()`

#### Outlier Detection
`outlierIQR()`, `outlierZScore()`, `localOutlierFactor()`

For more details, refer to the full documentation or the code comments.