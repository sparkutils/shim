### 0.2.0 <small>11th June, 2025</small>

#18 - Spark 4 AgnosticEncoder and Connect support:

> callFunction shim introduced - uses sql-api on 4 and DBR 17 runtimes
> isClassic shim introduced - returns true on all pre 4/17 runtimes and on Spark 4 and above true when a classic SparkSession is provided
> Breaking change on createLambdaFunction, now returns Column instead of Expression (uses sql-api on 4 and DBR 17 runtimes)
> DBR 17.0, 14.0, 13.1 are deprecated and will be removed in the next minor version

#7 - ExpressionEncoder usage is removed for Spark 4 and compatible runtimes

#12 - EOL DBR and Spark runtimes are removed: 2.4, 9.1, 10.4, 11.3

### 0.2.0 <small>11th June, 2025</small>

#5 - Spark 4 support, including DBR 17.0

#8 - DBR 16.4 support

#9 - isStateful function and fixes

#11 - copyStateful function

### 0.0.1 <small>8th March, 2024</small>

#1 - Quality support

#2 - Frameless support

#3 - 14.3 LTS support
