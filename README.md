# orredis

Just a a simple fast ORM for python built in rust

## How to demo

```shell
python3 -m venv env 
source env/bin/activate
pip install -r requirements.txt
maturin develop
python
import orredis
# do stuff with orredis
```

## How to Test

```shell
python3 -m venv env 
source env/bin/activate
pip install -r requirements.txt
maturin develop
pytest
```

## Benchmarks

16th September 2022

### Bulk Inserts

#### pydantic-redis

```
------------------------------------------------------- benchmark: 1 tests -------------------------------------------------------
Name (time in ms)                              Min     Max    Mean  StdDev  Median     IQR  Outliers       OPS  Rounds  Iterations
----------------------------------------------------------------------------------------------------------------------------------
test_benchmark_bulk_insert[redis_store]     1.0353  1.4726  1.0766  0.0746  1.0478  0.0307       4;7  928.8924      59           1
----------------------------------------------------------------------------------------------------------------------------------
```

#### orredis

```
---------------------------------------------------------------- benchmark: 1 tests ---------------------------------------------------------------
Name (time in us)                                Min         Max      Mean    StdDev    Median      IQR  Outliers  OPS (Kops/s)  Rounds  Iterations
---------------------------------------------------------------------------------------------------------------------------------------------------
test_benchmark_bulk_insert[redis_store]     713.9260  1,647.2760  795.9723  122.0521  767.7155  50.3980       8;9        1.2563     100           1
---------------------------------------------------------------------------------------------------------------------------------------------------
```

### Single Insert

#### pydantic-redis

```
------------------------------------------------------------------ benchmark: 1 tests ------------------------------------------------------------------
Name (time in us)                                        Min       Max      Mean   StdDev    Median      IQR  Outliers  OPS (Kops/s)  Rounds  Iterations
--------------------------------------------------------------------------------------------------------------------------------------------------------
test_benchmark_single_insert[redis_store-book0]     394.7420  674.7820  443.2762  54.0996  429.4530  39.5740       7;7        2.2559      61           1
--------------------------------------------------------------------------------------------------------------------------------------------------------
```

#### orredis

``` 
------------------------------------------------------------------ benchmark: 1 tests ------------------------------------------------------------------
Name (time in us)                                        Min       Max      Mean   StdDev    Median      IQR  Outliers  OPS (Kops/s)  Rounds  Iterations
--------------------------------------------------------------------------------------------------------------------------------------------------------
test_benchmark_single_insert[redis_store-book0]     252.2160  522.1410  287.8005  28.8017  292.8330  37.6785    249;40        3.4746    1511           1
--------------------------------------------------------------------------------------------------------------------------------------------------------
```

### Select All Items, All Columns

#### pydantic-redis

```
--------------------------------------------------------- benchmark: 1 tests --------------------------------------------------------
Name (time in ms)                                 Min     Max    Mean  StdDev  Median     IQR  Outliers       OPS  Rounds  Iterations
-------------------------------------------------------------------------------------------------------------------------------------
test_benchmark_select_default[redis_store]     1.1369  2.1050  1.2322  0.0939  1.2001  0.0653     65;57  811.5888     536           1
-------------------------------------------------------------------------------------------------------------------------------------
```

#### orredis

```
--------------------------------------------------------- benchmark: 1 tests --------------------------------------------------------
Name (time in ms)                                 Min     Max    Mean  StdDev  Median     IQR  Outliers       OPS  Rounds  Iterations
-------------------------------------------------------------------------------------------------------------------------------------
test_benchmark_select_default[redis_store]     2.7405  3.8420  2.9211  0.1427  2.8873  0.1426     53;13  342.3406     262           1
-------------------------------------------------------------------------------------------------------------------------------------
```

### Select All Items, Some Columns

#### pydantic-redis

```
----------------------------------------------------------------- benchmark: 1 tests ----------------------------------------------------------------
Name (time in us)                                   Min         Max      Mean   StdDev    Median      IQR  Outliers  OPS (Kops/s)  Rounds  Iterations
-----------------------------------------------------------------------------------------------------------------------------------------------------
test_benchmark_select_columns[redis_store]     829.1210  1,435.5640  936.6411  73.9743  910.0580  72.9880    142;34        1.0676     632           1
-----------------------------------------------------------------------------------------------------------------------------------------------------
```

#### orredis

```
--------------------------------------------------------- benchmark: 1 tests --------------------------------------------------------
Name (time in ms)                                 Min     Max    Mean  StdDev  Median     IQR  Outliers       OPS  Rounds  Iterations
-------------------------------------------------------------------------------------------------------------------------------------
test_benchmark_select_columns[redis_store]     1.3982  2.1448  1.4675  0.0568  1.4578  0.0412     53;27  681.4287     487           1
-------------------------------------------------------------------------------------------------------------------------------------

```

### Select Some Items, All Columns

#### pydantic-redis

``` 
------------------------------------------------------------------ benchmark: 1 tests ------------------------------------------------------------------
Name (time in us)                                      Min         Max        Mean    StdDev    Median       IQR  Outliers       OPS  Rounds  Iterations
--------------------------------------------------------------------------------------------------------------------------------------------------------
test_benchmark_select_some_items[redis_store]     638.8910  9,931.9470  1,010.0479  990.6769  805.8675  170.2030    26;101  990.0520     786           1
--------------------------------------------------------------------------------------------------------------------------------------------------------
```

#### orredis

``` 
---------------------------------------------------------- benchmark: 1 tests ----------------------------------------------------------
Name (time in ms)                                    Min     Max    Mean  StdDev  Median     IQR  Outliers       OPS  Rounds  Iterations
----------------------------------------------------------------------------------------------------------------------------------------
test_benchmark_select_some_items[redis_store]     1.3566  3.1007  1.5060  0.1592  1.4519  0.1252     58;38  663.9910     467           1
----------------------------------------------------------------------------------------------------------------------------------------
```

### Update

#### pydantic-redis

``` 

------------------------------------------------------------------------ benchmark: 1 tests -----------------------------------------------------------------------
Name (time in us)                                                   Min       Max      Mean   StdDev    Median      IQR  Outliers  OPS (Kops/s)  Rounds  Iterations
-------------------------------------------------------------------------------------------------------------------------------------------------------------------
test_benchmark_update[redis_store-Wuthering Heights-data0]     322.4000  962.5480  408.8422  88.3521  380.9710  97.1238     84;27        2.4459     643           1
-------------------------------------------------------------------------------------------------------------------------------------------------------------------
```

#### orredis

``` 
-------------------------------------------------------------------------- benchmark: 1 tests -------------------------------------------------------------------------
Name (time in us)                                                   Min          Max      Mean    StdDev    Median      IQR  Outliers  OPS (Kops/s)  Rounds  Iterations
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
test_benchmark_update[redis_store-Wuthering Heights-data0]     217.3990  10,227.0590  290.2731  345.1861  254.4015  48.4215    17;145        3.4450    1832           1
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
```

### Delete One

#### pydantic-redis

```
----------------------------------------------------------------------- benchmark: 1 tests ----------------------------------------------------------------------
Name (time in us)                                             Min          Max      Mean    StdDev    Median      IQR  Outliers  OPS (Kops/s)  Rounds  Iterations
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
test_benchmark_delete[redis_store-Wuthering Heights]     151.3360  12,350.7080  452.2397  799.0145  259.6150  82.5930     42;88        2.2112     664           1
-----------------------------------------------------------------------------------------------------------------------------------------------------------------

```

#### orredis

```
---------------------------------------------------------------------- benchmark: 1 tests ---------------------------------------------------------------------
Name (time in us)                                             Min         Max      Mean   StdDev    Median      IQR  Outliers  OPS (Kops/s)  Rounds  Iterations
---------------------------------------------------------------------------------------------------------------------------------------------------------------
test_benchmark_delete[redis_store-Wuthering Heights]     130.3440  2,769.9800  178.2922  89.9968  159.6075  43.9260   116;163        5.6088    2358           1
---------------------------------------------------------------------------------------------------------------------------------------------------------------
```

### Delete Bulk

#### pydantic-redis

```
---------------------------------------------------------------- benchmark: 1 tests ---------------------------------------------------------------
Name (time in us)                                Min         Max      Mean    StdDev    Median      IQR  Outliers  OPS (Kops/s)  Rounds  Iterations
---------------------------------------------------------------------------------------------------------------------------------------------------
test_benchmark_bulk_delete[redis_store]     183.0900  6,198.5030  557.8025  976.7148  240.6170  85.4450     15;20        1.7927     147           1
--------------------------------------------------------------------------------------------------------------------------------------------------- 
```

#### orredis

``` 
--------------------------------------------------------------- benchmark: 1 tests ---------------------------------------------------------------
Name (time in us)                                Min         Max      Mean   StdDev    Median      IQR  Outliers  OPS (Kops/s)  Rounds  Iterations
--------------------------------------------------------------------------------------------------------------------------------------------------
test_benchmark_bulk_delete[redis_store]     146.5190  1,668.0070  186.7605  67.9552  172.5450  37.2472   162;182        5.3545    2439           1
--------------------------------------------------------------------------------------------------------------------------------------------------
```

## How to test

- Clone the repo and enter its root folder

  ```bash
  git clone https://github.com/sopherapps/orredis.git && cd orredis
  ```

- Create a virtual environment and activate it

  ```bash
  virtualenv -p /usr/bin/python3.7 env && source env/bin/activate
  ```

- Install the dependencies

  ```bash
  pip install -r requirements.txt
  ```

- Install the package in the virtual environment

  ```bash
  maturin develop
  ```

- Run the tests command

  ```bash
  pytest --benchmark-disable
  ```

- Run benchmarks

  ```bash
  pytest --benchmark-compare --benchmark-autosave
  ```
