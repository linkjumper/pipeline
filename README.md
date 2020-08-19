# pipeline

The pipeline is a data flow-oriented implementation
of an acyclic directed graph. The modules are executed in quasi-parallel using the async await mechanism. In addition, tasks can be distributed over several processes.

The pipeline concept is a Python implementation of [C++ steamlein](https://github.com/gottliebtfreitag/steamlein).

|![](images/1_parallel_modules.png?raw=true)|
|-|
|*Image 1 Parallel module execution*|


|![](images/2_concatenated_nested_modules.png?raw=true)|
|-|
|*Image 2 Concatenated nested modules*|

|![](images/3_multi_provide_require.png?raw=true)|
|-|
|*Image 3 Multi provide require*|
