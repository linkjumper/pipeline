# pipeline

The pipeline is a data flow-oriented implementation
of an acyclic directed graph. The modules are executed in quasi-parallel using the async await mechanism. In addition, tasks can be distributed over several processes.

The pipeline concept is a Python implementation of [C++ steamlein](https://github.com/gottliebtfreitag/steamlein).

|![](images/1_parallel_modules.png?raw=true)|![](images/4_timingdiagram.png?raw=true)|
|-|-|
|*Parallel module execution* | *Timing diagram (see example code)*|


|![](images/2_concatenated_nested_modules.png?raw=true)|
|-|
|*Concatenated nested modules*|

|![](images/3_multi_provide_require.png?raw=true)|
|-|
|*Multi provide require*|
