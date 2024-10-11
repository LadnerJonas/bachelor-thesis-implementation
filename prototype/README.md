# Prototype
This prototype implements a benchmark to compare in-memory partition algorithms for the bachelor thesis.  

## Table of Contents
- [Building the Project](#building-the-project)
- [Running Input Data Initialization](#running-input-data-initialization)
- [Running Benchmarks](#running-benchmarks)
- [Formatting Code](#formatting-code)

## Building the Project
To build the project, run the following commands:

```bash
mkdir -p build
cd build
cmake ..
cmake --build .
```
This will build the project in the `build` directory and already create the necessary input files.


## Running Input Data Initialization
To generate input data for the application, run the following command:

```bash
cd build
cmake --build . -t create_input_data
```

## Running Benchmarks
To run the benchmarks, run the following command:

```bash
cd build
cmake --build . -t benchmark
```


## Formatting Code
To format the code, run the following command:

```bash
cd build
cmake --build . -t format
```