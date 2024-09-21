# Implementierung

This project implements a C++ application using CMake for building, with support for benchmarking, testing, and input data initialization using Python. The project includes features like code formatting, debug/release configurations, and Python environment management.

## Table of Contents
- [Cloning the Repository](#cloning-the-repository)
- [Setting up the Python Environment](#setting-up-the-python-environment)
- [Running Input Data Initialization](#running-input-data-initialization)
- [Building the Project](#building-the-project)
- [Formatting Code](#formatting-code)
- [Running Tests](#running-tests)
- [Running Benchmarks](#running-benchmarks)

## Cloning the Repository

To clone the repository along with its required submodules, run the following command:

```bash
git clone --recursive git@github.com:LadnerJonas/bachelor-thesis-implementation.git
```

## Setting up the Python Environment

To set up the Python environment, run the following commands:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

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


## Formatting Code
To format the code, run the following command:

```bash
cd build
cmake --build . -t format
```

## Running Tests
To run the tests, run the following command:

```bash
cd build
cmake --build . -t test
```

## Running Benchmarks
To run the benchmarks, run the following command:

```bash
cd build
cmake --build . -t benchmark
```
