# Implementation of the Bachelor Thesis

This bachelor thesis implements efficient shuffle operators for streaming database systems.

## Table of Contents
- [Cloning the Repository](#cloning-the-repository)
- [Setting up the Python Environment](#setting-up-the-python-environment)
- [Prototype](#prototype)
- [Shuffle operator implementations](#shuffle-operator-implementations)

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

## Prototype
The prototype implements a benchmark to compare in-memory partition algorithms.
### Running the Prototype
Please follow the instructions in the [prototype/README.md](prototype/README.md) file to run the prototype.

## Shuffle operator implementations
The various shuffle operator implementations can be found in the `include/` and `src/` directory. The benchmarks and tests can be executed in the `benchmark/` and `test/` directory.

## Overview repository
For an overview of the key findings and access to the accompanying thesis and presentation, please visit the [overview repository](https://github.com/LadnerJonas/bachelor-thesis)
