# Differential Dataflow Demos

## Overview

This repository contains demonstrations using **Differential Dataflow**, a Rust library for high-performance data processing. The examples showcase fundamental dataflow concepts, including input sessions, joins, filters, and probe operations for iterative computation.

## Features

- **Demo 1**: Simulates hierarchical data relationships and computes derived relationships using self-joins.
- **Demo 2**: Implements a dataflow query for customers and orders, including filtering, joining, and analyzing high-priced orders.

## Prerequisites

- Rust
- [Timely Dataflow](https://github.com/TimelyDataflow/timely-dataflow)
- [Differential Dataflow](https://github.com/TimelyDataflow/differential-dataflow)

## Usage

1. Clone the repository:
   ```bash
   git clone https://github.com/e-strauss/differential_dataflow_demos.git
   cd differential_dataflow_demos
   ```
2. Run the examples:
   ```bash
   cargo run
   ```


For more details, see the [source code](https://github.com/e-strauss/differential_dataflow_demos/blob/master/src/main.rs).