# FSST Plus 
FSST Plus is an improvement on FSST (the current state-of-the-art for random access string compression), giving it the ability to compress strings in a more efficient way. The goal is to integrate this enhanced compression algorithm into DuckDB's codebase.
FSST Plus optimizes compression ratios by targeting longer repeated prefixes and local redundancies in string data.

## Running the project
First run the commands

```(cd third_party/duckdb && GEN=ninja make)```

and

``
(cd third_party/fsst && cmake -S . -B build)
``

Then you should be able to run the Cmake application normally.