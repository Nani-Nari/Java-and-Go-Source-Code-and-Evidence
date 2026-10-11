# Assignment 6: Multi-threaded Data Processing System

Java and Go implementations process 12 tasks with 4 workers and save verified CSV results.

## Java
```bash
cd java
javac DataProcessingSystem.java
java DataProcessingSystem
```
Output: `java/java_results.csv`.

## Go
```bash
cd go
go run -race main.go
```
Output: `go/go_results.csv`.

## Validation
Each program checks 12 unique task IDs and correct squares before writing results. Worker order is nondeterministic. Screenshots in `evidence/` are captured from actual execution logs.

## GitHub
Create your own repository and upload this project. Paste the resulting URL into the report; no GitHub repository has been published automatically.
