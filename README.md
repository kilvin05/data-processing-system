# Data Processing System — Java & Go

This project implements a parallel data processing system in **Java** and **Go**, demonstrating:
- A shared task queue
- Multiple workers consuming tasks in parallel
- Safe aggregation of results
- Robust error handling and logging
- Clean, deadlock-free termination

## Project Structure
```
data-processing-system/
  ├── java/
  │   ├── pom.xml
  │   └── src/main/java/dps/Main.java
  └── go/
      └── main.go
```

## Java
### Build & Run
```bash
cd java
mvn -q -DskipTests package
java -jar target/dps-java-1.0.0-jar-with-dependencies.jar
```

### Output
- `java/out/java_results.txt` — processed results
- `java/out/java_app.log` — log output

## Go
### Build & Run
```bash
cd go
go run .
# or
go build -o dps-go
./dps-go
```

### Output
- `go/out/go_results.txt` — processed results

## Notes
- Both implementations use "poison pills" to signal workers to exit gracefully.
- Java uses `BlockingQueue`, `ExecutorService`, and synchronized access to a shared results list, flushed to a file at the end.
- Go uses channels for tasks/results, goroutines for workers, and a single writer goroutine for file output.
