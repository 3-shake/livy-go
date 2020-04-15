## Usage

```bash
$ cd /path/to/livy-go/integration_tests/wordcount/

$ sbt assembly

$ cd ..

$ docker-compose up -d

$ go test -v ./...
```
