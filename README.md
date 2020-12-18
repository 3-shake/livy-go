# livy-go

[![Actions Status](https://github.com/3-shake/livy-go/workflows/Test/badge.svg)](https://github.com/3-shake/livy-go/actions?workflow=Test)
[![Go Report Card](https://goreportcard.com/badge/github.com/3-shake/livy-go)](https://goreportcard.com/report/github.com/3-shake/livy-go)
[![Documentation](https://godoc.org/github.com/3-shake/livy-go?status.svg)](http://godoc.org/github.com/3-shake/livy-go)

## Getting Started

### Livy Setup

1. livy install
```
make livy.install
```

2. livy start
```
make livy.start
```

3. livy stop
```
make livy.stop
```

## リリース方法
1. `yarn install`
2. `git checkout -b master origin/master`
3. `yarn semantic-release --no-ci --branches master`
