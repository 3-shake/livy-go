# livy-go

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
