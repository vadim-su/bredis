# Bredis
Bredis is a Redis-like database with similar functions and an HTTP API.

## Installation
```bash
cargo install bredis
```

## Usage
```bash
bredis run
```

## API
### GET
```bash
curl http://localhost:8080/keys/mykey
```

### GET BY PREFIX
```bash
curl http://localhost:8080/keys?prefix=my
```

### SET
```bash
curl -X POST -d "{\"key\":\"mykey\",\"value\":\"myvalue\"}" http://localhost:8080/keys
```

### DELETE
```bash
curl -X DELETE http://localhost:8080/keys/mykey
```

### DELETE BY PREFIX
```bash
curl -X DELETE -d "{\"prefix\":\"my\"}" http://localhost:8080/keys
```
