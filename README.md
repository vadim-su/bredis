<img src="https://github.com/vadim-su/bredis/assets/1702003/f046fac9-f25f-4f9e-8aa2-fbca414df8e4" width="200" />

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
curl http://localhost:4123/keys/mykey
```

### GET BY PREFIX
```bash
curl http://localhost:4123/keys?prefix=my
```

### SET
```bash
curl -X POST -H "Content-Type: application/json" -d "{\"key\":\"mykey\",\"value\":\"myvalue\"}" http://localhost:4123/keys
```

### SET WITH EXPIRATION
```bash
curl -X POST -H "Content-Type: application/json" -d "{\"key\":\"mykey\",\"value\":\"myvalue\",\"ttl\":10}" http://localhost:4123/keys
```

### DELETE
```bash
curl -X DELETE http://localhost:4123/keys/mykey
```

### DELETE BY PREFIX
```bash
curl -X DELETE -H "Content-Type: application/json" -d "{\"prefix\":\"my\"}" http://localhost:4123/keys
```

### FLUSH
```bash
curl -X DELETE http://localhost:4123/keys
```

### GET TTL
```bash
curl http://localhost:4123/keys/mykey/ttl
```

### SET TTL
```bash
curl -X POST -H "Content-Type: application/json" -d "{\"key\":\"mykey\",\"ttl\":10}" http://localhost:4123/keys/ttl
```

### DELETE TTL
```bash
curl -X POST -H "Content-Type: application/json" -d "{\"key\":\"mykey, \"ttl\":-1}" http://localhost:4123/keys/ttl
```

## ROADMAP
- [X] Add EXPIRE and TTL operations
- [ ] Add pure in-memory rust backend
- [ ] Support stream protocol (websocks, protobuf, resp?)
