# 🗄️ kv-db

 **in-memory key-value database** written in Go. This project was written in order to get in touch with modern database architecture with persistence, replication, and network protocols.

## ✨ Features

### 🚀 Core Functionality
- **In-Memory Storage**: Fast key-value operations with sharded concurrent access based on runtime.NumCPU()
- **TCP Protocol**: Simple text-based commands over TCP for easy integration
- **Interactive Client**: Feature-rich CLI client with autocomplete and command history

### 💾 Persistence & Durability
- **Write-Ahead Log (WAL)**: Ensures data durability with configurable batching
- **Crash Recovery**: Automatic recovery from WAL on startup
- **Segmented Storage**: Efficient file management with configurable segment sizes

### 🔄 Replication
- **Logical Asynchronous Push Replication**: Real-time data replication using streaming WAL entries
- **Master-Slave Architecture**: Slaves automatically sync with master on connection, sending their last applied LSN
- **Batch Synchronization**: Efficient log shipping with configurable batch sizes


### ⚙️ Configuration & Operations
- **YAML Configuration**: Flexible configuration for all components
- **Graceful Shutdown**: Clean shutdown with proper resource cleanup
- **Comprehensive Logging**: Structured logging with configurable levels
- **Concurrent Safety**: Thread-safe operations with optimized locking

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐
│  TCP Client     │    │   TCP Server    │
│                 │    │                 │
│ • Interactive   │◄──►│ • Multi-client  │
│ • Autocomplete  │    │ • Connection    │
│ • History       │    │   pooling       │
└─────────────────┘    └─────────────────┘
                               │
                               ▼
                    ┌─────────────────┐
                    │    Database     │
                    │                 │
                    │ • Command Parser│
                    │ • Query Engine  │
                    └─────────┬───────┘
                              │
                              ▼
        ┌─────────────────────┴─────────────────────┐
        │                Storage                    │
        │                                           │
        │  ┌─────────────┐    ┌─────────────────┐   │
        │  │   Engine    │    │       WAL       │   │
        │  │             │    │                 │   │
        │  │ • In-Memory │    │ • Batching      │   │
        │  │ • Sharded   │    │ • Persistence   │   │
        │  │   Hash Map  │    │ • Recovery      │   │
        │  │ • Per shard │    │                 │   │
        │  │   locking   │    │                 │   │ 
        │  └─────────────┘    └─────────────────┘   │
        └───────────────────────────────────────────┘
                              │
                              ▼
                    ┌─────────────────┐
                    │   Replication   │
                    │                 │
                    │ • Async Push    │
                    │ • Master/Slave  │
                    │ • Auto-sync     │
                    └─────────────────┘
```

## 🚀 Quick Start

### Prerequisites
- **Go 1.24.3+**
- **Unix-like system** (Linux, macOS)

### 1. Clone and Build
```bash
git clone https://github.com/EzhovAndrew/kv-db.git
cd kv-db
go mod download
```

### 2. Start the Database Server
```bash
# Start with default configuration
go run cmd/server/main.go

# Or specify custom config
CONFIG_FILEPATH=./config.yaml go run cmd/server/main.go
```

The server will start on `127.0.0.1:3223` by default.

### 3. Connect with the Interactive Client
```bash
# Connect to local server
go run cmd/client/main.go

# Or connect to remote server
go run cmd/client/main.go -host 192.168.1.100 -port 3223
```

## 💻 Usage Examples

### Interactive Client Session
```bash
$ go run cmd/client/main.go
KV-DB Client
Type 'HELP' for available commands or 'EXIT'/'QUIT' to quit

kv-db> SET user:1 "John Doe"
OK

kv-db> GET user:1
Value: John Doe

kv-db> SET counter 42
OK

kv-db> DEL counter
OK

kv-db> GET counter
Value: key not found

kv-db> HELP
Available commands:

  DEL      Delete key-value pair
  EXIT     Exit the client
  GET      Retrieve value by key
  HELP     Show available commands
  QUIT     Exit the client
  SET      Set key-value pair

Use 'HELP <command>' for detailed information about a specific command
```

### Available Commands

| Command | Description | Usage | Example |
|---------|-------------|--------|---------|
| `GET` | Retrieve value by key | `GET <key>` | `GET user:123` |
| `SET` | Store key-value pair | `SET <key> <value>` | `SET name "Alice"` |
| `DEL` | Delete key-value pair | `DEL <key>` | `DEL temp_data` |
| `HELP` | Show command help | `HELP [command]` | `HELP SET` |
| `EXIT/QUIT` | Exit the client | `EXIT` | `EXIT` |

## ⚙️ Configuration

### Server Configuration (`config.yaml`)
```yaml
# Storage engine configuration
engine:
  type: "in_memory"

# Network settings
network:
  ip: "127.0.0.1"
  port: 3223
  max_connections: 100
  max_message_size: 4096
  idle_timeout: 300
  graceful_shutdown_timeout: 5

# Logging configuration
logging:
  level: "info"        # debug, info, warn, error, fatal
  output: "stdout"

# Write-Ahead Log settings
wal:
  flush_batch_size: 100
  flush_batch_timeout: 10
  max_segment_size: 10485760    # 10MB
  data_directory: "/tmp/kv_db/wal"

# Replication configuration
replication:
  role: "master"              # master or slave
  master_address: "127.0.0.1"
  master_port: 3333
```

### Slave Configuration (`config_slave.yaml`)
```yaml
# Copy master config and modify:
network:
  port: 3224  # Different port for slave

wal:
  data_directory: "/tmp/kv_db/wal_slave"

replication:
  role: "slave"
  master_address: "127.0.0.1"
  master_port: 3333
  slave_id: "slave1"
```

## 🔄 Replication Setup

### 1. Start Master Server
```bash
go run cmd/server/main.go
```

### 2. Start Slave Server
```bash
CONFIG_FILEPATH=./config_slave.yaml go run cmd/server/main.go
```

The slave will automatically:
- Connect to the master
- Sync existing data
- Receive real-time updates
- Handle reconnection on failures

### 3. Verify Replication
```bash
# Connect to master and add data
go run cmd/client/main.go -port 3223
kv-db> SET replicated_key test_value

# Connect to slave and verify
go run cmd/client/main.go -port 3224
kv-db> GET replicated_key
Value: test_value
```

## 🧪 Running Tests

### Run All Unit Tests
```bash
# Using the provided script
chmod +x unit_tests.sh
./unit_tests.sh
```

### Run Specific Package Tests
```bash
# Test storage components
go test ./internal/database/storage/...

# Test replication
go test ./internal/replication/...

# Test with verbose output
go test -v ./internal/database/storage/engine/in_memory/
```

### Test Coverage
```bash
# Generate coverage report
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out -o coverage.html
```

## 🛠️ Development

### Project Structure
```
kv-db/
├── cmd/                    # Application entry points
│   ├── client/             # Interactive client
│   └── server/             # Database server
├── internal/               # Internal packages
│   ├── concurrency/        # Thread-safe utilities
│   ├── configuration/      # Config management
│   ├── database/           # Core database logic
│   │   ├── compute/        # Query parsing
│   │   └── storage/        # Storage engine & WAL
│   ├── logging/            # Structured logging
│   ├── network/            # TCP server/client
│   ├── replication/        # Master-slave replication
│   └── utils/              # Common utilities
└── config*.yaml           # Configuration files
```

### Key Components

- **Database**: Main database interface and request handling
- **Storage Engine**: In-memory sharded hash map with concurrent access, partitioned by runtime.NumCPU()
- **WAL**: Write-ahead logging with batching and recovery
- **Replication**: Master-slave logical asynchronous push replication 
- **Network**: TCP protocol implementation
- **Client**: Interactive CLI with rich features

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📝 License

This project is a personal learning project demonstrating database internals and distributed systems concepts. 

**Feel free to use this code for learning, experimentation, or any other purpose!** 

I would greatly appreciate any feedback about the code quality, architecture decisions, or suggestions for improvements. If you find bugs, have ideas for optimizations, or spot areas where the code could be cleaner, please don't hesitate to:

- Open an issue to discuss improvements
- Submit a pull request with enhancements
- Reach out with general feedback or questions

Your input helps me grow as a developer! 🚀 

## 🎯 Roadmap

- [ ] Integration tests
- [ ] SDK
- [X] Sharded hash map in engine
- [ ] WAL segments compaction
- [ ] Query language extensions
- [ ] Metrics and monitoring
- [ ] Clustering support
- [ ] REST API interface
