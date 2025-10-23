# MapReduce Framework

A distributed MapReduce framework implementation in Python, inspired by Google's original MapReduce paper. Built as part of EECS 485 (Web Systems) at the University of Michigan.

## 🎯 Project Overview

This project implements a complete MapReduce framework that executes distributed data processing jobs across a cluster of machines. The system consists of a Manager that coordinates work distribution and multiple Worker processes that execute map and reduce tasks in parallel.

## ✨ Key Features

- **Distributed Processing**: Manager coordinates multiple Worker processes
- **Fault Tolerance**: Automatic task reassignment when Workers fail
- **Heartbeat Monitoring**: UDP-based health checks every 2 seconds
- **Concurrent Execution**: Multi-threaded architecture for parallel task processing
- **Job Queue Management**: Handles multiple sequential MapReduce jobs
- **TCP/UDP Communication**: Robust networking with JSON message protocol
- **Memory Efficient**: Streaming data processing with O(1) memory usage
- **Dynamic Partitioning**: Hash-based data distribution across reducers

## 🏗️ Architecture

### System Components

```
┌─────────────┐
│   Client    │
│ (submit.py) │
└──────┬──────┘
       │ TCP: new_job
       ↓
┌─────────────┐          UDP: heartbeat
│   Manager   │ ←─────────────────────┐
│   (Port     │                       │
│    6000)    │                       │
└──────┬──────┘                       │
       │ TCP: tasks                   │
       ├──────────────┬───────────────┤
       ↓              ↓               ↓
  ┌─────────┐   ┌─────────┐    ┌─────────┐
  │Worker 1 │   │Worker 2 │    │Worker N │
  │ (6001)  │   │ (6002)  │    │ (600N)  │
  └─────────┘   └─────────┘    └─────────┘
```

### Manager Responsibilities
- Listens on TCP port 6000 for job submissions
- Listens on UDP port 6000 for Worker heartbeats
- Partitions input files into map tasks
- Distributes tasks to available Workers
- Monitors Worker health and reassigns failed tasks
- Manages job queue and execution flow
- Coordinates map and reduce stages

### Worker Responsibilities
- Registers with Manager on startup
- Sends heartbeat messages every 2 seconds
- Executes map tasks: run mapper, partition output, sort results
- Executes reduce tasks: merge sorted inputs, run reducer
- Reports task completion to Manager
- Handles graceful shutdown

## 🔄 MapReduce Execution Flow

### Map Stage
1. **Input Partitioning**: Manager divides input files using round-robin
2. **Task Distribution**: Manager assigns map tasks to Workers by registration order
3. **Map Execution**: Worker runs mapper executable on input files
4. **Output Partitioning**: Worker hashes keys to partition output for reducers
5. **Sorting**: Worker sorts each partition file using UNIX sort
6. **File Transfer**: Worker moves sorted files to shared directory

### Reduce Stage
1. **Input Grouping**: Manager groups map outputs by partition number
2. **Task Distribution**: Manager assigns reduce tasks to Workers
3. **Input Merging**: Worker merges sorted input files using heapq
4. **Reduce Execution**: Worker streams merged input to reducer executable
5. **Output Generation**: Worker writes final output to specified directory

## 🛠️ Technology Stack

- **Language**: Python 3.8+
- **Concurrency**: Threading (Python threading module)
- **Networking**: TCP/UDP Sockets (Python socket module)
- **Data Processing**: Subprocess execution with pipes
- **Message Format**: JSON
- **Sorting**: UNIX sort utility
- **Logging**: Python logging module

## 📁 Project Structure

```
p4-mapreduce/
├── bin/
│   └── mapreduce          # Init script (start/stop/status/restart)
├── mapreduce/
│   ├── __init__.py
│   ├── manager/
│   │   ├── __init__.py
│   │   └── __main__.py    # Manager implementation
│   ├── worker/
│   │   ├── __init__.py
│   │   └── __main__.py    # Worker implementation
│   ├── submit.py          # Job submission client
│   └── utils/
│       ├── __init__.py
│       └── ordered_dict.py # Thread-safe dictionary
├── tests/
│   ├── testdata/
│   │   ├── exec/          # Sample map/reduce programs
│   │   ├── input/         # Test input files
│   │   └── correct/       # Expected outputs
│   └── test_*.py          # Unit tests
├── requirements.txt
└── pyproject.toml
```

## 🚀 Getting Started

### Prerequisites

- Python 3.8 or higher
- UNIX/Linux/macOS environment
- pip package manager

### Installation

1. Clone the repository
```bash
git clone https://github.com/YOUR_USERNAME/p4-mapreduce.git
cd p4-mapreduce
```

2. Create and activate virtual environment
```bash
python3 -m venv env
source env/bin/activate
```

3. Install dependencies
```bash
pip install -r requirements.txt
pip install -e .
```

### Running the MapReduce Framework

#### Using the Init Script (Recommended)

Start the MapReduce server (1 Manager + 2 Workers):
```bash
./bin/mapreduce start
```

Check server status:
```bash
./bin/mapreduce status
```

Stop the server:
```bash
./bin/mapreduce stop
```

Restart the server:
```bash
./bin/mapreduce restart
```

#### Manual Startup (For Debugging)

Start Manager:
```bash
mapreduce-manager --loglevel=DEBUG
```

Start Workers (in separate terminals):
```bash
mapreduce-worker --port 6001 --loglevel=DEBUG
mapreduce-worker --port 6002 --loglevel=DEBUG
```

### Submitting a MapReduce Job

Basic word count example:
```bash
mapreduce-submit \
  --input tests/testdata/input_small \
  --output output \
  --mapper tests/testdata/exec/wc_map.sh \
  --reducer tests/testdata/exec/wc_reduce.sh
```

Custom configuration:
```bash
mapreduce-submit \
  --input tests/testdata/input \
  --output output \
  --mapper tests/testdata/exec/wc_map.py \
  --reducer tests/testdata/exec/wc_reduce.py \
  --nmappers 4 \
  --nreducers 3
```

### Viewing Results

```bash
# View individual output files
head output/part-*

# View combined sorted output
cat output/part-* | sort
```

## 📊 Communication Protocol

### Message Types

#### Worker → Manager
- `register`: Worker registration on startup
- `heartbeat`: Periodic health check (every 2 seconds)
- `finished`: Task completion notification

#### Manager → Worker
- `register_ack`: Registration acknowledgment
- `new_map_task`: Map task assignment
- `new_reduce_task`: Reduce task assignment
- `shutdown`: Graceful shutdown signal

#### Client → Manager
- `new_manager_job`: Job submission
- `shutdown`: Server shutdown request

### Sample Messages

**Worker Registration:**
```json
{
  "message_type": "register",
  "worker_host": "localhost",
  "worker_port": 6001
}
```

**Map Task Assignment:**
```json
{
  "message_type": "new_map_task",
  "task_id": 0,
  "input_paths": ["file01", "file02"],
  "executable": "wc_map.sh",
  "output_directory": "/tmp/job-00000",
  "num_partitions": 2
}
```

## 🔒 Fault Tolerance

### Worker Failure Detection
- Manager expects heartbeat every 2 seconds
- Worker marked as **dead** after missing 5 consecutive heartbeats (10 seconds)
- Tasks reassigned to healthy Workers automatically

### Task Reassignment Strategy
1. Complete all unassigned tasks in current stage first
2. Reassign tasks from dead Workers to available Workers
3. Maintain task assignment order for deterministic behavior
4. Handle Worker revival and re-registration gracefully

### Connection Handling
- `ConnectionRefusedError` triggers immediate Worker death marking
- Manager continues execution even if all Workers die temporarily
- System resumes when new Workers register

## 🧪 Testing

Run all tests:
```bash
pytest -v
```

Run with detailed logging:
```bash
pytest -vvsx --log-cli-level=INFO
```

Run specific test:
```bash
pytest -vvsx --log-cli-level=INFO tests/test_manager_01.py
```

### Test Coverage
- Manager shutdown and Worker registration
- Job submission and execution
- Input partitioning and task distribution
- Map and reduce task execution
- Fault tolerance and Worker failure scenarios
- Multi-job queue management

### Fault Tolerance Testing

Test with intentionally slow jobs:
```bash
./bin/mapreduce start
mapreduce-submit \
  --mapper tests/testdata/exec/wc_map_slow.sh \
  --reducer tests/testdata/exec/wc_reduce_slow.sh

# Kill a Worker while job is running
pgrep -f mapreduce-worker | head -n1 | xargs kill

# Check logs to verify task reassignment
grep 'Received task' var/log/worker-*.log
```

## 🎯 Design Highlights

### Memory Efficiency
- **Streaming Processing**: Map output piped directly to partitioning code
- **O(1) Memory**: No buffering of entire datasets
- **Lazy Evaluation**: heapq.merge() for memory-efficient sorted merging

### Concurrency
- **Multi-threading**: Separate threads for TCP/UDP servers and heartbeat monitoring
- **Thread-safe Data Structures**: Custom OrderedDict for concurrent access
- **Non-blocking I/O**: Efficient socket communication

### Scalability
- **Horizontal Scaling**: Support for arbitrary number of Workers
- **Dynamic Partitioning**: Hash-based distribution ensures load balancing
- **Shared File System**: Temporary directories for distributed intermediate data

## 📈 Performance Characteristics

- **Startup Time**: ~2 seconds (Manager + 2 Workers)
- **Task Assignment**: O(1) per task
- **Worker Failure Detection**: 10 seconds maximum
- **Memory Usage**: O(1) relative to data size (streaming)
- **Throughput**: Scales linearly with number of Workers

## 🎓 Learning Outcomes

This project demonstrates proficiency in:

- **Distributed Systems**: Multi-process coordination, fault tolerance
- **Network Programming**: TCP/UDP sockets, JSON messaging
- **Concurrency**: Multi-threading, thread safety, race condition avoidance
- **Systems Programming**: Process management, file I/O, subprocess execution
- **Software Design**: Modular architecture, separation of concerns
- **Testing**: Unit testing with mocking, integration testing

## 📚 Technical Deep Dive

### Hash-based Partitioning

```python
import hashlib

def compute_partition(key, num_partitions):
    hexdigest = hashlib.md5(key.encode("utf-8")).hexdigest()
    keyhash = int(hexdigest, base=16)
    return keyhash % num_partitions
```

### Streaming Map Output

```python
with subprocess.Popen(
    [mapper_executable],
    stdin=input_file,
    stdout=subprocess.PIPE,
    text=True,
) as process:
    for line in process.stdout:
        # Process line-by-line without buffering
        partition = compute_partition(key, num_partitions)
        write_to_partition(line, partition)
```

### Merging Sorted Reduce Inputs

```python
import heapq

input_files = [open(f) for f in sorted_input_paths]
merged_stream = heapq.merge(*input_files)

with subprocess.Popen(
    [reducer_executable],
    stdin=subprocess.PIPE,
    stdout=output_file,
    text=True,
) as process:
    for line in merged_stream:
        process.stdin.write(line)
```

## 🐛 Debugging Tips

### Watch logs in real-time
```bash
tail -F var/log/manager.log
tail -F var/log/worker-6001.log
```

### Check for busy-waiting
```bash
time mapreduce-manager
# Good: user time << real time
# Bad: user time ≈ real time (indicates busy-waiting)
```

### Verify message flow
```bash
# Filter logs for specific message types
grep "message_type" var/log/*.log
```

## ⚠️ Common Pitfalls Avoided

- ✅ Thread-safe data structures for concurrent access
- ✅ Graceful handling of `ConnectionRefusedError`
- ✅ Proper socket cleanup with context managers
- ✅ Avoiding busy-waiting with blocking operations
- ✅ Correct JSON encoding of Path objects
- ✅ Handling Worker re-registration scenarios

## 🔒 Academic Integrity

This project was completed as part of EECS 485 at the University of Michigan. The code represents my own work and understanding of distributed systems and MapReduce concepts.

**Note**: If you are currently taking EECS 485, please adhere to the course's academic integrity policies. This repository is intended as a portfolio piece and learning reference.

## 📚 Resources

- [Project Specification](https://eecs485staff.github.io/p4-mapreduce/)
- [Google MapReduce Paper](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf)
- [Python Threading Documentation](https://docs.python.org/3/library/threading.html)
- [Python Socket Programming](https://docs.python.org/3/library/socket.html)
- [heapq Module Documentation](https://docs.python.org/3/library/heapq.html)

## 📧 Contact

Your Name - [GitHub](https://github.com/YOUR_USERNAME)

---

**Built with 🔧 at the University of Michigan**
