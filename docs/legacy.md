# WSL PySpark Setup Guide - No-Fail Edition

## The Problem

PySpark on Windows fails due to missing Hadoop native libraries. WSL (Windows Subsystem for Linux) provides a Linux environment where PySpark works natively, but requires proper configuration.

## Prerequisites Check

Open **WSL terminal** (Ubuntu) and run these checks:

### 1. Check Java
```bash
java -version
```

**Expected**: `openjdk version "11.0.x"`  
**If not found**: Install Java (see Step 1 below)

### 2. Check Python
```bash
python3 --version
```

**Expected**: `Python 3.8+`  
**If not found**: `sudo apt install python3 python3-pip -y`

### 3. Check PySpark
```bash
python3 -c "import pyspark; print(pyspark.__version__)"
```

**Expected**: `3.5.0` or similar  
**If error**: Install PySpark (see Step 2 below)

---

## Step-by-Step Setup

### Step 1: Install Java 11

```bash
sudo apt update
sudo apt install openjdk-11-jdk -y
```

**Verify**:
```bash
java -version
# Should show: openjdk version "11.0.x"
```

### Step 2: Install Python Dependencies

```bash
# Ensure pip is up to date
sudo apt install python3-pip python3-venv -y

# Create WSL venv (if not already created)
cd /mnt/c/Users/Fnuas/Desktop/SPRING_2026/interview\ prep/proejct_proto/poc
python3 -m venv .venv-wsl

# Activate venv
source .venv-wsl/bin/activate

# Install all dependencies
pip install --upgrade pip
pip install -r requirements.txt
```

### Step 3: Configure Environment Variables

Open your `.bashrc` file:
```bash
nano ~/.bashrc
```

**Scroll to the bottom** and add these lines:

```bash
# ============================================
# PySpark Configuration for Streaming POC
# ============================================

# Java Home
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

# PySpark Python
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3

# Add local bin to PATH
export PATH=$PATH:~/.local/bin

# Optional: Suppress Hadoop warnings
export HADOOP_HOME=/usr/local/hadoop
export SPARK_LOCAL_IP=127.0.0.1
```

**Save and exit**: Press `Ctrl+O`, `Enter`, then `Ctrl+X`

**Apply changes**:
```bash
source ~/.bashrc
```

### Step 4: Verify PySpark Installation

Test that PySpark can start:
```bash
python3 -c "from pyspark.sql import SparkSession; spark = SparkSession.builder.master('local[*]').getOrCreate(); print('✓ PySpark works!'); spark.stop()"
```

**Expected output**: `✓ PySpark works!`

### Step 5: Test Kafka Connector

The streaming job needs the Kafka connector JAR. Test it:

```bash
cd /mnt/c/Users/Fnuas/Desktop/SPRING_2026/interview\ prep/proejct_proto/poc
source .venv-wsl/bin/activate

python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .master('local[*]') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1') \
    .getOrCreate()
print('✓ Kafka connector loaded!')
spark.stop()
"
```

**Expected**: `✓ Kafka connector loaded!` (may take 30 seconds to download JARs first time)

---

## Critical Fix: Windows Kafka → WSL Connection

Your Kafka is running on **Windows Docker**, but WSL sees it on a different network.

### Find Your Windows IP

**In Windows PowerShell**, run:
```powershell
ipconfig
```

Look for **"Ethernet adapter vEthernet (WSL)"** or **"WiFi"** section:
```
IPv4 Address. . . . . . . . . . . : 192.168.1.100
                                     ^^^^^^^^^^^^^^
                                     Copy this IP
```

### Update Kafka Broker in Config

**Option A**: Update `app_wsl.yaml`:
```bash
cd /mnt/c/Users/Fnuas/Desktop/SPRING_2026/interview\ prep/proejct_proto/poc
nano configs/app_wsl.yaml
```

Change line 2:
```yaml
kafka:
  brokers: "192.168.1.100:9092"  # ← Use YOUR Windows IP
  topic: "card-events"
```

**Option B**: Use special WSL hostname (easier):
```yaml
kafka:
  brokers: "host.docker.internal:9092"  # This should work if Docker is configured correctly
  topic: "card-events"
```

---

## Running the Complete Pipeline in WSL

### Terminal 1 (Windows PowerShell): Start Kafka
```powershell
cd "c:\Users\Fnuas\Desktop\SPRING_2026\interview prep\proejct_proto\poc"
docker compose up -d
```

### Terminal 2 (Windows PowerShell): Start Simulator
```powershell
cd "c:\Users\Fnuas\Desktop\SPRING_2026\interview prep\proejct_proto\poc"
& "..\.venv\Scripts\python.exe" -m src.simulator.main --config configs/app_wsl.yaml
```

### Terminal 3 (WSL): Start Spark Streaming Job
```bash
cd /mnt/c/Users/Fnuas/Desktop/SPRING_2026/interview\ prep/proejct_proto/poc

# Activate venv
source .venv-wsl/bin/activate

# Load Snowflake credentials
set -a
source configs/snowflake.env
set +a

# Run streaming job
python3 -m src.streaming.streaming_job --config configs/app_wsl.yaml
```

**Expected output**:
```
Setting default log level to "WARN".
[BATCH 0000] all_scores local rows: 5
[BATCH 0000] sent 5 rows to Snowflake (CHURN_SCORES_RAW)
[BATCH 0001] all_scores local rows: 8
...
```

---

## Troubleshooting

### Error: "java.net.ConnectException: Connection refused"
**Cause**: WSL can't reach Kafka on Windows  
**Fix**: Update `app_wsl.yaml` with your Windows IP (see "Critical Fix" section above)

### Error: "JAVA_HOME is not set"
**Cause**: Environment variables not loaded  
**Fix**: Run `source ~/.bashrc` and verify with `echo $JAVA_HOME`

### Error: "No module named 'pyspark'"
**Cause**: Wrong Python or venv not activated  
**Fix**: 
```bash
source .venv-wsl/bin/activate
pip install pyspark
```

### Error: "Failed to find data source: kafka"
**Cause**: Kafka connector JAR not loaded  
**Fix**: The `streaming_job.py` already includes the JAR in `spark.jars.packages` config - this should auto-download on first run

### Kafka messages not appearing in Spark
**Test Kafka connectivity from WSL**:
```bash
# Install kafkacat (Kafka debugging tool)
sudo apt install kafkacat -y

# Test connection (replace with your Windows IP)
kafkacat -b 192.168.1.100:9092 -L

# Consume messages
kafkacat -b 192.168.1.100:9092 -t card-events -C -c 5
```

---

## Verification Checklist

Before running the pipeline, verify:

- [ ] Java installed: `java -version` shows 11.x
- [ ] PySpark works: Test command runs without errors
- [ ] Kafka connector loads: Test command downloads JARs successfully
- [ ] WSL can reach Kafka: `kafkacat` shows messages
- [ ] Snowflake credentials loaded: `echo $SNOWFLAKE_ACCOUNT` shows value
- [ ] Venv activated: Prompt shows `(.venv-wsl)`

---

## Quick Test Script

Save this as `test_wsl_setup.sh` and run it to verify everything:

```bash
#!/bin/bash
echo "=== WSL PySpark Setup Verification ==="
echo ""

echo "1. Checking Java..."
java -version 2>&1 | head -1

echo ""
echo "2. Checking Python..."
python3 --version

echo ""
echo "3. Checking PySpark..."
python3 -c "import pyspark; print(f'PySpark {pyspark.__version__}')" 2>/dev/null || echo "❌ PySpark not found"

echo ""
echo "4. Checking JAVA_HOME..."
echo "JAVA_HOME=$JAVA_HOME"

echo ""
echo "5. Checking Kafka connectivity..."
nc -zv host.docker.internal 9092 2>&1 | grep -q succeeded && echo "✓ Kafka reachable" || echo "❌ Kafka not reachable"

echo ""
echo "=== Setup verification complete ==="
```

Run it:
```bash
chmod +x test_wsl_setup.sh
./test_wsl_setup.sh
```

---

## Next Steps

Once all checks pass, you're ready to run the full pipeline! Follow the "Running the Complete Pipeline in WSL" section above.
