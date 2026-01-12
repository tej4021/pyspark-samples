# PySpark ETL Project

This repository contains PySpark scripts for ETL workflows for below. This README provides setup instructions, environment setup, and guidelines for running the scripts.
1) Slowly Changing Dimension (SCD) Type 2 processing
2) NoSql/Document data processing with spark
---

## 1. Environment Setup

We recommend using **Anaconda** for managing Python environments.

```
# Create a new conda environment
conda create -n pyspark_etl_setup python=3.10 -y

# Activate the environment
conda activate pyspark_etl_setup

# Install PySpark
pip install pyspark
```

---

## 2. Project Folder

Navigate to the project folder where your scripts are located:

```
cd <path-to-project-folder>
# Example:
cd C:\Users\HP\OneDrive\Desktop\pyspark_samples
```

---

## 3. Environment Variables (Windows)

Set the required environment variables:

```
# Python executable in the conda environment
set PYSPARK_PYTHON=C:\Users\HP\anaconda3\envs\pyspark_etl_setup\python.exe

# Hadoop home directory
set HADOOP_HOME=C:\hadoop
```

> Adjust paths according to your system and conda environment.

---

## 4. Logging

Each script creates a `logs` directory by default. Log files are timestamped for each run.

**Example from current SCD Type 2 script:**

```python
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

log_filename = f"<log_file_name>_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log_path = os.path.join(LOG_DIR, log_filename)

logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)
logger.info(f"Starting SCD Type 2 PySpark Job. Log file: {log_filename}")
```

- Log files will be created in `logs/` folder.
- Change the log file name as per the script -> scd_type2_job_
- Example log filename: `logs/scd_type2_job_20260111_153000.log`

---

## 5. Running PySpark Scripts

You can run the scripts either from **Anaconda Prompt**

### From Anaconda Prompt

```bash
spark-submit scripts/<filename>.py
- Example -> spark-submit scripts/SlowChangingDemPySpark.py
```
- This script reads source data, applies SCD Type 2 logic, and writes the results to the target table.
- Logging captures start time, progress, and completion messages.

---

## 7. Notes

- Ensure Hadoop is installed if using Spark with HDFS.
- Python version 3.10 is recommended.
- Conda environment name and paths can be changed; adjust environment variables accordingly.

---

This README acts as a **single reference point** for environment setup, running scripts, and checking logs.

