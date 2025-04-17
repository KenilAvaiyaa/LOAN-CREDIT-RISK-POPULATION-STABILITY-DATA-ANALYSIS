# LOAN-CREDIT-RISK-POPULATION-STABILITY-DATA-ANALYSIS
LOAN CREDIT RISK POPULATION STABILITY DATA ANALYSIS USING BIG DATA APPLICATIONS 

# Hadoop MapReduce WordCount Job Instructions

## 1. Set Up Hadoop and Java

Ensure Hadoop is installed and operational in pseudo-distributed or fully distributed mode. Java should also be installed with environment variables correctly set.

## 2. Connect to Master Node

Connect via SSH:

```bash
ssh -i <path_to_key> ubuntu@<master_public_IP>
```

## 3. Start Hadoop Cluster

If starting for the first time, format Namenode:

```bash
hdfs namenode -format
```

Then start HDFS and YARN:

```bash
start-dfs.sh
start-yarn.sh
```

Verify services:

```bash
jps
```

## 4. Upload Input CSV to Hadoop

Use SCP to transfer CSV file:

```bash
scp -i <path_to_key> <local_path_to_csv> ubuntu@<master_IP>:/home/ubuntu/
```

## 5. Create Input Directory in HDFS

```bash
hadoop fs -mkdir /project_input
```

## 6. Copy Input File to HDFS

```bash
hadoop fs -put input.txt /project_input
```

## 7. Java File Setup

To create a Java file:

```bash
vim DefaultRateByTermDriver.java
```

After editing, save and exit Vim:

```
ESC :wq
```

Or upload existing Java file:

```bash
scp -i <path_to_key> <local_path_to_java> ubuntu@<master_IP>:/home/ubuntu/
```

## 8. Compile Java Program

```bash
javac DefaultRateByTermDriver.java -cp $(hadoop classpath)
```

## 9. Create JAR File

```bash
jar cf DefaultRateByTermDriver.jar DefaultRateByTermDriver*.class
```

## 10. Run MapReduce Job

```bash
hadoop jar DefaultRateByTermDriver.jar DefaultRateByTermDriver /project_input /project_output_Default
```

Ensure input/output directories are properly set.

## 11. Retrieve Results

Retrieve from HDFS:

```bash
bin/hdfs dfs -get /project_output_Default
cd project_output_Default
cat part-r-00000
```

## 12. Stop Hadoop Cluster

```bash
stop-yarn.sh
stop-dfs.sh
```

## 13. Download File from Hadoop to Local

```bash
scp -i <path_to_key> ubuntu@<master_IP>:~/hadoop-2.6.5/DefaultRateByTermDriver.jar <local_download_path>
```

## Notes
- Ensure Hadoop services are running before executing.
- Delete existing output directories before running a job:

```bash
hadoop fs -rm -r /output_directory_name
```

- Ensure your input file is formatted correctly (one name per line or space-separated).
- Use a new terminal window for SCP operations.
