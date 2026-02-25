# Fraud Detection System using Kafka and Flink in Confluent Cloud
In this workshop, you will learn how to create a fraud detection system in Confluent Cloud. The data will be stored in a Kafka topic and the stream processing will be performed by Flink.
  
## Section 1 - Login to Confluent Cloud
1. Log in to [Confluent Cloud](https://confluent.cloud) and enter your email and password. If you don't have an account you can sign up [here](https://www.confluent.io/confluent-cloud/tryfree/).

## Section 2 - Create a Confluent Cloud Environment
In this step, we will create a new Environment.

A Confluent Cloud environment is a logical grouping within your Confluent Cloud organization that serves as a container for your Kafka clusters and related components such as Connect, ksqlDB, Schema Registry, and Flink compute pools.

You can create multiple environments within a single organization at no additional cost. This setup allows different teams or departments to work in separate environments, ensuring isolation and preventing interference between workloads.

Steps to create an environment:
1. On the Confluent Cloud home screen, click "Environments" in the left-hand menu.
2. Click the "Add cloud environment" button on the right side of the page.
3. Enter a name for your environment. For this workshop, we’ll use "demo_environment".
4. For the Stream Governance package, select "Essentials".
5. Click "Create" to finish setting up your environment.

## Section 3 - Create a Confluent Cloud Cluster
Once you have created an environment, you can proceed to create a Confluent Cloud cluster.

A Confluent Cloud cluster is a fully managed Apache Kafka cluster that runs in the cloud and is hosted and maintained by Confluent. it's where your Kafka topics, producers, and consumers live and interact. The cluster handles all the data streaming activity, such as: producing, storing, and consuming messages in real time.

We'll create a Confluent Cloud cluster with the following specifications:
- Cluster name: fraud_detection_cluster
- Cluster type: Basic
- Cloud provider: AWS
- Cloud provider region: Jakarta (ap-southeast-3)

To create the cluster you can follow these steps below:
1. Click on your newly created environment to open it.
2. In the left-hand menu, select "Clusters".
3. Click the "Add cluster" button on the right side of the page.
4. Enter a name for your cluster. For this workshop, we’ll use "fraud_detection_cluster".
5. Click the "Basic" cluster type.
6. Choose your preferred cloud provider. For this workshop, we'll use "AWS", and for the region, we'll choose Jakarta.
7. Click the "Launch cluster" button to create your cluster.

## Section 4 - Create a "Sample Data Datagen Source" connector
After creating a Confluent Cloud cluster, we'll generate some sample data using "Sample Data Datagen Source" connector.

To create the connector, follow the steps below:
1. Click the previously provisioned Confluent Cloud cluster.
2. In the left-hand menu, select "Connectors".
3. Click the "Sample Data Datagen Source" button.
4. Click the "Additional configuration" link.
5. Click on the "Add a new topic" button.
6. Write "transactions_topic" in the "Topic name" text box and write "1" in the "Partitions" text box.
7. Click the "Create with defaults" button.
8. Click on the "Continue" button.
9. Click the "Generate API key and download" button to generate a new API key and API secret.
10. Below the "Select output record value format" section, choose "AVRO".
11. For the "Select a schema" section, click on the "Provide your own schema" button.
12. Paste the following schema query into an empty cell:
```{
  "type": "record",
  "name": "transactionstream",
  "namespace": "transaction.demo",
  "fields": [
    {
      "name": "transaction_id",
      "type": {
        "type": "int",
        "arg.properties": {
          "range": { "min": 0, "max": 1000000, "step": 1 }
        }
      }
    },
    {
      "name": "timestamp",
      "type": {
        "type": "long",
        "arg.properties": {
          "iteration": { "start": 1731800000000, "step": 1000 }
        }
      }
    },
    {
      "name": "card_id",
      "type": {
        "type": "string",
        "arg.properties": {
          "options": [
            "101", "102", "103",
            "201", "202",
            "301", "302",
            "401",
            "501", "502"
          ]
        }
      }
    },
    {
      "name": "latitude",
      "type": {
        "type": "float",
        "arg.properties": {
          "options": [-6.1944, 3.5952, -8.4095]
        }
      }
    },
    {
      "name": "longitude",
      "type": {
        "type": "float",
        "arg.properties": {
          "options": [106.8157, 98.6722, 115.1889]
        }
      }
    },
    {
      "name": "amount_idr",
      "type": {
        "type": "float",
        "arg.properties": {
          "range": { "min": 10000.0, "max": 50000000.0 }
        }
      }
    }
  ]
}
```
5. Click the "Show advanced configurations" link below the schema text box.
6. Change the "Max interval between messages (ms)" to "10000", then click on the "Continue" button at the bottom right of the screen.
7. Leave the number of connector tasks as "1", then click the "Continue" button at the bottom right of the screen.
8. Write "transaction_data_connector" in the "Connector name" text box, then click the "Continue" button at the bottom right of the screen.

## Section 5 - Create a Flink Compute Pool
Now that the "transactions_topic" topic is filled with messages, we can start creating a Flink compute pool.

A Flink compute pool in Confluent Cloud for Apache Flink represents a set of compute resources bound to a region that is used to run your SQL statements. The resources provided by a compute pool are shared between all statements that use it. The capacity of a compute pool is measured in CFUs.
To create an Flink compute pool, you can follow the steps below:
1. In the left-hand menu, select "Flink".
2. Click the "Add compute pool" button on the right side of the page.
3. Choose your preferred cloud provider. For this workshop, we'll use "Azure", and for the region, we'll choose Jakarta. Please note that the Flink compute pool must match the Kafka cluster's region.
4. Enter a name for your compute pool. For this workshop, we’ll use "fraud_detection_flink_compute_pool".
5. Set the "Max size" value to 10 CFU.
6. Click the "Create" button to create your compute pool.

## Section 6 - Prepare a Flink Workspace
Once the Flink compute pool has been provisioned, you can setup a Flink SQL workspace where you will be writing your Flink queries. Follow the steps below to get started:
1. Click the "Open SQL workspace" button on the right side of the page.
2. Open the "Catalog" dropdown in the top-right corner and select "demo_environment" to access your previously provisioned environment.
3. Open the "Database" dropdown in the top right corner and choose "fraud_detection_cluster" to access your previously provisioned Kafka cluster.

## Section 7 - Transforming a Kafka Topic into a Flink-managed Table:
In this section, we will transform the "transactions_topic" Kafka topic into a Flink-managed table. Raw Kafka topics such as the "transactions_topic" needs to be transformed into Flink-managed tables with proper primary keys, event-time processing, and watermarks to ensure that downstream operations, such as joins, aggregations, and windowed analyses, work correctly. To do that, follow the steps below:
1. Paste the following Flink query into an empty cell to create a new table called the "transactions_topic_rekeyed":
```
CREATE TABLE transactions_topic_rekeyed (
  transaction_id INT NOT NULL PRIMARY KEY NOT ENFORCED,
  card_id STRING,
  latitude FLOAT,
  longitude FLOAT,
  amount_idr FLOAT,
  event_time TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) DISTRIBUTED BY (transaction_id) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'append'
);
```
2. Insert the existing data from "transactions_topic" into the new "transactions_topic_rekeyed" table using the following Flink query:
```
INSERT INTO transactions_topic_rekeyed
SELECT
  transaction_id,
  card_id,
  latitude,
  longitude,
  amount_idr,
  $rowtime as event_time
FROM transactions_topic;
```

## Section 8 - Perform Fraud Detection
Once you have populated the "transactions_topic_rekeyed" topic, you can proceed to deploy the fraud detection queries.

In this section, we will flag potentially fraudulent transactions using four methods:
- High-Value Threshold Rule
- Spending Pattern Rule
- Improbable Travel Distance Rule
- Combined Anomaly Rule

### Section 8.1 - Perform Fraud Detection using the High-Value Threshold Rule
The first and simplest way to flag a potentially fraudulent transaction is to mark every transaction that exceeds a certain monetary threshold. In the example below, I created a Flink query to flag every transaction above 10 million Indonesian Rupiah and send it to a separate topic called "simple_fraud_detection":  
```
--Mark the transaction as fraud if the transaction amount exceeds 10 million Indonesian Rupiah.
CREATE TABLE simple_fraud_detection AS
SELECT *
FROM transactions_topic_rekeyed
WHERE amount_idr > 10000000;
```
To view the flagged transactions, you can run the following query:
```
SELECT * FROM simple_fraud_detection;
```
While the query is fairly simple, it comes with several drawbacks, such as:
- It marks every large transaction as fraudulent.
- It does not account for different customer segments.
- It requires significant research to determine the optimal threshold.

### Section 8.2 - Perform Fraud Detection using the Spending Pattern Rule
The second method tries to overcome the limitation of the first method by flagging a transaction as potentially fraudulent only when the transaction amount is greater than X times the average transaction amount. By calculating the average transaction amount for each customer, the algorithm becomes much more flexible and does not flag every large transaction as fraudulent. It can also adjust itself based on the customer’s transaction behavior.

In this example, we will mark a transaction as fraudulent if the transaction amount exceeds 2.5 times the average transaction amount in the last 30 days:
```
--Mark the transaction as fraud if the transaction amount exceeds 2.5 times the average transaction amount in the last 30 days.
CREATE TABLE average_fraud_detection AS
SELECT
  *
FROM (
  SELECT
    t.*,
    AVG(amount_idr) OVER (
      PARTITION BY card_id
      ORDER BY event_time
      RANGE BETWEEN INTERVAL '30' DAY PRECEDING AND CURRENT ROW
    ) AS avg_amount_card_last_30d
  FROM transactions_topic_rekeyed AS t
)
WHERE amount_idr > 2.5 * avg_amount_card_last_30d;
```
To view the flagged transactions, you can run the following query:
```
SELECT * FROM average_fraud_detection;
```
### Section 8.3 - Perform Fraud Detection using the Improbable Travel Distance Rule
While the second method can highlight anomalies that might indicate fraudulent transactions, there is another method that can help detect possible fraud: analyzing the transaction location. If a customer uses their card for payments in multiple distant locations within a short period of time, this could indicate a potentially fraudulent transaction.

In this example, we will mark a transaction as possible fraud if, within a 5-minute window, the transaction occurs more than 1000 km away from that card’s first transaction location in the same window:
```
--Mark the transaction as fraud if within a 5‑minute window, a transaction happened more than 1000 km away from that card’s first transaction location.
CREATE TABLE distance_fraud_detection AS
WITH base_windowed AS (
  SELECT
    transaction_id,
    card_id,
    latitude,
    longitude,
    amount_idr,
    event_time,
    window_start,
    window_end
  FROM TABLE(
    TUMBLE(
      TABLE transactions_topic_rekeyed,
      DESCRIPTOR(event_time),
      INTERVAL '5' MINUTE
    )
  )
),
first_tx AS (
  SELECT
    card_id,
    window_start,
    window_end,
    latitude  AS first_latitude,
    longitude AS first_longitude
  FROM (
    SELECT
      card_id,
      window_start,
      window_end,
      latitude,
      longitude,
      ROW_NUMBER() OVER (
        PARTITION BY card_id, window_start, window_end
        ORDER BY event_time
      ) AS rn
    FROM base_windowed
  )
  WHERE rn = 1
),
joined AS (
  SELECT
    b.transaction_id,
    b.card_id,
    b.latitude,
    b.longitude,
    b.amount_idr,
    b.event_time,
    b.window_start,
    b.window_end,
    6371 * 2 * ASIN(
      SQRT(
        POWER(SIN((RADIANS(b.latitude) - RADIANS(f.first_latitude)) / 2), 2) +
        COS(RADIANS(f.first_latitude)) * COS(RADIANS(b.latitude)) *
        POWER(SIN((RADIANS(b.longitude) - RADIANS(f.first_longitude)) / 2), 2)
      )
    ) AS distance_km
  FROM base_windowed b
  JOIN first_tx f
    ON b.card_id     = f.card_id
   AND b.window_start = f.window_start
   AND b.window_end   = f.window_end
)
SELECT
  transaction_id,
  card_id,
  latitude,
  longitude,
  amount_idr,
  event_time,
  window_start,
  window_end,
  distance_km
FROM joined
WHERE distance_km > 1000;  -- adjust threshold (km) as needed
```
To view the flagged transactions, you can run the following query:
```
SELECT * FROM distance_fraud_detection;
```
### Section 8.4 - Perform Fraud Detection using the Combined Anomaly Rule
While the previous methods detect fraud based on either abnormal transaction amounts or suspicious transaction locations, this approach combines both indicators to increase detection accuracy. A transaction is more likely to be fraudulent if it is not only unusually large compared to the customer’s historical spending behavior, but also occurs far away from the customer’s recent transaction location within a short period of time.

In this example, we enrich each transaction with the card’s rolling 30-day average transaction amount. We then apply a 5-minute tumbling window to analyze short-term location changes. Within each window, we identify the first transaction per card as the reference location. Next, we calculate the geographic distance between each transaction and that reference point using the Haversine formula:

$$
d = 2r \cdot \arcsin\left(
\sqrt{
\sin^2\left(\frac{\varphi_2 - \varphi_1}{2}\right)
+
\cos(\varphi_1)\cos(\varphi_2)
\sin^2\left(\frac{\lambda_2 - \lambda_1}{2}\right)
}
\right)
$$

Where:
- $\varphi_1$ = latitude of point 1  
- $\varphi_2$ = latitude of point 2  
- $\lambda_1$ = longitude of point 1  
- $\lambda_2$ = longitude of point 2  

Finally, we flag a transaction as potentially fraudulent only if both of the following conditions are met:
- The transaction occurs more than 1000 km away from the first transaction location within the same 5-minute window.
- The transaction amount is greater than 2 times the card’s average transaction amount over the last 30 days.

By combining behavioral (spending pattern) and geographical (improbable travel distance) anomalies, this method reduces false positives and provides a stronger fraud detection signal compared to using either method independently.
```
CREATE TABLE distance_and_average_fraud_detection AS
WITH avg_enriched AS (
  -- Per-card rolling 30‑day average
  SELECT
    t.*,
    AVG(amount_idr) OVER (
      PARTITION BY card_id
      ORDER BY event_time
      RANGE BETWEEN INTERVAL '30' DAY PRECEDING AND CURRENT ROW
    ) AS avg_amount_card_last_30d
  FROM transactions_topic_rekeyed AS t
),
base_windowed AS (
  -- 5‑minute tumbling windows per event_time
  SELECT
    transaction_id,
    card_id,
    latitude,
    longitude,
    amount_idr,
    event_time,
    avg_amount_card_last_30d,
    window_start,
    window_end
  FROM TABLE(
    TUMBLE(
      TABLE avg_enriched,
      DESCRIPTOR(event_time),
      INTERVAL '5' MINUTE
    )
  )
),
first_tx AS (
  -- First tx per card + window → reference location
  SELECT
    card_id,
    window_start,
    window_end,
    latitude  AS first_latitude,
    longitude AS first_longitude
  FROM (
    SELECT
      card_id,
      window_start,
      window_end,
      latitude,
      longitude,
      ROW_NUMBER() OVER (
        PARTITION BY card_id, window_start, window_end
        ORDER BY event_time
      ) AS rn
    FROM base_windowed
  )
  WHERE rn = 1
),
joined AS (
  -- Compute distance from first tx in the 5‑minute window
  SELECT
    b.transaction_id,
    b.card_id,
    b.latitude,
    b.longitude,
    b.amount_idr,
    b.event_time,
    b.window_start,
    b.window_end,
    b.avg_amount_card_last_30d,
    6371 * 2 * ASIN(
      SQRT(
        POWER(SIN((RADIANS(b.latitude) - RADIANS(f.first_latitude)) / 2), 2) +
        COS(RADIANS(f.first_latitude)) * COS(RADIANS(b.latitude)) *
        POWER(SIN((RADIANS(b.longitude) - RADIANS(f.first_longitude)) / 2), 2)
      )
    ) AS distance_km
  FROM base_windowed b
  JOIN first_tx f
    ON b.card_id      = f.card_id
   AND b.window_start = f.window_start
   AND b.window_end   = f.window_end
)
SELECT
  transaction_id,
  card_id,
  latitude,
  longitude,
  amount_idr,
  event_time,
  window_start,
  window_end,
  distance_km,
  avg_amount_card_last_30d
FROM joined
WHERE distance_km > 1000
  AND amount_idr > 2.5 * avg_amount_card_last_30d;
```
To view the flagged transactions, you can run the following query:
```
SELECT * FROM distance_and_average_fraud_detection;
```  
