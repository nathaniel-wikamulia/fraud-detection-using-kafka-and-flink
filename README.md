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
