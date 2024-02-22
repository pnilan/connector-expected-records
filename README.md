# Connector Expected Records
This repository contains a tool for managing expected records for an Airbyte source connector.

## Usage
To run the project, you must execute it in the root of a source connector. You can use one of the following commands:

- `check`: Checks the connection.
- `list`: Lists available streams.
- `generate`: Generates expected records. You can use --all, --select, or --stream flag with a stream name to generate records for all, multiple, or a single stream respectively.
- `compare`: Compares newly read records to old expected records using DeepDiff. This command requires the --stream flag with a stream name and --pk flag with the primary key type.
- `update`: Similar to compare, but also enables updating individual records.

  
## Getting Started
1. Clone this repository to your local machine:
```bash
git clone https://github.com/your-username/connector-expected-records.git
```
2. Navigate to the repository directory:
```bash
cd connector-expected-records
```
3. Ensure you have the necessary dependencies installed. You might need to install them using:
```bash
pip install -r requirements.txt
```
4. Execute the desired command in the root of your source connector.
