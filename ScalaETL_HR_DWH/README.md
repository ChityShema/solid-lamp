# HR Top Performers ETL Pipeline - Automated Employee Performance Analysis

This Scala-based ETL pipeline analyzes employee performance data to identify top performers based on bonus history and training scores. It processes data from multiple sources, calculates composite performance metrics, and stores results in a PostgreSQL database for further analysis and reporting.

The system combines employee bonus records and training achievements to generate comprehensive performance insights. It uses Apache Spark for efficient data processing and implements a weighted scoring system that considers both performance ratings (40%) and training scores (60%). The pipeline handles data validation, aggregation, and persistence while providing robust logging and error handling capabilities.

## Repository Structure
```
.
├── build.sbt                    # Project build configuration with Spark and PostgreSQL dependencies
├── project/                     # SBT project configuration files
├── scripts/
│   └── run.sh                  # Shell script to execute the ETL pipeline
└── src/
    └── main/
        ├── resources/          # Configuration files
        │   ├── application.conf    # Database and file path configurations
        │   └── log4j.properties   # Logging configuration
        └── scala/com/hr/      # Main source code
            ├── TopPerformersETL.scala     # Main entry point
            ├── config/                    # Configuration management
            ├── models/                    # Data models and schemas
            ├── services/                  # Core ETL services
            └── utils/                     # Spark and schema utilities
```

## Usage Instructions
### Prerequisites
- Java 8 or higher
- Scala 2.12.15
- Apache Spark 3.2.0
- PostgreSQL database
- SBT 1.6.2

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd hr-top-performers-etl
```

2. Configure the database connection in `src/main/resources/application.conf`:
```hocon
database.postgres {
  url = "jdbc:postgresql://localhost:5432/your_database"
  user = "your_username"
  password = "your_password"
}
```

3. Build the project:
```bash
sbt clean compile
sbt package
```

### Quick Start

1. Ensure your input data files are in the correct location as specified in `application.conf`

2. Run the ETL pipeline:
```bash
./scripts/run.sh
```

### More Detailed Examples

Process employee performance data with custom parameters:
```scala
val spark = SparkUtils.createSparkSession("HR Top Performers ETL")
val reader = new DataReader(spark)
val processor = new DataProcessor(spark)

// Read input data
val bonusesDF = reader.readBonuses()
val trainingDF = reader.readTrainingScores()
val employeesDF = reader.readEmployees()

// Process and calculate scores
val topPerformersDF = processor.processTopPerformers(bonusesDF, trainingDF, employeesDF)
```

### Troubleshooting

Common Issues:

1. Database Connection Failures
```
Error: java.sql.SQLException: Connection refused
Solution: 
- Verify PostgreSQL is running
- Check database credentials in application.conf
- Ensure database port is accessible
```

2. Spark Memory Issues
```
Error: java.lang.OutOfMemoryError
Solution:
- Adjust Spark memory settings in run.sh:
  export SPARK_OPTS="--driver-memory 4g --executor-memory 4g"
```

Debug Mode:
- Enable debug logging in `log4j.properties`:
```properties
log4j.rootLogger=DEBUG, console
```
- Logs are written to `hadoop.log` in the configured log directory

## Data Flow
The ETL pipeline processes employee performance data through a series of transformations to identify top performers based on bonus history and training achievements.

```ascii
Input Sources                Processing                 Output
[Bonus Data] ----+
                 |
[Training Data] --+-->[Data Processing]-->[Score Calculation]-->[PostgreSQL]
                 |
[Employee Data]--+
```

Key Component Interactions:
1. DataReader loads data from CSV files and PostgreSQL
2. DataProcessor aggregates bonus amounts and calculates performance scores
3. Training scores are weighted and combined with performance ratings
4. Composite scores are calculated using 40/60 weighting
5. Results are persisted to PostgreSQL database
6. Error handling and logging occur throughout the pipeline