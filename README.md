
# Finance Data-Driven Platform

## Description

The Finance Data-Driven Platform is engineered to offer data-driven insights that will aid in investment decision-making. The platform achieves this by gathering data from various sources and applying machine learning models coupled with statistical analysis. At its core, it is built on a robust data extraction and processing pipeline that feeds a PostgreSQL database. This database serves as the input for both MLOps activities and the web application.

## Table of Contents

- [Description](#description)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
- [Usage](#usage)
- [Architecture](#architecture)
- [Data Pipeline](#data-pipeline)
- [Monitoring](#monitoring)
- [Future Improvements](#future-improvements)
- [License](#license)
- [Contributing](#contributing)
- [Authors and Acknowledgment](#authors-and-acknowledgment)
  
## Getting Started

### Prerequisites

- Python 3.x
- PostgreSQL
- FinancialModelingPrep API Key

### Installation

1. Clone the repository: `git clone https://github.com/yourusername/finance-data-driven-platform.git`
2. Navigate to the project directory.
3. Install the required dependencies: `pip install -r requirements.txt`
4. Create a `.env` file in the root directory and add your FinancialModelingPrep API Key as `FMP_SECRET_KEY`.

## Usage

1. To fetch and store data, run the following command: `python data_pipeline/main.py`
2. To set up the database, run: `python data_pipeline/setup_database.py`
3. Detailed instructions for backend and frontend are available in their respective directories.

## Architecture

The project is currently hosted on an Ubuntu server and uses Git and GitHub for version control. It comprises the following main components:
- Frontend of the Finance Data-Driven Platform
- Backend of the Finance Data-Driven Platform
- Data extraction and processing pipeline
- Investorkit package

## Data Pipeline

- Data is sourced from various APIs, initially from FinancialModelingPrep.
- The main focus is on company financial statements like balance sheets, income statements, and cash flows.
- Logic for historical data tracking and daily updates is implemented.
- Data is stored in a PostgreSQL database and SQLAlchemy is used for ORM.

## Monitoring

- Prometheus and Grafana are used for monitoring data ingestion and other activities.

## Future Improvements

- Transition to a microservices architecture for better scalability and maintainability.
- Docker and Kubernetes are being considered for orchestration.
- FastAPI will be the primary framework for the backend.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.

## Contributing

For contributions, please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct and the process for submitting pull requests.

## Authors and Acknowledgment

- Your name/your team's names
- Special thanks to all contributors and supporters.

