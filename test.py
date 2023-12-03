from dotenv import load_dotenv
import os
import MySQLdb

# Load environment variables
load_dotenv()

try:
    # Connect to the database
    connection = MySQLdb.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USERNAME"),
        passwd=os.getenv("DB_PASSWORD"),
        db=os.getenv("DB_NAME"),
        autocommit=True,
        ssl_mode="VERIFY_IDENTITY",
        ssl={"ca": "/etc/ssl/certs/ca-certificates.crt"},  # Updated CA certificate path
    )

    print("Successfully connected to the database")

    # You can add your database operations here

except MySQLdb.Error as e:
    print("Error while connecting to MySQL", e)

finally:
    # Close the connection
    if connection:
        connection.close()
        print("Database connection closed")
