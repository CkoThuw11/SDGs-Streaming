import time
import random
import os
import signal
import sys
import logging
from datetime import datetime
from typing import Optional
import psycopg2
from psycopg2.extensions import connection

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabaseConfig:
    """Database configuration management."""
    def __init__(self):
        self.host = os.getenv("DB_HOST", "postgres")
        self.port = os.getenv("DB_PORT", "5432")
        self.name = os.getenv("DB_NAME", "sdg_streaming")
        self.user = os.getenv("DB_USER", "user")
        self.password = os.getenv("DB_PASSWORD", "password")

class DataGenerator:
    """Manages the generation and insertion of synthetic sensor data."""
    
    LOCATIONS = ["Factory_A", "Factory_B", "Downtown", "Park_C"]
    SENSORS = [f"sensor_{i}" for i in range(1, 6)]

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self._conn: Optional[connection] = None
        self._running = True
        self._setup_signal_handlers()

    def _setup_signal_handlers(self):
        """Register signal handlers for graceful shutdown."""
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame):
        """Handle shutdown signals."""
        logger.info("Shutdown signal received. Cleaning up...")
        self._running = False

    def connect(self) -> bool:
        """Establish database connection with retry logic."""
        while self._running:
            try:
                self._conn = psycopg2.connect(
                    host=self.config.host,
                    port=self.config.port,
                    dbname=self.config.name,
                    user=self.config.user,
                    password=self.config.password
                )
                logger.info("Successfully connected to database.")
                return True
            except Exception as e:
                logger.error(f"Failed to connect to database: {e}. Retrying in 5s...")
                time.sleep(5)
        return False

    def generate_reading(self) -> tuple:
        """Generate a single random sensor reading."""
        sensor_id = random.choice(self.SENSORS)
        location = random.choice(self.LOCATIONS)
        # Simulate CO2 levels (baseline ~410ppm)
        co2_level = round(random.normalvariate(410, 20), 2)
        return sensor_id, co2_level, datetime.now(), location

    def run(self):
        """Main generation loop."""
        if not self.connect():
            return

        logger.info("Starting data generation loop...")
        cursor = self._conn.cursor()

        while self._running:
            try:
                data = self.generate_reading()
                cursor.execute(
                    """
                    INSERT INTO co2_measurements 
                    (sensor_id, co2_level, measurement_time, location) 
                    VALUES (%s, %s, %s, %s)
                    """,
                    data
                )
                self._conn.commit()
                logger.debug(f"Inserted reading: {data}")
                
                # Configurable delay
                time.sleep(random.uniform(10, 15))

            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                logger.error(f"Database connection lost: {e}. Reconnecting...")
                if not self.connect():
                    break
                cursor = self._conn.cursor()
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                self._conn.rollback()
                time.sleep(1)

        self._cleanup()

    def _cleanup(self):
        """Close resources."""
        if self._conn:
            self._conn.close()
            logger.info("Database connection closed.")

if __name__ == "__main__":
    db_config = DatabaseConfig()
    generator = DataGenerator(db_config)
    generator.run()

