import os
from typing import Dict

from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import PyMongoError
from pymongo.server_api import ServerApi

load_dotenv()


class MongoConnectionManager:
    def __init__(self):
        self._clients: Dict[str, AsyncIOMotorClient] = {}
        self.uri = os.environ.get("MONGODB_URL")
        if not self.uri:
            raise ValueError("MONGODB_URL environment variable is not set")

        self.client = AsyncIOMotorClient(
            self.uri,
            server_api=ServerApi(version="1", strict=True, deprecation_errors=True),
            maxPoolSize=50,
            minPoolSize=10,
            maxIdleTimeMS=30000,
            retryWrites=True,
        )

    def get_database(self, database_name: str):
        """Get a reference to a specific database."""
        return self.client[database_name]

    def get_collection(self, database_name: str, collection_name: str):
        """Get a reference to a specific collection in a database."""
        db = self.get_database(database_name)
        return db[collection_name]

    def close_connection(self):
        """Close the MongoDB client connection."""
        if self.client:
            self.client.close()

    async def ping(self):
        """Test the connection to the MongoDB server."""
        try:
            await self.client.admin.command("ping")
            return True
        except PyMongoError as e:
            print(f"Failed to ping MongoDB: {e}")
            return False


class LazyConnectionManager:
    def __init__(self):
        self._instance = None

    def _get_instance(self):
        if self._instance is None:
            self._instance = MongoConnectionManager()
        return self._instance

    def get_database(self, *args, **kwargs):
        return self._get_instance().get_database(*args, **kwargs)

    def get_collection(self, *args, **kwargs):
        return self._get_instance().get_collection(*args, **kwargs)

    def close_connection(self):
        if self._instance:
            return self._instance.close_connection()
        else:
            # If not initialized yet, create and close
            temp_instance = MongoConnectionManager()
            return temp_instance.close_connection()

    async def ping(self):
        return await self._get_instance().ping()


# Create a lazy-loaded instance
connection_manager = LazyConnectionManager()
