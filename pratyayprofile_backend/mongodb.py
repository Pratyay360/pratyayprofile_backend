import asyncio
from pymongo import AsyncMongoClient
from pymongo.server_api import ServerApi
import os
from dotenv import load_dotenv
load_dotenv()

async def push_data(data):
    # Replace the placeholder with your Atlas connection string
    uri = f"mongodb+srv://{os.environ.get('MONGODB_USER')}:{os.environ.get('MONGODB_PASSWORD')}@cluster0.01kyari.mongodb.net/?appName=Cluster0"
    # Create a MongoClient with a MongoClientOptions object to set the Stable API version
    client = AsyncMongoClient(uri, server_api=ServerApi(
        version='1', strict=True, deprecation_errors=True))
    try:
        # Insert the provided data into the collection
        await client['llmjson']['data'].insert_one({'data': data})
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        await client.close()


