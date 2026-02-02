import os
from typing import Any, Dict

import httpx
from bson import ObjectId
from dotenv import load_dotenv
from pymongo.errors import PyMongoError

from mongodb import connection_manager

load_dotenv()


def post_data(databaseName: str, collection_name: str, data: Dict[str, Any]):
    try:
        collection = connection_manager.get_collection(databaseName, collection_name)
        result = collection.insert_one(data)
        return result
    except PyMongoError as e:
        print(f"An error occurred while inserting data: {e}")
        raise


def get_data(databaseName: str, collection_name: str, query: dict = {}):
    try:
        collection = connection_manager.get_collection(databaseName, collection_name)
        if query:
            result = collection.find_one(query)
        else:
            result = collection.find_one()
        return result
    except PyMongoError as e:
        print(f"An error occurred while fetching data: {e}")
        raise


def get_multiple_data(
    databaseName: str, collection_name: str, query: dict = {}, limit: int = None
):
    try:
        collection = connection_manager.get_collection(databaseName, collection_name)
        cursor = collection.find(query) if query else collection.find()
        if limit:
            cursor = cursor.limit(limit)
        result = list(cursor)
        return result
    except PyMongoError as e:
        print(f"An error occurred while fetching multiple data: {e}")
        raise


def data_update(databaseName: str, collection_name: str, filter: Dict, update: Dict):
    try:
        collection = connection_manager.get_collection(databaseName, collection_name)
        result = collection.update_one(filter, update)
        return result
    except PyMongoError as e:
        print(f"An error occurred while updating data: {e}")
        raise


def data_delete(databaseName: str, collection_name: str, filter: Dict):
    try:
        collection = connection_manager.get_collection(databaseName, collection_name)
        result = collection.delete_one(filter)
        return result
    except PyMongoError as e:
        print(f"An error occurred while deleting data: {e}")
        raise


async def getBlogs(num: int = 10):
    try:
        query = f"""query Publication {{
          publication(host: "pratyaywrites.hashnode.dev") {{
            posts(first: {num}) {{
              edges {{
                node {{
                  id
                  coverImage {{
                    url
                  }}
                  title
                  brief
                  url
                }}
              }}
            }}
          }}
        }}"""

        async with httpx.AsyncClient() as client:
            response = await client.post(
                "https://gql.hashnode.com",
                headers={
                    "Content-Type": "application/json",
                },
                json={"query": query},
            )
            response.raise_for_status()  # Raise an exception for bad status codes
        result = response.json()

        # Check if the response contains errors
        if "errors" in result:
            raise Exception(f"GraphQL Error: {result['errors']}")

        return result["data"]["publication"]["posts"]["edges"]
    except httpx.RequestError as e:
        print(f"Request error occurred while fetching blogs: {e}")
        raise
    except KeyError as e:
        print(f"Unexpected response format: {e}")
        raise
    except Exception as e:
        print(f"An error occurred: {e}")
        raise


def message_send(databaseName: str, collection_name: str, data: Dict[str, Any]):
    try:
        collection = connection_manager.get_collection(databaseName, collection_name)
        result = collection.insert_one(data)
        return result
    except PyMongoError as e:
        print(f"An error occurred while sending message: {e}")
        raise


def message_receive(databaseName: str, collection_name: str, query: dict = None):
    try:
        collection = connection_manager.get_collection(databaseName, collection_name)
        if query:
            result = list(collection.find(query))
        else:
            result = list(collection.find())
        return result
    except PyMongoError as e:
        print(f"An error occurred while receiving message: {e}")
        raise


def message_delete(databaseName: str, collection_name: str, id: ObjectId):
    try:
        collection = connection_manager.get_collection(databaseName, collection_name)
        result = collection.delete_one({"_id": id})
        return result
    except PyMongoError as e:
        print(f"An error occurred while deleting message: {e}")
        raise
