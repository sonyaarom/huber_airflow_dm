import os
import logging
from typing import Dict, List, Tuple, Optional, Any
from pinecone import Pinecone, ServerlessSpec, PineconeException
from tqdm import tqdm
        
import numpy as np
from typing import Dict, List, Any
from pinecone import Pinecone, PineconeException
from tqdm import tqdm

from typing import Dict, List, Any
from tqdm import tqdm
import pinecone
from pinecone import Pinecone, PineconeException
import logging


# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_pinecone_credentials():
    from airflow.models import Variable
    api_key = Variable.get("PINECONE_API_KEY")
    environment = Variable.get("PINECONE_ENVIRONMENT")
    host = Variable.get("PINECONE_HOST")
    index_name = Variable.get("PINECONE_INDEX_NAME")
    return api_key, environment, host, index_name

def initialize_pinecone(api_key: str = None, environment: str = None) -> None:
    logger.info("Initializing Pinecone connection")
    
    api_key = api_key or os.getenv('PINECONE_API_KEY')
    environment = environment or os.getenv('PINECONE_ENVIRONMENT')
    
    if not api_key:
        logger.error("Pinecone API key not provided and PINECONE_API_KEY environment variable not set")
        raise ValueError("Pinecone API key not set")
    
    if not environment:
        logger.error("Pinecone environment not provided and PINECONE_ENVIRONMENT environment variable not set")
        raise ValueError("Pinecone environment not set")
    
    try:
        pinecone.init(api_key=api_key, environment=environment)
        logger.info("Pinecone connection initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize Pinecone: {str(e)}")
        raise

def create_pinecone_index(pc: Pinecone, index_name: str, dimension: int, project_name: str, 
                          cloud: str = "aws", region: str = "us-east-1", metric: str = 'cosine') -> Tuple[Optional[Pinecone.Index], bool]:
    """
    Creates or accesses a Pinecone index with the specified parameters.
    """
    full_index_name = f"{index_name}"
    logger.info(f"Attempting to create or access index: {full_index_name} with metric: {metric}")
    try:
        if full_index_name not in pc.list_indexes():
            pc.create_index(
                full_index_name, 
                dimension=dimension, 
                metric=metric,
                spec=ServerlessSpec(cloud=cloud, region=region),
                deletion_protection='disabled'
            )
            logger.info(f"Created new index: {full_index_name} in {cloud} {region} with metric: {metric}")
            return pc.Index(full_index_name), True
        else:
            logger.info(f"Index {full_index_name} already exists. Proceeding with existing index.")
            return pc.Index(full_index_name), False
    except PineconeException as e:
        if "ALREADY_EXISTS" in str(e):
            logger.warning(f"Index {full_index_name} already exists. Proceeding with existing index.")
            return pc.Index(full_index_name), False
        else:
            logger.error(f"Error creating/accessing index {full_index_name}: {str(e)}")
            return None, False
        


def validate_vector(vector: List[float]) -> List[float]:
    """
    Validate the vector, replacing NaN values with 0.
    """
    return np.nan_to_num(vector, nan=0.1).tolist()


def format_sparse_vector(sparse_dict: Dict[str, float]) -> Dict[str, List]:
    """
    Formats the sparse vector to match Pinecone's expected format.
    
    :param sparse_dict: Dictionary with string keys and float values
    :return: Dictionary with 'indices' and 'values' lists
    """
    indices = [int(key) for key in sparse_dict.keys()]
    values = list(sparse_dict.values())
    return {"indices": indices, "values": values}

def upload_to_pinecone_task(**kwargs):
    api_key, environment, host, index_name = get_pinecone_credentials()
    
    logger.info(f"Initializing Pinecone with API key: {api_key[:5]}..., environment: {environment}, host: {host}")
    
    pc = Pinecone(api_key=api_key)
    
    logger.info(f"Attempting to access index: {index_name}")
    index = pc.Index(index_name, environment=environment, host=host)
    
    try:
        stats = index.describe_index_stats()
        logger.info(f"Successfully connected to index. Stats: {stats}")
    except Exception as e:
        logger.error(f"Error accessing index: {str(e)}")
        raise

def upload_to_pinecone(documents: Dict[int, List[Dict[str, Any]]], index_name: str,
                       project_name: str, metric: str = 'cosine', host : str = None) -> None:
    """
    Uploads document embeddings and sparse vectors to an existing Pinecone index.
    """
    import pinecone  # Ensure pinecone is imported
    logger.info(f"Starting upload process to Pinecone for project {project_name} with metric: {metric}")

    # Access the existing index
    try:
        index = pinecone.Index(index_name)
    except Exception as e:
        logger.error(f"Error accessing index {index_name}: {str(e)}")
        return

    for chunk_size, docs in documents.items():
        if not docs:
            logger.warning(f"No documents found for chunk size {chunk_size}. Skipping.")
            continue

        # Validate and clean vectors
        cleaned_docs = []
        for doc in docs:
            try:
                cleaned_vector = validate_vector(doc['values'])
                cleaned_doc = doc.copy()
                cleaned_doc['values'] = cleaned_vector
                cleaned_docs.append(cleaned_doc)
            except Exception as e:
                logger.error(f"Error cleaning vector for document {doc.get('unique_id', 'unknown')}: {str(e)}")

        if not cleaned_docs:
            logger.warning(f"No valid documents after cleaning for chunk size {chunk_size}. Skipping.")
            continue

        # Prepare vectors to upsert
        vectors_to_upsert = []
        for doc in cleaned_docs:
            vector = {
                'id': doc['unique_id'],
                'values': doc['values'],
                'metadata': {
                    **doc.get('metadata', {}),
                    "chunk_size": chunk_size,
                    "project": project_name
                }
            }
            # Format and add sparse values if they exist
            if 'sparse_values' in doc and doc['sparse_values']:
                vector['sparse_values'] = format_sparse_vector(doc['sparse_values'])
            vectors_to_upsert.append(vector)

        batch_size = 100
        for i in tqdm(range(0, len(vectors_to_upsert), batch_size), desc=f"Uploading to {index_name}"):
            batch = vectors_to_upsert[i:i+batch_size]
            try:
                index.upsert(vectors=batch)
            except Exception as e:
                logger.error(f"Error upserting to index {index_name}: {str(e)}")
                break
        else:
            logger.info(f"Successfully uploaded {len(vectors_to_upsert)} documents to index '{index_name}'")

    logger.info("Upload process completed.")


logger = logging.getLogger(__name__)

def prepare_documents_for_upload(documents: List[Dict[str, Any]], chunk_size: int, doc_type: str, project_name: str) -> List[Dict[str, Any]]:
    """
    Prepares documents for upload by creating a structure similar to what's used in upload_to_pinecone.
    
    Args:
    documents (List[Dict[str, Any]]): List of document dictionaries.
    chunk_size (int): Size of the chunk for these documents.
    doc_type (str): Type of the document (e.g., 'text', 'recursive').
    project_name (str): Name of the project.

    Returns:
    List[Dict[str, Any]]: List of prepared documents ready for upload.
    """
    logger.info(f"Preparing {len(documents)} documents for upload. Chunk size: {chunk_size}, Doc type: {doc_type}, Project: {project_name}")

    prepared_docs = []

    for doc in tqdm(documents, desc="Preparing documents"):
        try:
            # Validate and clean vector
            cleaned_vector = validate_vector(doc.get('values', []))

            # Prepare the document structure
            prepared_doc = {
                'id': doc.get('unique_id', ''),
                'values': cleaned_vector,
                'metadata': {
                    'url': doc.get('url', ''),
                    'last_updated': doc.get('last_updated', ''),
                    'text': doc.get('text', ''),
                    'chunk_size': chunk_size,
                    'doc_type': doc_type,
                    'project': project_name
                }
            }

            # Add any additional metadata fields
            for key, value in doc.items():
                if key not in ['unique_id', 'values', 'url', 'last_updated', 'text']:
                    prepared_doc['metadata'][key] = value

            # Format and add sparse values if they exist
            if 'sparse_values' in doc and doc['sparse_values']:
                prepared_doc['sparse_values'] = format_sparse_vector(doc['sparse_values'])

            prepared_docs.append(prepared_doc)

        except Exception as e:
            logger.error(f"Error preparing document {doc.get('unique_id', 'unknown')}: {str(e)}")

    logger.info(f"Successfully prepared {len(prepared_docs)} documents for upload")

    return prepared_docs

def delete_index(pc: Pinecone, index_name: str) -> bool:
    """
    Deletes a Pinecone index.
    """
    try:
        pc.delete_index(index_name)
        logger.info(f"Successfully deleted index: {index_name}")
        return True
    except PineconeException as e:
        logger.error(f"Error deleting index {index_name}: {str(e)}")
        return False

def list_indexes(pc: Pinecone) -> List[str]:
    """
    Lists all Pinecone indexes.
    """
    try:
        indexes = pc.list_indexes()
        logger.info(f"Found indexes: {indexes}")
        return indexes
    except PineconeException as e:
        logger.error(f"Error listing indexes: {str(e)}")
        return []

# Additional utility functions can be added here as needed