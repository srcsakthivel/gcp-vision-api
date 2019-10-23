import os
import io
import tempfile

from google.cloud import storage, vision, bigquery
from wand.image import Image

def hello_gcs(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    
    file_data = event
    
    file_name = file_data['name']
    bucket_name = file_data['bucket']
    
    storage_client = storage.Client()
    bqclient = bigquery.Client()
    # Instantiates a client
    client = vision.ImageAnnotatorClient()

    blob = storage_client.bucket(bucket_name).get_blob(file_name)
    blob_uri = f'gs://{bucket_name}/{file_name}'
    blob_source = {'source': {'image_uri': blob_uri}}
	
    local_audio_fileref = tempfile.NamedTemporaryFile(mode='w+b', suffix='.jpg', delete=False)
    local_audio_filepath = local_audio_fileref.name
    blob.download_to_filename(local_audio_filepath)

    # The name of the image file to annotate
    file_name = local_audio_filepath

    # Loads the image into memory
    with io.open(file_name, 'rb') as image_file:
        content = image_file.read()

    image = vision.types.Image(content=content)

    # Performs label detection on the image file
    response = client.label_detection(image=image)
    labels = response.label_annotations
	
    dataset_id = 'my_dataset'
    table_id = 'my_table'
    table_ref = bqclient.dataset(dataset_id).table(table_id)
    table = bqclient.get_table(table_ref)

    for label in labels:
        rows_to_insert = [(label.mid,label.description,label.score,label.topicality)]
        errors  = bqclient.insert_rows(table, rows_to_insert)
