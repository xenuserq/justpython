import os

settings = {
    'host': os.environ.get('ACCOUNT_HOST', 'https://xenadm.documents.azure.com:443/'),
    'master_key': os.environ.get('ACCOUNT_KEY', 'Sk1rwzA6QeOsrGeF0rW56BgrpgN0z2QbGYkrkOm1fUlm2027Pf6cokHbCxRTXvvy46XoZVee6PBzACDbFSCkPg=='),
    'database_id': os.environ.get('COSMOS_DATABASE', 'focusdb'),
    'container_id': os.environ.get('COSMOS_CONTAINER', 'focusdt'),
}