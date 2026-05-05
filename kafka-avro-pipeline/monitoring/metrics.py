from prometheus_client import start_http_server, Counter

messages_processed = Counter('messages_processed', 'Total processed')

def start():
    start_http_server(8000)
