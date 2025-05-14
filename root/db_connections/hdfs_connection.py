from hdfs import InsecureClient

class HdfsConnection:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.hdfs_client = None

    def __enter__(self):
        self.hdfs_client = InsecureClient(f"http://{self.host}:{self.port}")
        return self.hdfs_client

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False
