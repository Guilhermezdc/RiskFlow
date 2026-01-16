from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from dotenv import load_dotenv
import os
from faker import Faker

env = load_dotenv(".env")


class CreateUserClient:
    
    """
    This class generates fake user data and saves it to a Cassandra database.
    """
    
    def __init__(self):
        self.user_id = None,
        self.name = None,
        self.email = None,
        self.city = None,
        self.state = None,
        self.country = None,
    
    def generate_user(self):
        fake = Faker('pt-BR')
        self.user_id = fake.uuid4()
        self.name = fake.name()
        self.email = fake.email()
        self.city = fake.city()
        self.state = fake.state()
        self.country = fake.country()
        self.ip_address = fake.ipv4()
        self.device = fake.user_agent()
        return self

    def save_to_cassandra(self):
        with _CassandraConnection() as cassandra_conn:
            insert_query = """
                INSERT INTO users (user_id, name, email, city, state, country, ip_address, device)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            parameters = (
                self.user_id,
                self.name,
                self.email,
                self.city,
                self.state,
                self.country,
                self.ip_address,
                self.device
            )
            cassandra_conn.execute_query(insert_query, parameters)

class _CassandraConnection:

    """
    This class manages the connection to a Cassandra database using environment variables for configuration.
    It uses the context manager protocol to ensure that the connection is properly opened and closed.
    """

    def __init__(self):
        self._username = os.getenv("CASSANDRA_USER", "root")
        self._password = os.getenv("CASSANDRA_PASSWORD", "example_password")
        self._host = os.getenv("CASSANDRA_HOST", "cassandra")
        self._port = int(os.getenv("CASSANDRA_PORT", 9042))
        self._keyspace = os.getenv("CASSANDRA_KEYSPACE", "riskflow")

        self.cluster = None
        self.session = None

    def __enter__(self):
        auth_provider = PlainTextAuthProvider(
            username=self._username,
            password=self._password
        )

        self.cluster = Cluster(
            contact_points=[self._host],
            port=self._port,
            auth_provider=auth_provider
        )

        self.session = self.cluster.connect(self._keyspace)
        return self

    def execute_query(self, query, parameters=None):
        if not self.session:
            raise RuntimeError("Cassandra session is not initialized")

        if parameters:
            return self.session.execute(query, parameters)
        return self.session.execute(query)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cluster:
            self.cluster.shutdown()
