from dotenv import load_dotenv
import os
from faker import Faker
from infra.cassandra.dbConnection import CassandraConnection

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
        with CassandraConnection() as cassandra_conn:
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
