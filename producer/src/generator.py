from dataclasses import dataclass, asdict
from typing import Optional, List
from random import choice, randint, uniform
from faker import Faker
from infra.cassandra.dbConnection import CassandraConnection

fake = Faker('pt-BR')



@dataclass
class Transaction:
    clientId: int
    transactionId: str
    clientName: str
    paymentMethod: str
    amount: float
    installments: int
    cardInfo: Optional[str] = None

class DataGenerator:

    def __init__(self):
        self.clientIds: List[int] = self.getAllClientIds()

    def getAllClientIds(self) -> List[int]:
        with CassandraConnection() as cassandraconn:
            try:
                result = cassandraconn.execute_query(
                    "SELECT id FROM users"
                )
                return [row[0] for row in result]
            except Exception as e:
                raise RuntimeError("Erro ao buscar client IDs") from e

    def generateTransaction(self) -> Transaction:
        payment_method = choice(['PIX', 'CREDITCARD', 'DEBITCARD'])

        transaction = Transaction(
            clientId=choice(self.clientIds),
            transactionId=fake.uuid4(),
            clientName=fake.name(),
            paymentMethod=payment_method,
            amount=uniform(1.0, 400000.0),  # cents
            installments=1
        )

        if payment_method == 'CREDITCARD':
            transaction.cardInfo = fake.credit_card_full()
            transaction.installments = randint(1, 12)

        return transaction



if __name__ == "__main__":
    generator = DataGenerator()

    transaction = generator.generateTransaction()

    print(transaction)              # objeto tipado
    print(asdict(transaction))       # dict (Kafka / JSON)
