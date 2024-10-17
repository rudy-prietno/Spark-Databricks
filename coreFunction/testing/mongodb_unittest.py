import unittest
from unittest.mock import patch, MagicMock

class TestMongoDBConnector(unittest.TestCase):

    def setUp(self):
        """Set up valid connection parameters for testing."""
        self.valid_host = "host_db"
        self.valid_port = 21017
        self.valid_user = "user_db"
        self.valid_password = "password_db"
        self.valid_db_name = "name_db"
        self.auth_source = "auth_source"

        # Optional SSL
        self.ssl_ca = "path/to/ca-cert.pem"

    def test_missing_credentials(self):
        """Test connection with missing credentials."""
        # We remove the password here to simulate missing credentials
        with self.assertRaises(MongoDBConnectionError) as context:
            MongoDBConnector(
                host=self.valid_host,
                port=self.valid_port,
                user=self.valid_user,
                password=None,  # Missing password
                db_name=self.valid_db_name
            )
        self.assertIn("Missing MongoDB credentials", str(context.exception))
        print("Test passed: Missing credentials handled correctly.")

    @patch("pymongo.MongoClient")
    def test_valid_connection(self, mock_mongo_client):
        """Test a valid connection."""
        mock_client = MagicMock()
        mock_mongo_client.return_value = mock_client
        
        connector = MongoDBConnector(
            host=self.valid_host,
            port=self.valid_port,
            user=self.valid_user,
            password=self.valid_password,
            db_name=self.valid_db_name
        )
        self.assertIsNotNone(connector.get_connection(), "Connection should not be None")
        print("Test passed: Valid connection established.")

    @patch("pymongo.MongoClient")
    def test_valid_connection_with_ssl(self, mock_mongo_client):
        """Test a valid connection with SSL."""
        mock_client = MagicMock()
        mock_mongo_client.return_value = mock_client
        
        connector = MongoDBConnector(
            host=self.valid_host,
            port=self.valid_port,
            user=self.valid_user,
            password=self.valid_password,
            db_name=self.valid_db_name,
            ssl=True,
            ssl_ca=self.ssl_ca
        )
        self.assertIsNotNone(connector.get_connection(), "Connection should not be None")
        print("Test passed: Valid connection with SSL established.")

    @patch("pymongo.MongoClient")
    def test_singleton_pattern(self, mock_mongo_client):
        """Test that only one instance of the connection is created (Singleton)."""
        mock_client = MagicMock()
        mock_mongo_client.return_value = mock_client

        connector1 = MongoDBConnector(
            host=self.valid_host,
            port=self.valid_port,
            user=self.valid_user,
            password=self.valid_password,
            db_name=self.valid_db_name
        )
        connector2 = MongoDBConnector(
            host=self.valid_host,
            port=self.valid_port,
            user=self.valid_user,
            password=self.valid_password,
            db_name=self.valid_db_name
        )
        self.assertIs(connector1, connector2, "Both instances should be the same (Singleton)")
        print(f"Test passed: Singleton pattern works. ID1: {id(connector1)}, ID2: {id(connector2)}")

    @patch("pymongo.MongoClient")
    def test_close_connection(self, mock_mongo_client):
        """Test closing the MongoDB connection."""
        mock_client = MagicMock()
        mock_mongo_client.return_value = mock_client
        
        connector = MongoDBConnector(
            host=self.valid_host,
            port=self.valid_port,
            user=self.valid_user,
            password=self.valid_password,
            db_name=self.valid_db_name
        )
        conn = connector.get_connection()
        self.assertIsNotNone(conn, "Connection should not be None")

        result = connector.close_connection()
        self.assertTrue(result, "Connection should be closed")
        print("Test passed: Connection closed successfully.")

# Run the tests
if __name__ == "__main__":
    unittest.main(argv=[''], verbosity=2, exit=False)
