# Required imports for unittest framework
import unittest

# Unit test class for testing PostgreSQLConnector
class TestPostgreSQLConnector(unittest.TestCase):

    def setUp(self):
        """Set up valid connection parameters for testing."""
        self.valid_host = "host_db"
        self.valid_db = "name_db"
        self.valid_user = "user_db"
        self.valid_password = "password_db"
        self.valid_port = 5432

    def test_missing_credentials(self):
        """Test connection with missing credentials."""
        with self.assertRaises(PostgreSQLConnectionError) as context:
            # Instance with missing password
            PostgreSQLConnector(
                host=self.valid_host,
                db=self.valid_db,
                user=self.valid_user,
                password="",  # Missing password
                port=self.valid_port
            )
        self.assertIn("Missing PostgreSQL credentials: password", str(context.exception))
        print("Test passed: Missing credentials handled correctly.")

    def test_valid_connection(self):
        """Test a valid connection."""
        try:
            connector = PostgreSQLConnector(
                host=self.valid_host,
                db=self.valid_db,
                user=self.valid_user,
                password=self.valid_password,
                port=self.valid_port
            )
            conn = connector.get_connection()
            self.assertIsNotNone(conn, "Connection should not be None")
            print("Test passed: Valid connection established.")
        except Exception as e:
            self.fail(f"Test failed: {e}")

    def test_singleton_pattern(self):
        """Test that only one instance of the connection is created (Singleton)."""
        connector1 = PostgreSQLConnector(
            host=self.valid_host,
            db=self.valid_db,
            user=self.valid_user,
            password=self.valid_password,
            port=self.valid_port
        )
        connector2 = PostgreSQLConnector(
            host=self.valid_host,
            db=self.valid_db,
            user=self.valid_user,
            password=self.valid_password,
            port=self.valid_port
        )
        self.assertIs(connector1, connector2, "Both instances should be the same (Singleton)")

        # Check if connection IDs are the same
        connection_id1 = connector1.get_connection_id()
        connection_id2 = connector2.get_connection_id()
        self.assertEqual(connection_id1, connection_id2, "Connection IDs should be the same")
        print(f"Test passed: Singleton pattern works. Connection ID: {connection_id1}:{connection_id2}")

    def test_close_connection(self):
        """Test closing the connection."""
        connector = PostgreSQLConnector(
            host=self.valid_host,
            db=self.valid_db,
            user=self.valid_user,
            password=self.valid_password,
            port=self.valid_port
        )
        conn = connector.get_connection()
        self.assertIsNotNone(conn, "Connection should not be None")

        result = connector.close_connection()
        self.assertTrue(result, "Connection should be closed")
        print("Test passed: Connection closed successfully.")


# Run the tests in the notebook
unittest.main(argv=[''], verbosity=2, exit=False)
