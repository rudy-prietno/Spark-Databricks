import argparse

class BaseArgumentParser:
    """
    Base class for parsing command-line arguments for database connections.
    Common arguments include host, port, user, password.
    """

    def __init__(self):
        self.parser = argparse.ArgumentParser(description="Database Connection Parameters")

    def add_common_arguments(self):
        """
        Adds common arguments like host, port, user, password to the parser.
        """
        self.parser.add_argument("--host", required=True, help="Database host")
        self.parser.add_argument("--port", required=True, help="Database port number")
        self.parser.add_argument("--user", required=True, help="Database username")
        self.parser.add_argument("--password", required=True, help="Database password")

    def parse(self):
        """
        Parses the command-line arguments and returns them.
        """
        return self.parser.parse_args()


class PostgreSQLArgumentParser(BaseArgumentParser):
    """
    Argument parser for PostgreSQL, adding specific arguments like sslmode.
    """

    def __init__(self):
        super().__init__()
        self.add_common_arguments()
        self.parser.add_argument("--db", required=True, help="PostgreSQL database name")
        self.parser.add_argument("--sslmode", default="require", help="SSL mode (default: require)")


class MySQLArgumentParser(BaseArgumentParser):
    """
    Argument parser for MySQL.
    """

    def __init__(self):
        super().__init__()
        self.add_common_arguments()
        self.parser.add_argument("--db", required=True, help="MySQL database name")
        self.parser.add_argument("--sslmode", default="require", help="SSL mode (default: require)")


class SQLServerArgumentParser(BaseArgumentParser):
    """
    Argument parser for SQL Server.
    """

    def __init__(self):
        super().__init__()
        self.add_common_arguments()
        self.parser.add_argument("--db", required=True, help="SQL Server database name")
        self.parser.add_argument("--sslmode", default="require", help="SSL mode (default: require)")


class MongoDBArgumentParser(BaseArgumentParser):
    """
    Argument parser for MongoDB, adding specific arguments like authSource.
    """

    def __init__(self):
        super().__init__()
        self.add_common_arguments()
        self.parser.add_argument("--authSource", default="admin", help="MongoDB authentication database")
