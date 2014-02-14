'''
server.py 
Metadata Database Broker
'''

from swift.common.db import DatabaseBroker

# Interface with metadata database
class MetaDatabase(DatabaseBroker):

    # Initialize DB
    def initialize(self, *args, **kwargs):
        super(self).initialize(*args, **kwargs)

        # TODO: discuss content of schema
        self.conn.executescript("""
            CREATE TABLE account_metadata (
            );
            CREATE TABLE object_metadata (
            );
            CREATE TABLE container_metadata (
            );
        """)

    '''
    Insert adds rows if they don't exist, and updates them when they do 
    exist.
    '''
    # Accounts 
    def insertAccount(self, data):

    # Objects
    def insertObject(self, data):

    # Containers
    def insertContainer(self, data):
