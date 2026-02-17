
import pytest
from unittest.mock import MagicMock, patch
from src.database.db_manager import DatabaseManager

@pytest.fixture
def mock_psycopg2_pool():
    with patch('src.database.db_manager.psycopg2.pool.ThreadedConnectionPool') as mock_pool:
        yield mock_pool

def test_singleton_pattern(mock_psycopg2_pool):
    """Test that DatabaseManager follows singleton pattern."""
    # Reset singleton instance for testing
    DatabaseManager._instance = None
    
    db1 = DatabaseManager()
    db2 = DatabaseManager()
    
    assert db1 is db2
    mock_psycopg2_pool.assert_called_once()

def test_connection_pool_initialization(mock_psycopg2_pool):
    """Test that connection pool is initialized with correct params."""
    DatabaseManager._instance = None
    db = DatabaseManager()
    
    assert db.connection_pool is not None
    # getconn is called by _ensure_schema during initialization
    assert mock_psycopg2_pool.return_value.getconn.call_count >= 1

def test_ensure_schema_execution(mock_psycopg2_pool):
    """Test that schema creation SQL is executed."""
    DatabaseManager._instance = None
    
    # Mock connection and cursor
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_psycopg2_pool.return_value.getconn.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    
    db = DatabaseManager()
    
    # Verify execute was called (for creating table and index)
    assert mock_cursor.execute.call_count >= 1
