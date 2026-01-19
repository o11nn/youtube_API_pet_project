def test_api_key(api_key):
    assert api_key == "MOCK_KEY1234" 

def test_channel_handle(channel_handle):
    assert channel_handle == "MRCHESE"

def test_postgres_conn(mock_postgres_conn_vars):
    conn = mock_postgres_conn_vars
    assert conn.host == "mock_host"
    assert conn.schema == "mock_db_name"
    assert conn.login == "mock_username"
    assert conn.password == "mock_password"
    assert conn.port == 1234