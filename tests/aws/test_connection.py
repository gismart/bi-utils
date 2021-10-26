from bi_utils.aws import connection


def test_get_creds():
    creds = connection.get_creds()
    assert all(key in creds for key in ("username", "password", "host", "port", "dbname"))


def test_get_redshift():
    with connection.get_redshift() as redshift:
        redshift.execute("SELECT 1")
        result = list(redshift.to_dict())
        assert len(result) == 1


def test_create_engine():
    engine = connection.create_engine()
    with engine.connect() as conn:
        result = list(conn.execute("SELECT 1"))
        assert len(result) == 1


def test_connect():
    with connection.connect("data_quality_monitoring") as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
            result = cursor.fetchall()
            assert len(result) == 1
