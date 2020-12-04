from typing import Any


def get_query(sql_path: str, *args: Any, **kwargs: Any) -> str:
    '''Read query from file and insert params'''
    query_template = open(sql_path).read()
    query = query_template.format(*args, **kwargs)
    return query


def build_where(**equal_conditions: Any) -> str:
    '''
    Build WHERE statement with several `equal_conditions`

    If no `equal_conditions` passed return `WHERE 1 = 1`
    '''
    if not equal_conditions:
        return 'WHERE 1 = 1'
    where = 'WHERE '
    for i, (column, value) in enumerate(equal_conditions.items()):
        if i > 0:
            where += ' AND '
        if value is None:
            where += f'{column} IS NULL'
        else:
            if isinstance(value, (int, float)):
                where += f'{column} = {value}'
            else:
                where += f"{column} = '{value}'"
    return where
