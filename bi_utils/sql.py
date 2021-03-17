from typing import Any


def get_query(sql_path: str, *args: Any, **kwargs: Any) -> str:
    '''Read query from file and insert params'''
    query_template = open(sql_path).read()
    query = query_template.format(*args, **kwargs)
    return query


def build_set(**equal_conditions: Any) -> str:
    '''
    Build SET statement with several `equal_conditions`
    '''
    if not equal_conditions:
        raise ValueError('Pass at least 1 equal condition as keyword argument')
    set_str = 'SET '
    for i, (column, value) in enumerate(equal_conditions.items()):
        if i > 0:
            set_str += ', '
        if value is None:
            set_str += f'{column} = NULL'
        else:
            if isinstance(value, (int, float)):
                set_str += f'{column} = {value}'
            else:
                set_str += f"{column} = '{value}'"
    return set_str


def build_where(**equal_conditions: Any) -> str:
    '''
    Build WHERE statement with several `equal_conditions`

    If no `equal_conditions` passed return `WHERE 1 = 1`
    '''
    if not equal_conditions:
        return 'WHERE 1 = 1'
    where_str = 'WHERE '
    for i, (column, value) in enumerate(equal_conditions.items()):
        if i > 0:
            where_str += ' AND '
        if value is None:
            where_str += f'{column} IS NULL'
        else:
            if isinstance(value, (int, float)):
                where_str += f'{column} = {value}'
            else:
                where_str += f"{column} = '{value}'"
    return where_str
