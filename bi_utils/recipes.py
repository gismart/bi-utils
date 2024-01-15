def dict_merge(receiver: dict, updater: dict) -> dict:
    """Update receiver dict with updater dict's values recursively"""
    receiver = receiver.copy()
    for k in updater:
        if (k in receiver and isinstance(receiver[k], dict) and isinstance(updater[k], dict)):
            receiver[k] = dict_merge(receiver[k], updater[k])
        else:
            receiver[k] = updater[k]
    return receiver
