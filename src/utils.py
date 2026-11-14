import re

ID_LIMIT = 2**256
ID_PATTERN = re.compile(r"[0-9a-fA-F]{64}")


def validate_id(value: int) -> None:
    if type(value) is not int or not 0 <= value < ID_LIMIT:
        raise ValueError("ID must be 256-bit int")


def hex_to_id(value: str) -> int:
    """Return the ID (node, swarm, etc.) from a valid hex string.

    Parameters
    ----------
    value : str
        Should be a hex string containing exactly 64 characters.

    Returns
    -------
    int
        Unsigned 256-bit identifier.

    Raises
    ------
    ValueError
        The value is not a string of exactly 64 hexadecimal characters.
    """

    if not isinstance(value, str) or ID_PATTERN.fullmatch(value) is None:
        raise ValueError("Identifier must contain exactly 64 hexadecimal characters")
    return int(value, 16)


def id_to_hex(id: int) -> str:
    """Return the hex string from a valid ID.

    Parameters
    ----------
    id : int
        Identifier in the range `[0, 2**256)`.

    Returns
    -------
    str
        Identifier padded to 64 hexadecimal characters.

    Raises
    ------
    ValueError
        If `id` is not a valid ID.
    """

    validate_id(id)
    return f"{id:064x}"
