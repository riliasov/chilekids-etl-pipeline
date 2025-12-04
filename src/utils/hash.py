"""Детерминированное вычисление хеша для обнаружения изменений payload."""
import json
import hashlib
from typing import Dict, Any


def payload_hash(payload: Dict[str, Any]) -> str:
    """
    Вычисляет детерминированный MD5 хеш словаря payload.
    
    Использует сортировку ключей и компактную сериализацию JSON, чтобы гарантировать,
    что один и тот же payload всегда дает одинаковый хеш, независимо от порядка ключей.
    
    Args:
        payload: Словарь для хеширования
        
    Returns:
        32-символьная шестнадцатеричная строка MD5 хеша
        
    Example:
        >>> payload_hash({"b": 2, "a": 1})
        'c899a67bbee5b1e5d0e580bc01468c44'
        >>> payload_hash({"a": 1, "b": 2})  # Тот же хеш, несмотря на разный порядок
        'c899a67bbee5b1e5d0e580bc01468c44'
    """
    # Serialize with sorted keys for determinism
    serialized = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(',', ':')
    )
    return hashlib.md5(serialized.encode('utf-8')).hexdigest()
