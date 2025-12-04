"""Deterministic hash computation for payload change detection."""
import json
import hashlib
from typing import Dict, Any


def payload_hash(payload: Dict[str, Any]) -> str:
    """
    Compute a deterministic MD5 hash of a payload dictionary.
    
    Uses sorted keys and compact JSON serialization to ensure the same
    payload always produces the same hash, regardless of key order.
    
    Args:
        payload: Dictionary to hash
        
    Returns:
        32-character hexadecimal MD5 hash string
        
    Example:
        >>> payload_hash({"b": 2, "a": 1})
        'c899a67bbee5b1e5d0e580bc01468c44'
        >>> payload_hash({"a": 1, "b": 2})  # Same hash despite different order
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
