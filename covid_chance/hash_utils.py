import hashlib
import json


def md5str(s: str) -> str:
    return hashlib.md5(s.encode()).hexdigest()


def hashobj(*args) -> str:
    return md5str(json.dumps(args, sort_keys=True))
