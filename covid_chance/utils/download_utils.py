import re
import urllib.parse
from typing import Sequence


def clean_url(
    url: str,
    remove_keys: Sequence[str] = (
        'deviceId',
        'fbclid',
        'ftag',
        'guccounter',
        'guce_referrer',
        'guce_referrer_sig',
        'guid',
        'ito',
        'lid',
        'maca',
        'mc_cid',
        'mc_eid',
        'ns_campaign',
        'ns_mchannel',
        'pm_ln',
        'r',
        'source',
        'truid',
        'utm_campaign',
        'utm_medium',
        'utm_source',
        'via',
        'wprov',
    ),
) -> str:
    u = urllib.parse.urlsplit(url)
    qs = urllib.parse.parse_qs(u.query)
    for k in remove_keys:
        if k in qs:
            del qs[k]
    new_query = urllib.parse.urlencode(qs, doseq=True)
    return urllib.parse.urlunsplit(
        (u.scheme or 'https', u.netloc, u.path, new_query, u.fragment)
    )


def simplify_url(url: str) -> str:
    if not url:
        return ''
    u = urllib.parse.urlsplit(url)
    netloc = re.sub('^.+@', '', u.netloc)
    return urllib.parse.urlunsplit(('', netloc, u.path, u.query, ''))
