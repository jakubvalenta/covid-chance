import datetime
from pathlib import Path
from typing import Dict, Optional

from covid_chance.file_utils import (
    read_csv_dict, write_csv_dict, write_csv_dict_row,
)


class TweetList(list):
    path: Path

    def __init__(self, path: Path):
        super().__init__()
        self.path = path
        if not self.path.is_file():
            return
        with path.open('r') as f:
            self.extend(read_csv_dict(f))

    def __contains__(self, tweet) -> bool:
        return self.find(tweet) is not None

    def find(self, tweet) -> Optional[Dict[str, str]]:
        for existing_tweet in self:
            if existing_tweet['parsed'] == tweet['parsed']:
                return existing_tweet
        return None

    def append(self, tweet):
        tweet_with_timestamp = {
            **tweet,
            'added': datetime.datetime.now().isoformat(),
        }
        if not self.path.is_file():
            with self.path.open('w') as f:
                write_csv_dict([tweet_with_timestamp], f)
        else:
            with self.path.open('a') as f:
                write_csv_dict_row(tweet_with_timestamp, f)
        super().append(tweet)
