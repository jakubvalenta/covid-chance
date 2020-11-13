import datetime
import enum
import logging
from pathlib import Path
from typing import Any, Dict

from sqlalchemy import Column, DateTime, Enum, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.orm.session import Session

from covid_chance.utils.hash_utils import md5str

logger = logging.getLogger(__name__)

Base = declarative_base()


class TweetReviewStatus(enum.Enum):
    none = 0
    approved = 1
    rejected = 2


class PageURL(Base):  # type: ignore
    __tablename__ = 'urls'

    id = Column(Integer, primary_key=True)
    url = Column(String, unique=True, index=True)
    feed_name = Column(String)
    inserted = Column(DateTime, default=datetime.datetime.now)

    def __repr__(self) -> str:
        return f'Page(url={self.url}, feed_name={self.feed_name})'


class ArchivedPageURL(Base):  # type: ignore
    __tablename__ = 'archives'

    id = Column(Integer, primary_key=True)
    feed_url = Column(String, index=True)
    archived_url = Column(String)
    date = Column(DateTime, default=datetime.datetime.now, index=True)
    inserted = Column(DateTime, default=datetime.datetime.now)

    def __repr__(self) -> str:
        return (
            f'ArchivedPageURL(archived_url={self.archived_url}, '
            f'feed_url={self.feed_url}, date={self.date.isoformat()})'
        )


class Page(Base):  # type: ignore
    __tablename__ = 'pages'

    id = Column(Integer, primary_key=True)
    url = Column(String, unique=True, index=True)
    text = Column(String)
    inserted = Column(DateTime, default=datetime.datetime.now)

    def __repr__(self) -> str:
        return f'Page(url={self.url})'


class PageLine(Base):  # type: ignore
    __tablename__ = 'lines'

    id = Column(Integer, primary_key=True)
    url = Column(String, index=True)
    line = Column(String, default='')
    param_hash = Column(String, index=True)
    inserted = Column(DateTime, default=datetime.datetime.now)

    def __repr__(self) -> str:
        return f'Line(url={self.url}, line={self.line})'


class ParsedPageLine(Base):  # type: ignore
    __tablename__ = 'parsed'

    id = Column(Integer, primary_key=True)
    url = Column(String)
    line = Column(String)
    parsed = Column(String, index=True)
    param_hash = Column(String, index=True)
    inserted = Column(DateTime, default=datetime.datetime.now)

    @classmethod
    def from_page_line(cls, line: PageLine, parsed: str) -> 'ParsedPageLine':
        return cls(
            url=line.url,
            line=line.line,
            parsed=parsed,
            param_hash=line.param_hash,
        )

    def __repr__(self) -> str:
        return f'ParsedLine(url={self.url}, parsed={self.parsed})'


class Tweet(Base):  # type: ignore
    __tablename__ = 'reviewed'

    id = Column(Integer, primary_key=True)
    url = Column(String, index=True)
    line = Column(String)
    parsed = Column(String, index=True)
    status = Column(Enum(TweetReviewStatus), default=TweetReviewStatus.none)
    edited = Column(String)
    inserted = Column(DateTime, default=datetime.datetime.now)

    MAX_TWEET_LENGTH = 247

    @property
    def text(self) -> str:
        return self.edited or self.parsed

    @property
    def invalid(self) -> bool:
        return len(self.text) > self.MAX_TWEET_LENGTH

    @classmethod
    def from_parsed_page_line(
        cls, parsed_page_line: ParsedPageLine
    ) -> 'Tweet':
        return cls(
            url=parsed_page_line.url,
            line=parsed_page_line.line,
            parsed=parsed_page_line.parsed,
        )

    def __repr__(self) -> str:
        return f'Tweet(url={self.url}, text={self.text})'


class PostedTweet(Base):  # type: ignore
    __tablename__ = 'posted'

    id = Column(Integer, primary_key=True)
    url = Column(String)
    line = Column(String)
    parsed = Column(String)
    status = Column(String)
    edited = Column(String)
    text = Column(String, unique=True)
    inserted = Column(DateTime, default=datetime.datetime.now)

    @classmethod
    def from_tweet(cls, tweet: Tweet, text: str) -> 'Tweet':
        return cls(
            url=tweet.url,
            line=tweet.line,
            parsed=tweet.parsed,
            status=tweet.status,
            edited=tweet.edited,
            text=text,
        )

    def __repr__(self) -> str:
        return f'PostedTweet(url={self.url}, tweet={self.tweet})'


class ExportedTweet(Base):  # type: ignore
    __tablename__ = 'print_export'

    id = Column(Integer, primary_key=True)
    url = Column(String)
    text = Column(String, unique=True)
    title = Column(String)
    description = Column(String)
    image_path = Column(String)
    domain = Column(String)
    timestamp = Column(DateTime)
    inserted = Column(DateTime, default=datetime.datetime.now)

    @property
    def image_path_rel(self) -> str:
        p = Path(self.image_path)
        return str(p.relative_to(p.parents[1]))

    @property
    def text_hash(self) -> str:
        return md5str(self.text)[:7]

    def to_dict(self) -> Dict[str, Any]:
        return {
            'text': self.text,
            'text_hash': self.text_hash,
            'title': self.title,
            'description': self.description,
            'timestamp': self.timestamp,
            'image_path': self.image_path,
            'domain': self.domain,
        }

    def __repr__(self) -> str:
        return f'ExportedTweet(url={self.url}, text={self.text})'


def create_session(url: str) -> Session:
    engine = create_engine(url)
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    return scoped_session(session_factory)
