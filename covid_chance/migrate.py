import logging
import time

import psycopg2
from sqlalchemy.orm.session import Session

from covid_chance.model import (
    ArchivedPageURL, ExportedTweet, Page, PageLine, PageURL, ParsedPageLine,
    PostedTweet, Tweet, TweetReviewStatus, count, create_session,
)

logger = logging.getLogger(__name__)


def migrate_page_urls(session: Session, cur, table_urls: str):
    t0 = time.time()
    logger.info('Migrating %s', table_urls)
    n_in = 0
    n_out = 0
    cur.execute(f'SELECT url, feed_name, inserted FROM {table_urls}')
    for url, feed_name, inserted in cur:
        n_in += 1
        logger.debug('%d %s', n_in, url)
        if not count(session.query(PageURL).filter(PageURL.url == url)):
            page_url = PageURL(
                url=url,
                feed_name=feed_name,
                inserted=inserted,
            )
            session.add(page_url)
            if n_in % 10000 == 0:
                logger.info('%d flush', n_in)
                session.flush()
            n_out += 1
    logger.info('commit')
    session.commit()
    logger.info(
        'Migrated %s: %d -> %d in %ds',
        table_urls,
        n_in,
        n_out,
        time.time() - t0,
    )


def migrate_archived_page_urls(session: Session, cur, table_archives: str):
    t0 = time.time()
    logger.info('Migrating %s', table_archives)
    n_in = 0
    n_out = 0
    cur.execute(
        f'SELECT feed_url, archived_url, date, inserted FROM {table_archives}'
    )
    for feed_url, archived_url, date, inserted in cur:
        n_in += 1
        logger.debug('%d %s', n_in, archived_url)
        if not count(
            session.query(ArchivedPageURL).filter(
                ArchivedPageURL.feed_url == feed_url,
                ArchivedPageURL.archived_url == archived_url,
                ArchivedPageURL.date == date,
            )
        ):
            archived_page_url = ArchivedPageURL(
                feed_url=feed_url,
                archived_url=archived_url,
                date=date,
                inserted=inserted,
            )
            session.add(archived_page_url)
            if n_in % 10000 == 0:
                logger.info('%d flush', n_in)
                session.flush()
            n_out += 1
    logger.info('commit')
    session.commit()
    logger.info(
        'Migrated %s: %d -> %d in %ds',
        table_archives,
        n_in,
        n_out,
        time.time() - t0,
    )


def migrate_pages(session: Session, cur, table_pages: str):
    t0 = time.time()
    logger.info('Migrating %s', table_pages)
    n_in = 0
    n_out = 0
    cur.execute(f'SELECT url, text, inserted FROM {table_pages}')
    for url, text, inserted in cur:
        n_in += 1
        logger.debug('%d %s', n_in, url)
        if not count(session.query(Page).filter(Page.url == url)):
            page = Page(
                url=url,
                text=text,
                inserted=inserted,
            )
            session.add(page)
            if n_in % 10000 == 0:
                logger.info('%d flush', n_in)
                session.flush()
            n_out += 1
    logger.info('commit')
    session.commit()
    logger.info(
        'Migrated %s: %d -> %d in %ds',
        table_pages,
        n_in,
        n_out,
        time.time() - t0,
    )


def migrate_page_lines(session: Session, cur, table_lines: str):
    t0 = time.time()
    logger.info('Migrating %s', table_lines)
    n_in = 0
    n_out = 0
    cur.execute(f'SELECT url, line, param_hash, inserted FROM {table_lines}')
    for url, line, param_hash, inserted in cur:
        n_in += 1
        logger.debug('%d %s', n_in, url)
        if not count(
            session.query(PageLine).filter(
                PageLine.url == url,
                PageLine.line == line,
                PageLine.param_hash == param_hash,
            )
        ):
            page_line = PageLine(
                url=url,
                line=line,
                param_hash=param_hash,
                inserted=inserted,
            )
            session.add(page_line)
            if n_in % 10000 == 0:
                logger.info('%d flush', n_in)
                session.flush()
            n_out += 1
    logger.info('commit')
    session.commit()
    logger.info(
        'Migrated %s: %d -> %d in %ds',
        table_lines,
        n_in,
        n_out,
        time.time() - t0,
    )


def migrate_parsed_page_lines(session: Session, cur, table_parsed: str):
    t0 = time.time()
    logger.info('Migrating %s', table_parsed)
    n_in = 0
    n_out = 0
    cur.execute(
        f'SELECT url, line, parsed, param_hash, inserted FROM {table_parsed}'
    )
    for url, line, parsed, param_hash, inserted in cur:
        n_in += 1
        logger.debug('%d %s', n_in, url)
        if not count(
            session.query(ParsedPageLine).filter(
                ParsedPageLine.url == url,
                ParsedPageLine.line == line,
                ParsedPageLine.param_hash == param_hash,
            )
        ):
            parsed_page_line = ParsedPageLine(
                url=url,
                line=line,
                parsed=parsed,
                param_hash=param_hash,
                inserted=inserted,
            )
            session.add(parsed_page_line)
            if n_in % 10000 == 0:
                logger.info('%d flush', n_in)
                session.flush()
            n_out += 1
    logger.info('commit')
    session.commit()
    logger.info(
        'Migrated %s: %d -> %d in %ds',
        table_parsed,
        n_in,
        n_out,
        time.time() - t0,
    )


def convert_status(status_str) -> TweetReviewStatus:
    if not status_str:
        return TweetReviewStatus.none
    if status_str in ('accept', 'approved'):
        return TweetReviewStatus.approved
    if status_str == 'rejected':
        return TweetReviewStatus.rejected
    raise ValueError(f'Invalid status "{status_str}"')


def migrate_tweets(session: Session, cur, table_reviewed: str):
    t0 = time.time()
    logger.info('Migrating %s', table_reviewed)
    n_in = 0
    n_out = 0
    cur.execute(
        'SELECT url, line, parsed, status, edited, inserted '
        f'FROM {table_reviewed}'
    )
    for url, line, parsed, status_str, edited, inserted in cur:
        n_in += 1
        logger.debug('%d %s', n_in, url)
        if not count(
            session.query(Tweet).filter(
                Tweet.url == url, Tweet.parsed == parsed
            )
        ):
            tweet = Tweet(
                url=url,
                line=line,
                parsed=parsed,
                status=convert_status(status_str),
                edited=edited,
                inserted=inserted,
            )
            session.add(tweet)
            if n_in % 10000 == 0:
                logger.info('%d flush', n_in)
                session.flush()
            n_out += 1
    logger.info('commit')
    session.commit()
    logger.info(
        'Migrated %s: %d -> %d in %ds',
        table_reviewed,
        n_in,
        n_out,
        time.time() - t0,
    )


def migrate_posted_tweets(session: Session, cur, table_posted: str):
    t0 = time.time()
    logger.info('Migrating %s', table_posted)
    n_in = 0
    n_out = 0
    cur.execute(
        'SELECT url, line, parsed, status, edited, tweet, inserted '
        f'FROM {table_posted}'
    )
    for url, line, parsed, status_str, edited, tweet, inserted in cur:
        n_in += 1
        logger.debug('%d %s', n_in, url)
        if not count(
            session.query(PostedTweet).filter(PostedTweet.text == tweet)
        ):
            posted_tweet = PostedTweet(
                url=url,
                line=line,
                parsed=parsed,
                status=convert_status(status_str),
                edited=edited,
                text=tweet,
                inserted=inserted,
            )
            session.add(posted_tweet)
            if n_in % 10000 == 0:
                logger.info('%d flush', n_in)
                session.flush()
            n_out += 1
    logger.info('commit')
    session.commit()
    logger.info(
        'Migrated %s: %d -> %d in %ds',
        table_posted,
        n_in,
        n_out,
        time.time() - t0,
    )


def migrate_exported_tweets(session: Session, cur, table_print_export: str):
    t0 = time.time()
    logger.info('Migrating %s', table_print_export)
    n_in = 0
    n_out = 0
    cur.execute(
        'SELECT url, text, title, description, image_path, domain, timestamp, '
        f'inserted FROM {table_print_export}'
    )
    for (
        url,
        text,
        title,
        description,
        image_path,
        domain,
        timestamp,
        inserted,
    ) in cur:
        n_in += 1
        logger.debug('%d %s', n_in, url)
        if not count(
            session.query(ExportedTweet).filter(ExportedTweet.text == text)
        ):
            exported_tweet = ExportedTweet(
                url=url,
                text=text,
                title=title,
                description=description,
                image_path=image_path,
                domain=domain,
                timestamp=timestamp,
                inserted=inserted,
            )
            session.add(exported_tweet)
            if n_in % 10000 == 0:
                logger.info('%d flush', n_in)
                session.flush()
            n_out += 1
    logger.info('commit')
    session.commit()
    logger.info(
        'Migrated %s: %d -> %d in %ds',
        table_print_export,
        n_in,
        n_out,
        time.time() - t0,
    )


def main(config: dict):
    if not config['db'].get('host'):
        logger.error('Old database is not configured. Nothing to do')
        return

    conn = psycopg2.connect(
        host=config['db']['host'],
        database=config['db']['database'],
        user=config['db']['user'],
        password=config['db']['password'],
    )
    cur = conn.cursor()

    session = create_session(config['db']['url'])
    session.configure(autoflush=False, expire_on_commit=False)

    migrate_page_urls(session, cur, config['db']['table_urls'])
    migrate_archived_page_urls(session, cur, config['db']['table_archives'])
    migrate_pages(session, cur, config['db']['table_pages'])
    migrate_page_lines(session, cur, config['db']['table_lines'])
    migrate_parsed_page_lines(session, cur, config['db']['table_parsed'])
    migrate_tweets(session, cur, config['db']['table_reviewed'])
    migrate_posted_tweets(session, cur, config['db']['table_posted'])
    migrate_exported_tweets(session, cur, config['db']['table_print_export'])

    session.close()
    conn.close()
