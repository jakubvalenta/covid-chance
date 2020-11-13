import logging

import psycopg2
from sqlalchemy.orm.session import Session

from covid_chance.model import (
    ArchivedPageURL, ExportedTweet, Page, PageLine, PageURL, ParsedPageLine,
    PostedTweet, Tweet, create_session,
)

logger = logging.getLogger(__name__)


def migrate_page_urls(session: Session, cur, table_urls: str):
    logger.info('Migrating %s', table_urls)
    n_in = 0
    n_out = 0
    for (url, feed_name, inserted) in cur.execute(
        f'SELECT url, feed_name, inserted FROM {table_urls}'
    ):
        n_in += 1
        if not session.query(PageURL).filter(PageURL.url == url).exists():
            page_url = PageURL(
                url=url,
                feed_name=feed_name,
                inserted=inserted,
            )
            session.add(page_url)
            n_out += 1
    session.commit()
    logger.info('Migrated %s: %d -> %d', table_urls, n_in, n_out)


def migrate_archived_page_urls(session: Session, cur, table_archives: str):
    logger.info('Migrating %s', table_archives)
    n_in = 0
    n_out = 0
    for (feed_url, archived_url, date, inserted) in cur.execute(
        f'SELECT feed_url, archived_url, date, inserted FROM {table_archives}'
    ):
        n_in += 1
        if (
            not session.query(ArchivedPageURL)
            .filter(
                ArchivedPageURL.feed_url == feed_url,
                ArchivedPageURL.archived_url == archived_url,
                ArchivedPageURL.date == date,
            )
            .exists()
        ):
            archived_page_url = ArchivedPageURL(
                feed_url=feed_url,
                archived_url=archived_url,
                date=date,
                inserted=inserted,
            )
            session.add(archived_page_url)
            n_out += 1
    session.commit()
    logger.info('Migrated %s: %d -> %d', table_archives, n_in, n_out)


def migrate_pages(session: Session, cur, table_pages: str):
    logger.info('Migrating %s', table_pages)
    n_in = 0
    n_out = 0
    for (url, text, inserted) in cur.execute(
        f'SELECT url, text, inserted FROM {table_pages}'
    ):
        n_in += 1
        if not session.query(Page).filter(Page.url == url).exists():
            page = Page(
                url=url,
                text=text,
                inserted=inserted,
            )
            session.add(page)
            n_out += 1
    session.commit()
    logger.info('Migrated %s: %d -> %d', table_pages, n_in, n_out)


def migrate_page_lines(session: Session, cur, table_lines: str):
    logger.info('Migrating %s', table_lines)
    n_in = 0
    n_out = 0
    for (url, line, param_hash, inserted) in cur.execute(
        f'SELECT url, line, param_hash, inserted FROM {table_lines}'
    ):
        n_in += 1
        if (
            not session.query(PageLine)
            .filter(PageLine.url == url, PageLine.param_hash == param_hash)
            .exists()
        ):
            page_line = PageLine(
                url=url,
                line=line,
                param_hash=param_hash,
                inserted=inserted,
            )
            session.add(page_line)
            n_out += 1
    session.commit()
    logger.info('Migrated %s: %d -> %d', table_lines, n_in, n_out)


def migrate_parsed_page_lines(session: Session, cur, table_parsed: str):
    logger.info('Migrating %s', table_parsed)
    n_in = 0
    n_out = 0
    for (url, line, parsed, param_hash, inserted) in cur.execute(
        'SELECT url, line, parsed, param_hash, inserted '
        f'FROM {table_parsed}'
    ):
        n_in += 1
        if (
            not session.query(ParsedPageLine)
            .filter(
                ParsedPageLine.url == url,
                ParsedPageLine.param_hash == param_hash,
            )
            .exists()
        ):
            parsed_page_line = ParsedPageLine(
                url=url,
                line=line,
                parsed=parsed,
                param_hash=param_hash,
                inserted=inserted,
            )
            session.add(parsed_page_line)
            n_out += 1
    session.commit()
    logger.info('Migrated %s: %d -> %d', table_parsed, n_in, n_out)


def migrate_tweets(session: Session, cur, table_reviewed: str):
    logger.info('Migrating %s', table_reviewed)
    n_in = 0
    n_out = 0
    for (url, line, parsed, status, edited, inserted) in cur.execute(
        'SELECT url, line, parsed, status, edited, inserted '
        f'FROM {table_reviewed}'
    ):
        n_in += 1
        if (
            not session.query(Tweet)
            .filter(Tweet.url == url, Tweet.parsed == parsed)
            .exists()
        ):
            tweet = Tweet(
                url=url,
                line=line,
                parsed=parsed,
                status=status,
                edited=edited,
                inserted=inserted,
            )
            session.add(tweet)
            n_out += 1
    session.commit()
    logger.info('Migrated %s: %d -> %d', table_reviewed, n_in, n_out)


def migrate_posted_tweets(session: Session, cur, table_posted: str):
    logger.info('Migrating %s', table_posted)
    n_in = 0
    n_out = 0
    for (url, line, parsed, status, edited, tweet, inserted) in cur.execute(
        'SELECT url, line, parsed, status, edited, tweet, inserted '
        f'FROM {table_posted}'
    ):
        n_in += 1
        if (
            not session.query(PostedTweet)
            .filter(PostedTweet.text == tweet)
            .exists()
        ):
            posted_tweet = PostedTweet(
                url=url,
                line=line,
                parsed=parsed,
                status=status,
                edited=edited,
                text=tweet,
                inserted=inserted,
            )
            session.add(posted_tweet)
            n_out += 1
    session.commit()
    logger.info('Migrated %s: %d -> %d', table_posted, n_in, n_out)


def migrate_exported_tweets(session: Session, cur, table_print_export: str):
    logger.info('Migrating %s', table_print_export)
    n_in = 0
    n_out = 0
    for (
        url,
        text,
        title,
        description,
        image_path,
        domain,
        timestamp,
        inserted,
    ) in cur.execute(
        'SELECT url, text, title, description, image_path, domain, timstamp, '
        f'inserted FROM {table_print_export}'
    ):
        n_in += 1
        if (
            not session.query(ExportedTweet)
            .filter(ExportedTweet.text == text)
            .exists()
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
            n_out += 1
    session.commit()
    logger.info('Migrated %s: %d -> %d', table_print_export, n_in, n_out)


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

    migrate_archived_page_urls(session, cur, config['table_archives'])
    migrate_page_urls(session, cur, config['table_urls'])
    migrate_pages(session, cur, config['table_pages'])
    migrate_page_lines(session, cur, config['table_lines'])
    migrate_parsed_page_lines(session, cur, config['table_parsed'])
    migrate_tweets(session, cur, config['table_reviewed'])
    migrate_posted_tweets(session, cur, config['table_posted'])
    migrate_exported_tweets(session, cur, config['table_print_export'])

    conn.close()
