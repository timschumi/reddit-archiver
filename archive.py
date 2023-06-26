#!/usr/bin/env python3

import argparse
import logging
import os
import sys
import typing

import base36
import praw
import praw.models
import prawcore
import prawcore.exceptions
import psycopg

POSTGRES_HOST = os.environ.get("POSTGRES_HOST")
POSTGRES_USER = os.environ.get("POSTGRES_USER")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
POSTGRES_DATABASE = os.environ.get("POSTGRES_DATABASE")

REDDIT_ID = os.environ.get('REDDIT_ID')
REDDIT_SECRET = os.environ.get('REDDIT_SECRET')


def db() -> psycopg.Connection[typing.Any]:
    if hasattr(db, "connection"):
        try:
            db.connection.cursor().execute("SELECT 1")
        except psycopg.OperationalError:
            del db.connection

    if not hasattr(db, "connection") or db.connection.closed:
        db.connection = psycopg.connect(
            f"host={POSTGRES_HOST}"
            f" user={POSTGRES_USER}"
            f" password={POSTGRES_PASSWORD}"
            f" dbname={POSTGRES_DATABASE}"
        )

    return db.connection


def create_database_layout():
    with db().cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS redditor (
                id BIGINT PRIMARY KEY NOT NULL,
                name TEXT NOT NULL,
                UNIQUE (name)
            );
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS subreddit (
                id BIGINT PRIMARY KEY NOT NULL,
                name TEXT NOT NULL,
                UNIQUE (name)
            );
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS submission (
                id BIGINT PRIMARY KEY NOT NULL,
                subreddit BIGINT NOT NULL,
                title TEXT NOT NULL,
                author BIGINT,
                score INTEGER NOT NULL,
                content TEXT,
                timestamp BIGINT NOT NULL,
                distinguished BOOLEAN NOT NULL,
                stickied BOOLEAN NOT NULL,
                removed BOOLEAN NOT NULL,
                FOREIGN KEY (subreddit) REFERENCES subreddit (id),
                FOREIGN KEY (author) REFERENCES redditor (id) ON DELETE SET NULL
            );
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS comment (
                id BIGINT PRIMARY KEY NOT NULL,
                submission BIGINT NOT NULL,
                parent BIGINT,
                author BIGINT,
                score INTEGER NOT NULL,
                content TEXT,
                timestamp BIGINT NOT NULL,
                distinguished BOOLEAN NOT NULL,
                stickied BOOLEAN NOT NULL,
                removed BOOLEAN NOT NULL,
                FOREIGN KEY (submission) REFERENCES submission (id),
                FOREIGN KEY (parent) REFERENCES comment (id),
                FOREIGN KEY (author) REFERENCES redditor (id) ON DELETE SET NULL
            );
            """
        )
        db().commit()


def get_subreddit_id(subreddit: praw.models.Subreddit):
    numeric_id = base36.loads(subreddit.id)

    with db().cursor() as cursor:
        cursor.execute("SELECT COUNT(1) FROM subreddit WHERE id = %s", (numeric_id,))
        if cursor.fetchone()[0] != 0:
            return numeric_id

        cursor.execute("INSERT INTO subreddit (id, name) VALUES (%s, %s)", (numeric_id, subreddit.display_name))
        db().commit()

    return numeric_id


def get_redditor_id(redditor: praw.models.Redditor):
    try:
        if redditor is None or not hasattr(redditor, 'id'):
            return None
        redditor_id = redditor.id
    except prawcore.exceptions.NotFound:
        return None

    numeric_id = base36.loads(redditor_id)

    with db().cursor() as cursor:
        cursor.execute("SELECT COUNT(1) FROM redditor WHERE id = %s", (numeric_id,))
        if cursor.fetchone()[0] != 0:
            return numeric_id

        cursor.execute("INSERT INTO redditor (id, name) VALUES (%s, %s)", (numeric_id, redditor.name))
        db().commit()

    return numeric_id


def insert_submission(submission: praw.models.Submission):
    submission_id = base36.loads(submission.id)
    with db().cursor() as cursor:
        cursor.execute("SELECT COUNT(1) FROM submission WHERE id = %s", (submission_id,))
        if cursor.fetchone()[0] != 0:
            logging.debug("Skipping submission with ID '%s'", submission.id)
            return

        logging.info("Storing submission with ID '%s'", submission.id)
        cursor.execute(
            """
            INSERT INTO submission (id, subreddit, title, author, score, content, timestamp, distinguished, stickied, removed)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ;
            """,
            (
                submission_id,
                get_subreddit_id(submission.subreddit),
                submission.title,
                get_redditor_id(submission.author),
                submission.score,
                submission.selftext if submission.is_self else submission.url,
                submission.created_utc,
                True if submission.distinguished else False,
                submission.stickied,
                submission.removed_by_category is not None,
            )
        )
        db().commit()


def insert_comment(comment: praw.models.Comment):
    comment_id = base36.loads(comment.id)
    with db().cursor() as cursor:
        cursor.execute("SELECT COUNT(1) FROM comment WHERE id = %s", (comment_id,))
        if cursor.fetchone()[0] != 0:
            logging.debug("Skipping comment with ID '%s'", comment.id)
            return

        logging.info("Storing comment with ID '%s'", comment.id)
        cursor.execute(
            """
            INSERT INTO comment (id, submission, parent, author, score, content, timestamp, distinguished, stickied, removed)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ;
            """,
            (
                comment_id,
                base36.loads(comment.submission.id),
                base36.loads(comment.parent_id[3:]) if comment.parent_id.startswith('t1_') else None,
                get_redditor_id(comment.author),
                comment.score,
                comment.body,
                comment.created_utc,
                True if comment.distinguished else False,
                comment.stickied,
                comment.banned_by is not None or comment.body is None,
            )
        )
        db().commit()


def process_submission(submission: praw.models.Submission):
    insert_submission(submission)

    with db().cursor() as cursor:
        cursor.execute("SELECT COUNT(1) FROM comment WHERE submission = %s", (base36.loads(submission.id),))
        stored_comments = cursor.fetchone()[0]

    if stored_comments < submission.num_comments:
        logging.info("Post '%s' only has %s out of %s comments stored, starting rehydration...", submission.id, stored_comments, submission.num_comments)

        comment_tree = submission.comments
        while len(comment_tree.replace_more()) > 0:
            pass

        for comment in comment_tree.list():
            insert_comment(comment)


def process_comment(comment: praw.models.Comment):
    process_submission(comment.submission)


def process_any(item):
    if isinstance(item, praw.models.Submission):
        process_submission(item)
    elif isinstance(item, praw.models.Comment):
        process_comment(item)
    else:
        logging.error("Trying to process unknown item type: %s", type(item))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--subreddit', action='append', default=[])
    parser.add_argument('--redditor', action='append', default=[])
    parser.add_argument('--submission', action='append', default=[])
    parser.add_argument('--submission-file')
    args = parser.parse_args()

    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    create_database_layout()

    reddit_client = praw.Reddit(user_agent="linux:net.timschumi.redditarchiver:v1.0.0 (by /u/timschumi)",
                                client_id=REDDIT_ID,
                                client_secret=REDDIT_SECRET,
                                check_for_async=False)
    reddit_client.read_only = True

    for subreddit in args.subreddit:
        for submission in reddit_client.subreddit(subreddit).hot(limit=None):
            process_submission(submission)
        for submission in reddit_client.subreddit(subreddit).new(limit=None):
            process_submission(submission)
        for submission in reddit_client.subreddit(subreddit).rising(limit=None):
            process_submission(submission)
        for time_filter in ["all", "day", "hour", "month", "week", "year"]:
            for submission in reddit_client.subreddit(subreddit).top(time_filter=time_filter, limit=None):
                process_submission(submission)
            for submission in reddit_client.subreddit(subreddit).controversial(time_filter=time_filter, limit=None):
                process_submission(submission)
        for gilded_item in reddit_client.subreddit(subreddit).gilded(limit=None):
            process_any(gilded_item)

    for redditor in args.redditor:
        for item in reddit_client.redditor(redditor).hot(limit=None):
            process_any(item)
        for item in reddit_client.redditor(redditor).new(limit=None):
            process_any(item)
        for time_filter in ["all", "day", "hour", "month", "week", "year"]:
            for item in reddit_client.redditor(redditor).top(time_filter=time_filter, limit=None):
                process_any(item)
            for item in reddit_client.redditor(redditor).controversial(time_filter=time_filter, limit=None):
                process_any(item)
        for item in reddit_client.redditor(redditor).gilded(limit=None):
            process_any(item)
        try:
            for item in reddit_client.redditor(redditor).saved(limit=None):
                process_any(item)
        except prawcore.Forbidden:
            logging.info("No access to saved items of redditor '%s', skipping...", redditor)

    for submission in args.submission:
        process_submission(reddit_client.submission(submission))

    if args.submission_file:
        with open(args.submission_file, "r") as file:
            lines = file.readlines()

        for line in lines:
            line = line.strip()

            if not line:
                continue

            process_submission(reddit_client.submission(line))


if __name__ == "__main__":
    sys.exit(main())
