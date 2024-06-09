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
REDDIT_USERNAME = os.environ.get('REDDIT_USERNAME')
REDDIT_PASSWORD = os.environ.get('REDDIT_PASSWORD')


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
                hidden_comments INTEGER NOT NULL DEFAULT 0,
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
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS saved_submission (
                id SERIAL PRIMARY KEY NOT NULL,
                owner BIGINT,
                submission BIGINT NOT NULL,
                FOREIGN KEY (submission) REFERENCES submission (id),
                FOREIGN KEY (owner) REFERENCES redditor (id)
            );
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS saved_comment (
                id SERIAL PRIMARY KEY NOT NULL,
                owner BIGINT,
                comment BIGINT NOT NULL,
                FOREIGN KEY (comment) REFERENCES comment (id),
                FOREIGN KEY (owner) REFERENCES redditor (id)
            );
            """
        )
        db().commit()


def get_subreddit_id(subreddit: praw.models.Subreddit):
    numeric_id = base36.loads(subreddit.id)

    if not hasattr(get_subreddit_id, "cache"):
        get_subreddit_id.cache = set()
    if numeric_id in get_subreddit_id.cache:
        return numeric_id

    with db().cursor() as cursor:
        cursor.execute("SELECT COUNT(1) FROM subreddit WHERE id = %s", (numeric_id,))
        if cursor.fetchone()[0] != 0:
            get_subreddit_id.cache.add(numeric_id)
            return numeric_id

        cursor.execute("INSERT INTO subreddit (id, name) VALUES (%s, %s)", (numeric_id, subreddit.display_name))
        db().commit()

    get_subreddit_id.cache.add(numeric_id)
    return numeric_id


def get_redditor_id(redditor: praw.models.Redditor):
    try:
        if redditor is None or not hasattr(redditor, 'id'):
            return None
        redditor_id = redditor.id
    except prawcore.exceptions.NotFound:
        return None

    numeric_id = base36.loads(redditor_id)

    if not hasattr(get_redditor_id, "cache"):
        get_redditor_id.cache = set()
    if numeric_id in get_redditor_id.cache:
        return numeric_id

    with db().cursor() as cursor:
        cursor.execute("SELECT COUNT(1) FROM redditor WHERE id = %s", (numeric_id,))
        if cursor.fetchone()[0] != 0:
            get_redditor_id.cache.add(numeric_id)
            return numeric_id

        cursor.execute("INSERT INTO redditor (id, name) VALUES (%s, %s)", (numeric_id, redditor.name))
        db().commit()

    get_redditor_id.cache.add(numeric_id)
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

    insert_comment_unchecked(comment)

def insert_comment_unchecked(comment: praw.models.Comment):
    comment_id = base36.loads(comment.id)
    with db().cursor() as cursor:
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


def process_submission(submission: praw.models.Submission, saved_by=None):
    insert_submission(submission)

    with db().cursor() as cursor:
        cursor.execute("SELECT COUNT(1) FROM comment WHERE submission = %s", (base36.loads(submission.id),))
        stored_comments = cursor.fetchone()[0]
        cursor.execute("SELECT hidden_comments FROM submission WHERE id = %s", (base36.loads(submission.id),))
        hidden_comments = cursor.fetchone()[0]

    if stored_comments + hidden_comments < submission.num_comments:
        logging.info("Post '%s' only has %s (%s including hidden comments) out of %s comments stored, starting rehydration...", submission.id, stored_comments, stored_comments + hidden_comments, submission.num_comments)

        comment_tree = submission.comments
        while len(comment_tree.replace_more()) > 0:
            pass

        with db().cursor() as cursor:
            cursor.execute("SELECT id FROM comment WHERE submission = %s", (base36.loads(submission.id),))
            existing_comments = {e[0] for e in cursor.fetchall()}

        for comment in comment_tree.list():
            if base36.loads(comment.id) in existing_comments:
                continue
            insert_comment_unchecked(comment)

        with db().cursor() as cursor:
            cursor.execute("UPDATE submission SET hidden_comments = %s WHERE id = %s", (submission.num_comments - len(comment_tree.list()), base36.loads(submission.id)))
            db().commit()

    if saved_by:
        with db().cursor() as cursor:
            cursor.execute("SELECT COUNT(1) FROM saved_submission WHERE owner = %s AND submission = %s", (saved_by, base36.loads(submission.id)))
            if cursor.fetchone()[0] == 0:
                cursor.execute("INSERT INTO saved_submission (owner, submission) VALUES (%s, %s)", (saved_by, base36.loads(submission.id)))
            db().commit()


def process_comment(comment: praw.models.Comment, saved_by=None):
    process_submission(comment.submission)

    chain = []
    while True:
        chain.append(comment)

        parent_comment_id = base36.loads(comment.parent_id[3:]) if comment.parent_id.startswith('t1_') else None
        if parent_comment_id is None:
            break

        with db().cursor() as cursor:
            cursor.execute("SELECT COUNT(1) FROM comment WHERE id = %s", (parent_comment_id,))
            if cursor.fetchone()[0] != 0:
                break

        comment = comment.parent()

    for comment in reversed(chain):
        insert_comment(comment)

    if saved_by:
        with db().cursor() as cursor:
            cursor.execute("SELECT COUNT(1) FROM saved_comment WHERE owner = %s AND comment = %s", (saved_by, base36.loads(comment.id)))
            if cursor.fetchone()[0] == 0:
                cursor.execute("INSERT INTO saved_comment (owner, comment) VALUES (%s, %s)", (saved_by, base36.loads(comment.id)))
            db().commit()


def process_any(item, **kwargs):
    if isinstance(item, praw.models.Submission):
        process_submission(item, **kwargs)
    elif isinstance(item, praw.models.Comment):
        process_comment(item, **kwargs)
    else:
        logging.error("Trying to process unknown item type: %s", type(item))


def safe_iterable(func):
    try:
        return func()
    except prawcore.exceptions.NotFound:
        logging.exception("Failed to retrieve object list via the API, falling back to an empty list")
        return []


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--me', action='store_true')
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
                                username=REDDIT_USERNAME,
                                password=REDDIT_PASSWORD,
                                check_for_async=False)

    if not args.me:
        # --me does not really require non-read-only access, but setting read_only disables authentication alltogether, which we don't want.
        reddit_client.read_only = True

    if args.me:
        me = reddit_client.user.me()

        if me is None:
            logging.error("Archiving the user was requested, but the user is not authenticated")
            return 1

        args.redditor.append(me.name)

        redditor_id = get_redditor_id(me)

        try:
            for item in me.saved(limit=None):
                process_any(item, saved_by=redditor_id)
        except prawcore.Forbidden:
            logging.warn("No access to saved items of own user, skipping...")

    for subreddit_name in args.subreddit:
        try:
            subreddit = reddit_client.subreddit(subreddit_name)
        except prawcore.exceptions.NotFound:
            logging.exception("Failed to find subreddit '%s'", subreddit_name)

        for submission in subreddit.hot(limit=None):
            process_submission(submission)
        for submission in subreddit.new(limit=None):
            process_submission(submission)
        for submission in subreddit.rising(limit=None):
            process_submission(submission)
        for time_filter in ["all", "day", "hour", "month", "week", "year"]:
            for submission in subreddit.top(time_filter=time_filter, limit=None):
                process_submission(submission)
            for submission in subreddit.controversial(time_filter=time_filter, limit=None):
                process_submission(submission)
        for gilded_item in safe_iterable(lambda: list(subreddit.gilded(limit=None))):
            process_any(gilded_item)

    for redditor_name in args.redditor:
        try:
            redditor = reddit_client.redditor(redditor_name)
        except prawcore.exceptions.NotFound:
            logging.exception("Failed to find redditor '%s'", redditor_name)

        for item in redditor.hot(limit=None):
            process_any(item)
        for item in redditor.new(limit=None):
            process_any(item)
        for time_filter in ["all", "day", "hour", "month", "week", "year"]:
            for item in redditor.top(time_filter=time_filter, limit=None):
                process_any(item)
            for item in redditor.controversial(time_filter=time_filter, limit=None):
                process_any(item)
        for item in safe_iterable(lambda: list(redditor.gilded(limit=None))):
            process_any(item)

    for submission in args.submission:
        try:
            process_submission(reddit_client.submission(submission))
        except prawcore.exceptions.NotFound:
            logging.exception("Failed to find submission '%s'", submission)

    if args.submission_file:
        with open(args.submission_file, "r") as file:
            lines = file.readlines()

        for line in lines:
            line = line.strip()

            if not line:
                continue

            try:
                process_submission(reddit_client.submission(line))
            except prawcore.exceptions.NotFound:
                logging.exception("Failed to find submission '%s'", line)


if __name__ == "__main__":
    sys.exit(main())
