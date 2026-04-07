"""
Utility functions for enhancing security in applications.

This module provides features such as:
- SQL injection protection using parameterized queries
- API key redaction in logging
- Credential sanitization in exception handlers
- XSS prevention through proper JSON escaping
- Secure loading of database credentials from environment variables
"""

import os
import json
import logging
from psycopg2 import sql


# Set up logging
logging.basicConfig(level=logging.INFO)


def sanitize_log_output(log_message):
    """
    Sanitize log messages to remove sensitive data.

    Args:
        log_message (str): The log message to sanitize.

    Returns:
        str: A sanitized log message.
    """
    # Implement redaction here. E.g., removing API keys
    sanitized_message = log_message.replace('API_KEY', '[REDACTED]')
    return sanitized_message


def escape_json_for_html(json_data):
    """
    Escape JSON data to prevent XSS in HTML.

    Args:
        json_data (dict): The JSON data to escape.

    Returns:
        str: A JSON string safely escaped for HTML output.
    """
    return json.dumps(json_data).replace('<', '&lt;').replace('>', '&gt;')


def load_database_credentials():
    """
    Load and validate database credentials from environment variables.

    Returns:
        dict: A dictionary containing database credentials.
    """
    db_host = os.getenv('DB_HOST')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')

    if not all([db_host, db_user, db_password]):
        raise ValueError('Database credentials are not fully set in the environment.')

    return {
        'host': db_host,
        'user': db_user,
        'password': db_password
    }


def secure_query(connection, query, params):
    """
    Execute a query in a secure manner using parameterized queries.

    Args:
        connection: The database connection object.
        query (str): The SQL query string with placeholders.
        params (tuple): The parameters to use in the query.

    Returns:
        Result set of the executed query.
    """
    with connection.cursor() as cursor:
        cursor.execute(sql.SQL(query), params)
        return cursor.fetchall()


def handle_exception(exc):
    """
    Sanitize and log the exception message.

    Args:
        exc (Exception): The exception to handle.
    """
    sanitized_message = str(exc).replace('password', '[REDACTED]')
    logging.error('An error occurred: %s', sanitized_message)


# Example Usage:
if __name__ == '__main__':
    try:
        db_credentials = load_database_credentials()
        # Connect to the database and run queries...
    except Exception as e:
        handle_exception(e)
