#!/usr/bin/env python3

import psycopg2

""" Interact with 'news' postgres database and store results in a file """


# Question strings for output
question_1 = "\n1. What are the most popular three articles of all time?\n"
question_2 = "\n2. Who are the most popular article authors of all time?\n"
question_3 = "\n3. When did more than 1% of requests lead to errors?\n"


# SQL commands to create two helper views
sql_article_ranking_view = """
    CREATE OR REPLACE VIEW article_ranking AS
    SELECT title, author, count(*) AS views
    FROM articles, log
    WHERE log.path LIKE CONCAT('%', articles.slug)
    GROUP BY articles.title, articles.author
    ORDER BY views DESC;
"""
sql_daily_errors_view = """
    CREATE OR REPLACE VIEW daily_report AS
    SELECT date(time) as day,
       round(100.00*sum(case log.status when '200 OK'
       then 0 else 1 end)/count(log.status), 2)
       AS errors_percentage
    FROM log
    GROUP BY day
    ORDER BY errors_percentage DESC;
"""

# SQL commands to query the news database
sql_query_1 = """
    SELECT title, views
    FROM article_ranking
    LIMIT 3;
"""
sql_query_2 = """
    SELECT authors.name, sum(article_ranking.views) as views
    FROM article_ranking, authors
    WHERE authors.id = article_ranking.author
    GROUP BY authors.name
    ORDER BY views desc;
"""
sql_query_3 = """
    SELECT *
    FROM daily_report
    WHERE errors_percentage > 1;
"""

# Connect with news postgres database
db = psycopg2.connect("dbname=news user=vagrant")


def create_view(sql_statement):
    """ Create a view in database.
    Args:
        a string containing an sql statement.
    """
    cursor = db.cursor()
    cursor.execute(sql_statement)
    db.commit()
    cursor.close()


def query(sql_query):
    """ Execute a query in database.
    Args:
        a string containing an sql query.
    Return:
        a list of retrieved rows.
    """
    cursor = db.cursor()
    cursor.execute(sql_query)
    data = cursor.fetchall()
    cursor.close()

    return data


def create_output_string(result_list):
    """ Create a string with the query results.
    ARGS:
        a list of rows returned from query().
    Return:
        a string that will be used to create the output file.
    """
    output = "\n"
    n = 1
    for e in result_list:
        try:
            views = str(e[1])
            output += "    " + str(n) + "." + e[0]
            output += " - " + views + " views\n"
        except TypeError:
            output += "    " + "Date: " + str(e[0])
            output += " - Errors: " + str(e[1]) + " %\n"
        n += 1

    return output


print("Querying database...\n")

# Create views
create_view(sql_article_ranking_view)
create_view(sql_daily_errors_view)

# Execute queries
articles_list = query(sql_query_1)
authors_list = query(sql_query_2)
errors_report = query(sql_query_3)

db.close()

print("Writing results on file...\n")

# Write all the results in output.txt
with open("output.txt", "w") as file:
    file.write("LOGS ANALYSIS PROJECT OUTPUT FILE\n")
    file.write("-" * 33 + "\n")
    file.write(question_1.upper())
    file.write(create_output_string(articles_list))
    file.write(question_2.upper())
    file.write(create_output_string(authors_list))
    file.write(question_3.upper())
    file.write(create_output_string(errors_report))
    file.close()

print("Done! In the same folder of log_analysis.py you will find output.txt, "
      "open it to see the results.")
