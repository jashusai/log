#!/usr/bin/env python2

import psycopg2

q_1 = 'Most popular three articles of all time'
sqlquery_1 = """
        select articles.title, count(*) as num
        from articles
        join log
        on log.path like concat('/article/%', articles.slug)
        group by articles.title
        order by num desc
        limit 3;
"""

q_2 = 'Most popular article authors of all time'
sqlquery_2 = """
        select authors.name, count(*) as num
        from authors
        join articles
        on authors.id = articles.author
        join log
        on log.path like concat('/article/%', articles.slug)
        group by authors.name
        order by num desc
        limit 3;
"""

q_3 = 'On which days did more than 1% of requests lead to errors?'
sqlquery_3 = """
select * from (
    select a.day,
    round(cast((100*b.hits) as numeric) / cast(a.hits as numeric), 2)
    as errp from
        (select date(time) as day, count(*) as hits from log group by day) as a
        inner join
        (select date(time) as day, count(*) as hits from log where status
        like '%404%' group by day) as b
    on a.day = b.day)
as t where errp > 1.0;
"""


class News:
    def __init__(s):
        try:
            s.db = psycopg2.connect('dbname=news')
            s.cursor = s.db.cursor()
        except Exception as e:
            print e

    def query_execute(s, sqlquery):
        s.cursor.execute(sqlquery)
        return s.cursor.fetchall()

    def solution(s, q, sqlquery, suffix='views'):
        sqlquery = sqlquery.replace('\n', ' ')
        result = s.query_execute(sqlquery)
        print q
        for i in range(len(result)):
            print '\t', i + 1, '.', result[i][0], '--', result[i][1], suffix
        # blank line
        print ''

    def exit(s):
        s.db.close()


if __name__ == '__main__':
    obj = News()
    obj.solution(q_1, sqlquery_1)
    obj.solution(q_2, sqlquery_2)
    obj.solution(q_3, sqlquery_3, '% error')
    obj.exit()
