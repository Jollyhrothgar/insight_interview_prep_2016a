#!/usr/bin/env python
import psycopg2


def create_db(username,dbname,dbpassword):
    engine = create_engine('postgres://%s:%s@localhost/%s'%(username,dbpassword,dbname))
    print "Connecting to",engine.url
    
    if not database_exists(engine.url):
        create_database(engine.url)
    print "Creating Database!",(database_exists(engine.url))


def main():
    print "hello, world!"

if __name__ == '__main__':
    main()
