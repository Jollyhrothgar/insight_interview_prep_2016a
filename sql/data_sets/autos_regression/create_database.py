#!/usr/bin/env python

# This allows us to create a database engine, which is the layer
# which talks to the database
from sqlalchemy import create_engine

# These tools let us check if a database exists, given an engine, or 
# create a database if no database exists, given an engine
from sqlalchemy_utils import database_exists, create_database, drop_database

# Here, we use sqlalchemy's built in types, which map to various database
# types.
from sqlalchemy import Column, Integer, String, Float

# Used for deriving classes from the declarative_base class for ORM management.
from sqlalchemy.ext.declarative import declarative_base

# This actually lets us talk to, and update the database.
from sqlalchemy.orm import sessionmaker

import sys

# Declaring A Mapping To an Object Representing the Table we are creating.
# This is an otherwise normal python class, but it inherits attributes from
# the declarative_base baseclass, which allow for automatic generation of
# table schema for any relational database.
Base = declarative_base()
class Car(Base):
    __tablename__ = 'auto_mpg'

    # Note that String types do not need a length in PostgreSQL and SQLite
    # but we have to specify for MySQL
    car_id = Column(Integer,primary_key = True) # a unique integer, with 1:1 mapping to car_name
    mpg = Column(Float)
    cylinders = Column(Integer)
    displacement = Column(Float)
    horsepower = Column(Float)
    weight = Column(Float)
    acceleration = Column(Float)
    model_year = Column(Float)
    origin = Column(Integer)
    car_name = Column(String)

    def __repr__(self):
        return "<User(car_id='%s', mpg='%s', cylinders='%s',displacement='%s',horsepower='%s',weight='%s',acceleration='%s',model_year='%s',origin='%s',car_name='%s')>" % (self.car_id, self.mpg, self.cylinders, self.displacement, self.horsepower, self.weight, self.acceleration, self.model_year, self.origin, self.car_name)

def load_data(filename):
    '''
    Loads car values into a list of SQL Alchemy Car-type objects
    1. mpg:           continuous
    2. cylinders:     multi-valued discrete
    3. displacement:  continuous
    4. horsepower:    continuous
    5. weight:        continuous
    6. acceleration:  continuous
    7. model year:    multi-valued discrete
    8. origin:        multi-valued discrete
    9. car name:   string (unique for each instance)
    '''
    sql_object_list = []
    with open(filename,'r') as f:
        counter = 0
        for line in f.readlines():
            autos = {}
            tokens = line.split()
            car_instance = Car()
            car_instance.car_id = counter
            car_instance.mpg    = tokens[0]
            car_instance.cylinders    = int(tokens[1])
            car_instance.displacement = float(tokens[2])
            try:
                car_instance.horsepower = float(tokens[3])
            except:
                car_instance.horsepower = None 
            car_instance.weight       = float(tokens[4])
            car_instance.acceleration = float(tokens[5])
            car_instance.model_year   = int(tokens[6])
            car_instance.origin       = int(tokens[7])
            car_instance.car_name     = ' '.join(tokens[8:])
            counter += 1
            sql_object_list.append(car_instance)
    print 'loaded',len(sql_object_list),'entries'
    return sql_object_list


def create_db(username,dbname,dbpassword):
    '''
    Returns a tuple (<bool>,database_engine_handle), such that the user can
    check to see if the database was created sucessfully, and if so, then access
    th sql_alchemy engine via the database_engine_handle
    '''
    # Here, we're using postgres, but sqlalchemy can connect to other things too.
    engine = create_engine('postgres://%s:%s@localhost/%s'%(username,dbpassword,dbname))
    print "Connecting to",engine.url
    
    if not database_exists(engine.url):
        create_database(engine.url)
    else:
        drop_database(engine.url)
        create_database(engine.url)
    database_exists_check = database_exists(engine.url)
    print "Database created successfully?:",database_exists_check
    return (database_exists_check,engine)

def main():
    print 'loading data'
    this_name = sys.argv[0]
    args = sys.argv[1:]
    if len(args) != 2:
        print "usage is:",this_name,"--read_file <filename>"
        sys.exit(1)
    elif args[0] != '--read_file' :
        print "usage is:",this_name,"--read_file <filename>"
        sys.exit(1)
    in_file = args[1]
    engine_tuple = create_db('postgres','auto_mpg','simple')
    if engine_tuple[0] == False:
        print 'Database was not created successfully, so no data will be loaded.'
        sys.exit(1)
    # Okay, if we made it to here, we now have an active database engine, which 
    # is a layer between the ORM (SQLAlchemy) and the database. We can now use
    # the table meta-data which is programmed into the declarative class, to
    # actually create the database.
    engine = engine_tuple[1]
    
    # Create a table
    Base.metadata.create_all(engine)

    # Load the data into the derived sql object class
    data = load_data(in_file)

    # Create the session object
    Session = sessionmaker()

    # attach the session object to our engine
    Session.configure(bind=engine)

    # create a handle for the configured session
    session = Session()

    # add the new or updated data to the session (we can do a lot more
    # with a session besides adding and deleting data). For example, session
    # is a local instance of data, which can be maniuplated and queried without
    # actually talking to the database.
    session.add_all(data)
    
    # Now, once we've made changes or done our analysis, and if we want these
    # changes to be reflected in the SQL database, we can commit these changes 
    # to the database.
    session.commit()

if __name__ == '__main__':
    main()
