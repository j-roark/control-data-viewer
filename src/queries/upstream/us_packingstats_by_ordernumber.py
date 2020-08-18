'''

Version : Python 3.7
Dependencies : MySQLdb, pandas
Written by : John Roark
Purpose : To generate upstream reports based only off of order information.

'''


import progressbar
import time
import random
import pandas as pd
import click
from datetime import date
from MySQLdb import _mysql
from src.write_to_excel import write_to_excel
from src.queries.upstream.env.upstreamenv import db_us_return


def main():

    sql_limit = 10000
    sql_fields = 'SELECT PackageID, Duration, ThreadlineID, StartTime, EndTime, Grade, Lot FROM packages_26_weeks'
    sql_orders = []
    sql_query = ''

    sql_orders, sql_limit = cli_start(sql_orders, sql_limit)

    try:

        print('Constructing SQL statements...')
        sql_fields, sql_orders, sql_query = sql_statement_constructor(
            sql_limit, sql_fields, sql_orders, sql_query)

    except BaseException:

        print('Failure in creating sql statements, closing..')
        time.sleep(5)
        return

    try:

        print('Connecting to databases...')

        db_us_lst = []
        db_us_lst = db_us_return(db_us_lst)
        db_us = _mysql.connect(
            db_us_lst[0],
            db_us_lst[1],
            db_us_lst[2],
            db_us_lst[3])

    except BaseException:

        print('Failed to connect to databases, closing...')
        time.sleep(5)
        return

    try:

        packages, durations, threadlines, starttime, endtime, grades, sql_orders = [], [], [], [], [], [], []

        print('Executing ordernum to package ID translation')
        packages, durations, threadlines, starttime, endtime, grades, sql_orders = ordernum_to_packageid(
            db_us, sql_query, packages, durations, threadlines, starttime, endtime, grades, sql_orders)

    except BaseException:

        print('Failure in ordernum to packageid translation process, closing...')
        time.sleep(5)
        return

    try:

        print('Populating package statistics from SQL')
        package_stats_fetch(
            db_us,
            packages,
            durations,
            threadlines,
            starttime,
            endtime,
            grades,
            sql_orders)

    except BaseException:

        print('Failure to retrieve packing stats from SQL')
        time.sleep(5)
        return

    return


def cli_start(sql_orders, sql_limit):

    click.clear()
    db = None
    click.echo(
        'Welcome to the Control Data Viewer Tool. Please follow the directions on screen as they are given.')
    click.echo(
        'You are currently using the Upstream Packing Statistics by Order Number Tool.')
    prompt = ''
    sql_orders = []

    while prompt != 0:

        prompt = click.prompt(
            'Enter order (or 0 to stop entering sql_orders)', type=int)

        if prompt == 0:
            break

        elif len(str(prompt)) != 7:
            click.secho(click.style(
                f'{prompt} is not a valid order, start over from the beginning please...'))
            sql_orders = []
            continue

        elif len(str(prompt)) == 7:
            sql_orders.append(int(prompt))
            print(sql_orders)

    if click.confirm('sql_limit number of packages? Default = 10000:'):
        sql_limit = click.prompt(
            'Enter total allowed number of packages',
            type=int,
            default=10000)

    else:
        sql_limit = 10000

    return sql_orders, sql_limit


def sql_statement_constructor(sql_limit, sql_fields, sql_orders, sql_query):

    # Build the SQL statement from the list of sql_orders

    sql_sql_orders = f" WHERE lot LIKE '%{sql_orders[0]}'"

    for i, v in enumerate(sql_orders):
        sql_sql_orders = sql_sql_orders + f" OR lot LIKE '%{v}'"

    sql_query = sql_fields + sql_sql_orders + "LIMIT " + str(sql_limit)
    return sql_fields, sql_orders, sql_query


def ordernum_to_packageid(
        db,
        sql_query,
        packages,
        durations,
        threadlines,
        starttime,
        endtime,
        grades,
        sql_orders):

    # Now that you have the query stored in a sql_query object run it, and iterate the response.
    # This is where packageid, order, threadline, start times, and end times
    # are generated.

    db.query(sql_query)

    query = db.store_result()
    packages, durations = [], []

    for row in query.fetch_row(maxrows=100000):

        packages.append(str(row[0], 'utf-8'))
        durations.append(str(row[1], 'utf-8'))
        threadlines.append(str(row[2], 'utf-8'))
        starttime.append(str(row[3], 'utf-8'))
        endtime.append(str(row[4], 'utf-8'))

        grade = (str(row[5], 'utf-8'))

        if grade == '0':
            grade = 'A'

        elif grade == '1':
            grade = 'B'

        elif grade == '2':
            grade = 'C'

        grades.append(grade)

        order = (str(row[6], 'utf-8'))
        order = order[-7:]

        sql_orders.append(order)

    return packages, durations, threadlines, starttime, endtime, grades, sql_orders


def package_stats_fetch(
        db,
        packages,
        durations,
        threadlines,
        starttime,
        endtime,
        grades,
        sql_orders):

    # For each package in each order, query SQL for more data- populating the
    # rest of the values in three different queries per package.
    final_data = []

    with progressbar.ProgressBar(max_value=len(packages)) as bar:
        for i, package in enumerate(packages):

            pkg_query = f"SELECT PackageID, Avg, StdDev FROM packagestatistics WHERE PackageID = '{package}'"
            db.query(pkg_query)
            stats = db.store_result()

            packageid = 0

            duration = durations[i]
            threadlineid = threadlines[i]
            start = starttime[i]
            end = endtime[i]
            grade = grades[i]
            order = sql_orders[i]

            magnitude_avg = 0
            magnitude_stddev = 0
            filaments = ''
            polymer = ''
            cam = ''
            operator = ''
            style = ''
            ponumber = ''
            color = ''
            machine = ''
            position = ''
            threadline = ''
            nodes_per_meter_avg = ''
            nodes_per_meter_stdev = ''

            # Grab some basic statistics
            for k, stat in enumerate(stats.fetch_row(maxrows=0)):
                if k == 0:
                    packageid = str(stat[0], 'utf-8')
                    magnitude_avg = str(stat[1], 'utf-8')
                    magnitude_stddev = str(stat[2], 'utf-8')

                    packageid = int(packageid)
                    amplitude = float(magnitude_avg)
                    magnitude_stddev = float(magnitude_stddev)

                    continue

                elif k == 2:
                    nodes_per_meter_avg = str(stat[1], 'utf-8')
                    nodes_per_meter_stdev = str(stat[2], 'utf-8')

            if packageid is None or packageid == 0:
                break

            # Get other properties. If else statement to inverse the stack from
            # key value to row.
            props_query = f"SELECT PropName, PropValue FROM packageproperties WHERE PackageID = '{package}'"
            db.query(props_query)

            props = db.store_result()

            for prop in props.fetch_row(maxrows=0):

                proptype = str(prop[0], 'utf-8')
                if proptype == 'Filaments':
                    filaments = int(str(prop[1], 'utf-8'))
                    continue

                elif proptype == 'Polymer':
                    polymer = str(prop[1], 'utf-8')
                    continue

                elif proptype == 'CAM':
                    cam = str(prop[1], 'utf-8')
                    continue

                elif proptype == 'Operator':
                    operator = str(prop[1], 'utf-8')
                    continue

                elif proptype == 'Style':
                    style = str(prop[1], 'utf-8')
                    continue

                elif proptype == 'PONumber':
                    ponumber = str(prop[1], 'utf-8')
                    continue

                elif proptype == 'Coloring':
                    color = str(prop[1], 'utf-8')
                    continue

            # Grab machine, position, threadline
            thread_query = f'''

            select threadlinesubject.MachineName, threadlinesubject.PositionName, threadlinesubject.ThreadlineName from package
            LEFT JOIN threadlinesubject
            ON threadlinesubject.ThreadlineID = package.ThreadlineID
            where PackageID = {package}
            group by package.ThreadlineID

            '''

            db.query(thread_query)
            threads = db.store_result()

            for thread in threads.fetch_row(maxrows=10000):
                machine = str(thread[0], 'utf-8')
                position = str(thread[1], 'utf-8')
                threadline = str(thread[2], 'utf-8')

            if i == 0:
                final_data = pd.DataFrame({
                    'package': [packageid],
                    'order': [order],
                    'duration': [duration],
                    'start': [start],
                    'end': [end],
                    'grade': [grade],
                    'magnitude avg': [magnitude_avg],
                    'magnitude stddev': [magnitude_stddev],
                    'nodes per meter avg': [nodes_per_meter_avg],
                    'nodes per meter stddev': [nodes_per_meter_stdev],
                    'filaments': [filaments],
                    'polymer': [polymer],
                    'cam': [cam],
                    'operator': [operator],
                    'style': [style],
                    'ponumber': [ponumber],
                    'color': [color],
                    'machine': [machine],
                    'position': [position],
                    'threadline': [threadline]
                })

            elif i > 0:

                package = pd.DataFrame({
                    'package': [packageid],
                    'order': [order],
                    'duration': [duration],
                    'start': [start],
                    'end': [end],
                    'grade': [grade],
                    'magnitude avg': [magnitude_avg],
                    'magnitude stddev': [magnitude_stddev],
                    'nodes per meter avg': [nodes_per_meter_avg],
                    'nodes per meter stddev': [nodes_per_meter_stdev],
                    'filaments': [filaments],
                    'polymer': [polymer],
                    'cam': [cam],
                    'operator': [operator],
                    'style': [style],
                    'ponumber': [ponumber],
                    'color': [color],
                    'machine': [machine],
                    'position': [position],
                    'threadline': [threadline]
                })
                dataframes = [final_data, package]
                final_data = pd.concat(dataframes)
                
            if i % 100 == 0:
                time.sleep(0.5)

            bar.update(i)
    
    print(len(final_data))
    write_to_excel(final_data)


if __name__ == "__main__":

    main()
