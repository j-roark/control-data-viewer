import click
import time
from src.queries.upstream.us_packingstats_by_ordernumber import main as us_packingstats_by_ordernumber


def main():

    click.clear()
    click.echo(
        'Welcome to the Control Data Viewer Tool. Please follow the directions on screen as they are given.')
    print()

    # dictionaries where k = friendly name, v = function memory reference
    upstream_tools = {
        'Packing Statistics By Order Number': us_packingstats_by_ordernumber,
        'TO DO: Packing Statistics By Operator': 'TODO',
        'TO DO: Packing Statistics By Purchase Order': 'TODO',
        'TO DO: Packing Statistics By Line, Machine, Threadline': 'TODO'
    }

    downstream_tools = {}

    prompt = ''
    prompt = click.prompt(
        'Please enter a U for Upstream, or a D for Downstream')
    print()

    if prompt.lower() == 'u':

        click.echo('Available Upstream Control Data Tools:')
        i = 1
        upstream = {}

        for friendly, function in upstream_tools.items():

            temp = []
            temp = [friendly, function]
            print(f'[{i}]: {friendly}')
            upstream.update({i: temp})
            i += 1
            
        print()
        selection = click.prompt(
            'Please enter number for the tool you would like to use: ', type=int)
        print()
        
        for k, lst in upstream.items():
            if selection == k:

                print(f'Tool {lst[0]}, (function) {lst[1]} selected')
                print('Running selection in 3 seconds...')
                time.sleep(3)
                lst[1]()


main()
