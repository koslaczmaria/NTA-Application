from io import StringIO
from nicegui import events, ui
import pandas as pd 

from convert import convert_file
from flowsDatabase import test_connection, import_netflow
from locationsDatabase import check_if_db_exists, check_if_table_exists, create_table, import_locations

header= ['']
actions_text = ''
dictionary_user = {}
dictionary_short = {}
end = False

@ui.refreshable
def actions_ui() -> None:
    global actions_text
    ui.label(actions_text)

@ui.refreshable
def final_label_ui() -> None:
    if end==True:
        ui.label('Connect to flows database using credentials: ')
        ui.label('Database URL: neo4j://localhost:7687')
        ui.label('Username: neo4j')
        ui.label('Password: password')
        ui.button('Open visualization app', on_click=lambda: ui.navigate.to('https://www.yworks.com/neo4j-explorer/', new_tab=True))
        ui.button('Open Neo4j Browser', on_click=lambda: ui.navigate.to('http://localhost:7474/browser/', new_tab=True))

def update_dictionaries(key_user, key_short, value):
    global dictionary_user, dictionary_short
    dictionary_user.update({key_user: value})
    dictionary_short.update({key_short: value})

@ui.refreshable
def select_ui(key_user=None, key_short=None) -> None:
    global dictionary_user, dictionary_short
    if key_user != None and key_short != None:
        ui.select(header, on_change=lambda e: update_dictionaries(key_user, key_short,e.value))

@ui.refreshable
def table_ui() -> None:
    columns = [{'name': 'field', 'label': 'Field name', 'field': 'field', 'required': True, 'align': 'left'},
               {'name': 'value', 'label': 'Selected value', 'field': 'value', 'required': True, 'align': 'left'},]
    rows = []
    for key, value in dictionary_user.items():
        rows.append({'field': key, 'value': value})
    ui.table(columns=columns, rows=rows, row_key='field')

def handle_upload(e: events.UploadEventArguments):
    global header
    with StringIO(e.content.read().decode("utf-8")) as f:
        df = pd.read_csv(f)
    df.to_csv('/app/data/input_file.csv', sep=',', 
              encoding='utf-8', index=False)
    header=list(df.columns.values)
    select_ui.refresh()

def confirm_fields() -> None:
    value_not_0=True
    for v in dictionary_user.values():
        if len(v) == 0:
            value_not_0 = False
    if len(dictionary_user)!=8 and value_not_0==False:
        ui.notify('Match all fields.')
    else:
        
        table_ui.refresh()
        stepper.next()

async def start_processing() -> None:
    global actions_text, dictionary_short, end
    btn_processing.disable()
    result = check_if_db_exists()
    result2 = check_if_table_exists()
    if result == False:
        actions_text += 'Locations database does not exist - verify credentials. '
        actions_ui.refresh()
        return
    if result2 == False:
        actions_text += 'Connection to locations database succesfull, but ipv4 addresses table does not exist - creating table. '
        actions_ui.refresh()
        create_table()
        await import_locations('data/IP2LOCATION-LITE-DB5.CSV')
    elif result2 == True:
        actions_text += 'Locations database and already exists - skipping creation. '
        actions_ui.refresh()
    else:
        actions_text += 'Locations database is not running correctly. '
        actions_ui.refresh()
        return
    try:
        await test_connection()
        actions_text += 'Connection to flows database succesfull. Starting flows import. '
        actions_ui.refresh()
    except:
        actions_text += 'Could not connect to flows database. '
        actions_ui.refresh()
        return
    
    filename = '/app/data/input_file.csv'
    await import_netflow(filename=filename, fields=dictionary_short)
    actions_text += 'Finished flows import. '
    actions_ui.refresh()
    end = True
    final_label_ui.refresh()

ui.page_title('NTA Application')
with ui.stepper().props('vertical').classes('w-full') as stepper:
    with ui.step('Upload file'):
        ui.label('Select file with flow data to upload')
        ui.upload(on_upload=handle_upload, max_files=1, label='Format: csv file with headers').props('accept=.csv').classes('max-w-full') # ,max_file_size=100_000_000
        with ui.stepper_navigation():
            ui.button('Next', on_click=stepper.next)
    with ui.step('Match fields'):
        ui.label('Match fields from the file')
        with ui.grid(columns=2):
            text = 'Source IPv4'
            label = ui.label(text)
            select_ui(key_user=text, key_short='SrcAddr')

            text = 'Source port'
            label = ui.label(text)
            select_ui(key_user=text, key_short='Sport')
            
            text = 'Destination IPv4'
            label = ui.label(text)
            select_ui(key_user=text, key_short='DstAddr')

            text = 'Destination port'
            label = ui.label(text)
            select_ui(key_user=text, key_short='Dport')

            text = 'Protocol'
            label = ui.label(text)
            select_ui(key_user=text, key_short='Proto')

            text = 'Number of bytes'
            label = ui.label(text)
            select_ui(key_user=text, key_short='TotBytes')

            text = 'Number of packets'
            label = ui.label(text)
            select_ui(key_user=text, key_short='TotPkts')

            text = 'Duration'
            label = ui.label(text)
            select_ui(key_user=text, key_short='Dur')
        with ui.stepper_navigation():
            ui.button('Next', on_click=confirm_fields)
            ui.button('Back', on_click=stepper.previous).props('flat')
    with ui.step('Confirm'):
        table_ui()
        with ui.stepper_navigation():
            ui.button('Confirm', on_click=stepper.next)
            ui.button('Back', on_click=stepper.previous).props('flat')
    with ui.step('Actions'):
        btn_processing = ui.button('Start processing', on_click=start_processing)
        actions_ui()
        final_label_ui()

ui.run()