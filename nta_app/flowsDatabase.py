import pandas as pd
from  neo4j import AsyncGraphDatabase
import ipaddress
from locationsDatabase import searchIP
import asyncio

def is_ip_public(ip):
    return not ipaddress.ip_address(ip).is_private

async def test_connection():
    async with AsyncGraphDatabase.driver("bolt://dbflows:7687", auth=("neo4j", "password")) as driver:
        driver.verify_connectivity()
        await asyncio.sleep(1)

async def import_netflow(filename, fields):
    await asyncio.sleep(1)
    df = pd.read_csv(filename, sep=',', decimal='.', header=0, engine='python')
    for key in fields.keys():
        df = df.rename(columns={fields[key]:key})
    df.insert(0, 'Id', range(1, len(df) + 1))
    df_netflow = df[['Id', 'SrcAddr', 'Sport', 'DstAddr', 'Proto', 'Dport', 'TotBytes', 'TotPkts', 'Dur']]
    df_netflow = df_netflow.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    df_netflow.to_csv('/app/db_insert/netflow_data.csv', index=False, sep=',')

    async with AsyncGraphDatabase.driver("bolt://dbflows:7687", auth=("neo4j", "password")) as driver:
        async with driver.session() as session:
            await session.run("MATCH (a) CALL {WITH a DETACH DELETE a} IN TRANSACTIONS OF 10000 ROWS;")
            await session.run("CALL apoc.schema.assert({},{},true) YIELD label, key RETURN *")

        addr_df = pd.concat([df[['SrcAddr','Sport']].rename(columns={'SrcAddr': 'ipv4', 'Sport': 'port'}),df[['DstAddr', 'Dport']].rename(columns={'DstAddr': 'ipv4', 'Dport': 'port'})])
        addr_df = addr_df.drop_duplicates().dropna(subset=['ipv4'])
        addr_df.to_csv(r'db_insert/address_nodes.csv', index=False)

        ip_df = pd.concat([df[['SrcAddr']].rename(columns={'SrcAddr': 'ipv4'}),df[['DstAddr']].rename(columns={'DstAddr': 'ipv4'})])
        ip_df = ip_df.drop_duplicates(). dropna()
        ip_df.to_csv(r'db_insert/ip_nodes.csv', index=False)

        # utworzenie listy lokalizacji dla adresow IP
        df_loc = pd.DataFrame(columns = ['ip_addr','country_code','country_name','region_name','city_name','latitude','longitude'])
        for idx, ip in ip_df.iterrows():
            if is_ip_public(ip['ipv4']):
                loc = searchIP(ip['ipv4'])[0]
                list_row = list(loc[4:])
                list_row[0] = ip['ipv4']
                list_row[3] = list_row[3] + " (" + list_row[1] + ")"
                list_row[4] = list_row[4] + " (" + list_row[3] + ")"
                df_loc.loc[len(df_loc)] = list_row
    
        df_loc = df_loc.drop_duplicates()
        country = df_loc[['country_code','country_name']].drop_duplicates()
        region = df_loc[['country_name','region_name']].drop_duplicates()
        city = df_loc[['region_name','city_name']].drop_duplicates()
        coordinates = df_loc[['city_name','latitude','longitude']].drop_duplicates()
        ip_loc = df_loc[['ip_addr','latitude','longitude']].drop_duplicates()

        country.to_csv(r'db_insert/country_nodes.csv', index=False)
        region.to_csv(r'db_insert/region_nodes.csv', index=False)
        city.to_csv(r'db_insert/city_nodes.csv', index=False)
        coordinates.to_csv(r'db_insert/coordinate_nodes.csv', index=False)
        ip_loc.to_csv(r'db_insert/iploc_nodes.csv', index=False)

        async with driver.session() as session:
            # utworzenie node'ów dla adresów
            query = 'LOAD CSV WITH HEADERS FROM "file:///address_nodes.csv" AS row CALL {WITH row CREATE (:address {ipv4: row.ipv4, port: row.port})} IN TRANSACTIONS OF 500 ROWS'
            await session.run(query)
            query = 'CREATE CONSTRAINT addr_idx IF NOT EXISTS FOR (a:address) REQUIRE (a.ipv4, a.port) IS UNIQUE'
            await session.run(query)

            # utworzenie node'ów dla adresów ip
            query = 'LOAD CSV WITH HEADERS FROM "file:///ip_nodes.csv" AS row CALL {WITH row CREATE (:ip {ipv4: row.ipv4})} IN TRANSACTIONS OF 500 ROWS'
            await session.run(query)
            query = 'CREATE CONSTRAINT ip_idx IF NOT EXISTS FOR (a:ip) REQUIRE a.ipv4 IS UNIQUE'
            await session.run(query)
    
            # utworzenie node'ów dla krajów
            query = 'LOAD CSV WITH HEADERS FROM "file:///country_nodes.csv" AS row CALL {WITH row CREATE (:country {code: row.country_code, name: row.country_name})} IN TRANSACTIONS OF 500 ROWS'
            await session.run(query)
            query = 'CREATE CONSTRAINT country_idx IF NOT EXISTS FOR (c:country) REQUIRE c.name IS UNIQUE'
            await session.run(query)

            # utworzenie node'ów dla regionów
            query = 'LOAD CSV WITH HEADERS FROM "file:///region_nodes.csv" AS row CALL {WITH row CREATE (:region {name: row.region_name})} IN TRANSACTIONS OF 500 ROWS'
            await session.run(query)
            query = 'CREATE CONSTRAINT region_idx IF NOT EXISTS FOR (r:region) REQUIRE r.name IS UNIQUE'
            await session.run(query)

            # utworzenie node'ów dla miast
            query = 'LOAD CSV WITH HEADERS FROM "file:///city_nodes.csv" AS row CALL {WITH row CREATE (:city {name: row.city_name})} IN TRANSACTIONS OF 500 ROWS'
            await session.run(query)
            query = 'CREATE CONSTRAINT city_idx IF NOT EXISTS FOR (c:city) REQUIRE c.name IS UNIQUE'
            await session.run(query)

            # utworzenie node'ów dla współrzędnych
            query = 'LOAD CSV WITH HEADERS FROM "file:///coordinate_nodes.csv" AS row CALL {WITH row CREATE (:coordinates {latitude: row.latitude, longitude: row.longitude})} IN TRANSACTIONS OF 500 ROWS'
            await session.run(query)
            query = 'CREATE CONSTRAINT coord_idx IF NOT EXISTS FOR (c:coordinates) REQUIRE (c.latitude,c.longitude) IS UNIQUE'
            await session.run(query)

            # utworzenie node'ów dla przepływów
            query = 'LOAD CSV WITH HEADERS FROM "file:///netflow_data.csv" AS row CALL {WITH row CREATE (:flow {id: row.Id, duration: row.Dur, ' 
            query += 'protocol: row.Proto, src_addr: row.SrcAddr, src_port: row.Sport, dst_addr: row.DstAddr, dst_port: row.Dport, ' 
            query += 'number_of_packets: row.TotPkts, number_of_bytes: row.TotBytes'
            query += '})} IN TRANSACTIONS OF 500 ROWS'
            await session.run(query)
            query = 'CREATE CONSTRAINT flow_idx IF NOT EXISTS FOR (f:flow) REQUIRE (f.id) IS UNIQUE'
            await session.run(query)

            # utworzenie relacji dla regionow i państw
            query = ''
            for idx,row in region.iterrows():
                query = 'MATCH (r:region {name:"' + row['region_name'] + '"}), (c:country {name:"' + row['country_name'] + '"}) '
                query += 'MERGE (r) - [:REG_IN] -> (c);'
                await session.run(query)

            # utworzenie relacji dla miast i regionów
            query = ''
            for idx,row in city.iterrows():
                query = 'MATCH (c:city {name:"' + row['city_name'] + '"}), (r:region {name:"' + row['region_name'] + '"}) '
                query += 'MERGE (c) - [:CITY_IN] -> (r);'
                await session.run(query)

            # utworzenie relacji dla wspolrzednych geograficznych i miast
            query = ''
            for idx,row in coordinates.iterrows():
                query = 'MATCH (l:coordinates {latitude:"' + str(row['latitude']) + '",longitude:"' + str(row['longitude']) + '"}), (c:city {name:"' + row['city_name'] + '"}) '
                query += 'MERGE (l) - [:LOC_IN] -> (c);'
                await session.run(query)
            
            # utworzenie relacji miedzy wspolrzedymi a adresami ip
            query = ''
            for idx,row in ip_loc.iterrows():
                query = 'MATCH (l:coordinates {latitude:"' + str(row['latitude']) + '",longitude:"' + str(row['longitude']) + '"}), (a:ip {ipv4:"' + row['ip_addr'] + '"}) '
                query += 'MERGE (a) - [:IP_IN] -> (l);'
                await session.run(query)

            # utworzenie relacji między adresami a ip
            query = ''
            for idx,row in ip_df.iterrows():
                query = 'MATCH (a:address {ipv4:"' + row['ipv4'] + '"}), (i:ip {ipv4:"' + row['ipv4'] + '"}) '
                query += 'MERGE (a) <- [:HAS_ADDR] - (i);'
                await session.run(query)
            
            # utworzenie relacji między przepływami
            query = 'LOAD CSV WITH HEADERS FROM "file:///netflow_data.csv" AS row CALL {WITH row MATCH (f:flow {id: row.Id}),(as:address {ipv4: row.SrcAddr, port: row.Sport}) '
            query += 'MERGE (as)-[:SOURCE]->(f)'
            query += '} IN TRANSACTIONS OF 500 ROWS'
            await session.run(query)

            query = 'LOAD CSV WITH HEADERS FROM "file:///netflow_data.csv" AS row CALL {WITH row MATCH (f:flow {id: row.Id}),(as:address {ipv4: row.SrcAddr}) '
            query += 'WHERE f.src_port IS NULL AND as.port IS NULL MERGE (as)-[:SOURCE]->(f)'
            query += '} IN TRANSACTIONS OF 500 ROWS'
            await session.run(query)

            query = 'LOAD CSV WITH HEADERS FROM "file:///netflow_data.csv" AS row CALL {WITH row MATCH (f:flow {id: row.Id}),(ad:address {ipv4: row.DstAddr, port: row.Dport}) '
            query += 'MERGE (ad)-[:DESTINATION]->(f)'
            query += '} IN TRANSACTIONS OF 500 ROWS'
            await session.run(query)

            query = 'LOAD CSV WITH HEADERS FROM "file:///netflow_data.csv" AS row CALL {WITH row MATCH (f:flow {id: row.Id}),(ad:address {ipv4: row.DstAddr}) '
            query += 'WHERE f.dst_addr IS NULL AND ad.port IS NULL MERGE (ad)-[:DESTINATION]->(f)'
            query += '} IN TRANSACTIONS OF 500 ROWS'
            await session.run(query)