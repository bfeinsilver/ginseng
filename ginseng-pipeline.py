# -*- coding: utf-8 -*-
"""
Created on Sat May  4 21:48:16 2019

@author: benja
"""

import requests
import luigi
import json
import xml.etree.ElementTree as ET
import utils
import time
import math
import csv
import io
import zipfile
import rasterio
import pandas as pd
from zipfile import ZipFile
from contextlib import ExitStack
from urllib3.util.retry import Retry

class EntrezTask(luigi.Task):
    url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/'
    retries = Retry(backoff_factor=0.1)
    adapter = requests.adapters.HTTPAdapter(max_retries=retries)
    timeout = 30
    retmax = 50 # Entrez limits JSON responses to 500 records per request.
    api_key = luigi.Parameter(default='<api-key>') # Must register with NCBI.

    
class GBIFTask(luigi.Task):
    url = 'http://api.gbif.org/v1/'
    retries = Retry(backoff_factor=0.1)
    adapter = requests.adapters.HTTPAdapter(max_retries=retries)
    timeout = 30


class SearchNuccore(EntrezTask):
    
    # This task searches the NCBI Nucleotide (Nuccore) database and, using the
    # Entrez History Server and ESearch utility, returns a web environment and
    # query key.
    
    data = ('chloroplast[Filter] AND plants[Filter] AND complete[Properties] '
            'NOT unverified[Title] AND (120000[SLEN] : 160000[SLEN])')
    term = luigi.Parameter(default=data)
    
    def output(self):
        return luigi.LocalTarget('data/nuccore-esearch-params.json')

    def run(self):
        with ExitStack() as stack:
            outfile = stack.enter_context(self.output().open('w'))
            s = stack.enter_context(requests.Session())
            
            payload = {'db':'nuccore',
                       'retmode':'json',
                       'usehistory':'y',
                       'term':self.term
                       }
            s.mount(self.url, self.adapter)
            r = s.get(self.url + 'esearch.fcgi', params=payload,
                timeout=self.timeout)
            if r.status_code == requests.codes.ok:
                outfile.write(r.text)
        

class GetNuccoreSummaries(EntrezTask):
    
    # This task returns a list of UIDs and Taxonomy IDs corresponding to the
    # previous web environment and query key using the ESummary utility.
    
    def requires(self):
        return SearchNuccore()
        
    def output(self):
        return luigi.LocalTarget('data/nuccore-docsummaries.txt')
        
    def run(self):
        with ExitStack() as stack:
            infile = stack.enter_context(self.input().open('r'))
            outfile = stack.enter_context(self.output().open('w'))
            s = stack.enter_context(requests.Session())
            
            s.mount(self.url, self.adapter)            
            result = json.load(infile)['esearchresult']
            query_key = result['querykey']
            webenv = result['webenv']
            count = int(result['count'])        
            for retstart in range(0, count, self.retmax):
                time.sleep(0.1) # 10 requests per second limit.
                message = 'Progress: {0:.0%}'.format(retstart / count)
                self.set_status_message(message)
                print(message)
                args = [query_key, webenv, retstart,
                        self.retmax, 'nuccore', self.api_key]
                prepped = utils.prep_esummary_req(*args)
                r = s.send(prepped, timeout=self.timeout, stream=False)
                if r.status_code == requests.codes.ok:
                    for key, value in r.json()['result'].items():
                        # Skips list of UIDs included in result.
                        if key != 'uids':
                            data = {'uid':value['uid'],
                                    'taxid':value['taxid']
                                    }
                            outfile.write('{uid},{taxid}\n'.format(**data))
                                

class RemoveDuplicateTaxIDs(luigi.Task):
    
    # This task returns a list of unique Taxonomy IDs.
    
    def requires(self):
        return GetNuccoreSummaries()
        
    def output(self):
        return luigi.LocalTarget('data/unique-taxids.txt')
        
    def run(self):
        with ExitStack() as stack:
            infile = stack.enter_context(self.input().open('r'))
            outfile = stack.enter_context(self.output().open('w'))        
            lines = [line.split(',') for line in infile.read().splitlines()]
            taxids = set([taxid for uid,taxid in lines])
            for taxid in taxids:
                outfile.write(taxid + '\n')
                
                                
class PostTaxIDs(EntrezTask):
    
    # This task posts the previous list of unique Taxonomy IDs to the NCBI
    # Taxonomy database and, using the Entrez History Server and EPost
    # utility, returns a web environment and query key.

    def requires(self):
        return RemoveDuplicateTaxIDs()
        
    def output(self):
        return luigi.LocalTarget('data/taxonomy-epost-params.xml')
        
    def run(self):
        with ExitStack() as stack:
            infile = stack.enter_context(self.input().open('r'))
            outfile = stack.enter_context(self.output().open('w'))
            s = stack.enter_context(requests.Session())        
        
            taxids = ','.join(infile.read().splitlines())
            payload = {'db':'taxonomy', 'id':taxids}
            s.mount(self.url, self.adapter)        
            r = s.post(self.url + 'epost.fcgi', data=payload,
                timeout=self.timeout)
            if r.status_code == requests.codes.ok:
                outfile.write(r.text)
                    
            
class GetTaxonomySummaries(EntrezTask):
    
    # This task returns a list of Taxonomy IDs and scientific names
    # corresponding to the previous web environment and query key using the
    # ESummary utility. 
    
    def requires(self):
        return PostTaxIDs(), RemoveDuplicateTaxIDs()
        
    def output(self):
        return luigi.LocalTarget('data/taxonomy-docsummaries.txt')
        
    def run(self):
        with ExitStack() as stack:
            infiles = [stack.enter_context(f.open('r')) for f in self.input()]
            outfile = stack.enter_context(self.output().open('w'))
            s = stack.enter_context(requests.Session())
            
            s.mount(self.url, self.adapter)
            tree = ET.parse(infiles[0])
            root = tree.getroot()
            query_key = root[0].text
            webenv = root[1].text
            count = len(infiles[1].read().splitlines())
            for retstart in range(0, count, self.retmax):
                time.sleep(0.1) # 10 requests per second limit.
                message = 'Progress: {0:.0%}'.format(retstart / count)
                self.set_status_message(message)
                print(message)
                args = [query_key, webenv, retstart,
                    self.retmax, 'taxonomy', self.api_key]
                prepped = utils.prep_esummary_req(*args)
                r = s.send(prepped, timeout=self.timeout, stream=False)
                if r.status_code == requests.codes.ok:
                    for key, value in r.json()['result'].items():
                        # Skips list of UIDs included in result.
                        if key != 'uids':
                            data = {'taxid':value['taxid'],
                                    'sname':value['scientificname']
                                    }                        
                            outfile.write('{taxid},{sname}\n'.format(**data))
                                    

class GBIFSpeciesMatch(GBIFTask):

    # This task uses the GBIF Species API to map the previous list of scientific
    # names from the NCBI Taxonomy Database to a list of GBIF species keys.
    
    def requires(self):
        return GetTaxonomySummaries()
        
    def output(self):
        return luigi.LocalTarget('data/gbif-species-matches.txt')
        
    def run(self):
        with ExitStack() as stack:
            infile = stack.enter_context(self.input().open('r'))
            outfile = stack.enter_context(self.output().open('w'))
            s = stack.enter_context(requests.Session())
            s.mount(self.url, self.adapter)
    
            entrez_data = [ln.split(',', maxsplit=1) for ln 
                           in infile.read().splitlines()]
        
            ranks = ['SPECIES', 'SUBSPECIES', 'VARIETY', 'SUBVARIETY', 'FORM',
                    'SUBFORM', 'CULTIVAR_GROUP', 'CULTIVAR']
                
            for i, line in enumerate(entrez_data):
                taxid, sname = line
                message = 'Progress: {0:.0%}'.format(i / len(entrez_data))
                self.set_status_message(message)
                if i % 10 == 0: print(message)
                payload = {'name': sname, 'kingdom':'plantae', 'strict':'true'}
                r = s.get(self.url + 'species/match', params=payload,
                          stream=False, timeout=self.timeout)
                if r.status_code == requests.codes.ok:
                    result = r.json()
                    if result['matchType'] != 'NONE' and result['rank'] in ranks:
                        data = {'taxid':taxid,
                                'skey':result['speciesKey']}
                        outfile.write('{taxid},{skey}\n'.format(**data))
                                    
                        
class RemoveDuplicateSpeciesKeys(luigi.Task):

    # This task returns a list of unique species keys.

    def requires(self):
        return GBIFSpeciesMatch()
        
    def output(self):
        return luigi.LocalTarget('data/unique-species-keys.txt')
        
    def run(self):
        with ExitStack() as stack:
            infile = stack.enter_context(self.input().open('r'))
            outfile = stack.enter_context(self.output().open('w'))
            lines = [line.split(',') for line in infile.read().splitlines()]
            species_keys = set([skey for taxid,skey in lines])
            for species_key in species_keys:
                outfile.write(species_key + '\n')
                
                
class PostUsageKeys(GBIFTask):

    # This task posts the previous list of unique species keys to the GBIF
    # Occurrence Store in chunk sizes of no larger than 300 and returns a list
    # of download IDs.

    user = luigi.Parameter(default='<user>') # Must create account with GBIF.
    pwd = luigi.Parameter(default='<pwd>') # Must create account with GBIF.
    chunk_size = 300
    retries = Retry(backoff_factor=4, status_forcelist=[503, 420], total=None,
        connect=10, read=10, redirect=10, status=250, method_whitelist=['POST'])
    adapter = requests.adapters.HTTPAdapter(max_retries=retries)

    def requires(self):
        return RemoveDuplicateSpeciesKeys()
    
    def output(self):
        return luigi.LocalTarget('data/download-IDs.txt')
    
    def run(self):
        with ExitStack() as stack:
            infile = stack.enter_context(self.input().open('r'))
            outfile = stack.enter_context(self.output().open('w'))
            s = stack.enter_context(requests.Session())    
    
            species_keys = infile.read().splitlines()
            length = len(species_keys)
        
            number_of_chunks = math.ceil(length / self.chunk_size)
        
            s.mount(self.url, self.adapter)
            s.auth = (self.user, self.pwd)
            
            for i, start in enumerate(range(0, length, self.chunk_size)):
                message = 'Progress: {0:.0%}'.format(i / number_of_chunks)
                self.set_status_message(message)
                print(message)
                
                end = start + self.chunk_size
            
                if end < length:
                    chunk = species_keys[start:end]
                else:
                    chunk = species_keys[start:]
                
                payload = utils.generate_query_expression(chunk)
                r = s.post(self.url + 'occurrence/download/request',
                            json=payload, timeout=self.timeout)
                print(r.status_code)
                if r.status_code == 201:
                    download_id = r.text
                    outfile.write(download_id + '\n')
                        
                
class GetDOIs(GBIFTask):

    # This task returns a list of Digital Object Identifiers (DOIs)
    # corresponding to the previous list of download IDs.

    def requires(self):
        return PostUsageKeys()
    
    def output(self):
        return luigi.LocalTarget('data/DOIs.txt')
    
    def run(self):
        with ExitStack() as stack:
            infile = stack.enter_context(self.input().open('r'))
            outfile = stack.enter_context(self.output().open('w'))
            s = stack.enter_context(requests.Session())
            
            download_ids = infile.read().splitlines()
            s.mount(self.url, self.adapter)
            
            for download_id in download_ids:
                r = s.get(self.url + 'occurrence/download/' + download_id)
                if r.status_code == requests.codes.ok:
                    doi = r.json()['doi']
                    outfile.write(doi + '\n')
                        
                
class GetDownloadLinks(GBIFTask):
    
    # This task checks the status of the previously submitted download requests
    # and returns a list of download links.    
    
    def requires(self):
        return PostUsageKeys()
    
    def output(self):
        return luigi.LocalTarget('data/download-links.txt')
    
    def run(self):
        with ExitStack() as stack:
            infile = stack.enter_context(self.input().open('r'))
            outfile = stack.enter_context(self.output().open('w'))
            s = stack.enter_context(requests.Session())
    
            download_ids = infile.read().splitlines()
            s.mount(self.url, self.adapter)
            for i, download_id in enumerate(download_ids):
                message = 'Progress: {0:.0%}'.format(i / len(download_ids))
                self.set_status_message(message)
                print(message)
                url = self.url + 'occurrence/download/' + download_id
                for j in range(250):
                    download_link = utils.get_download_link(s, url, self.timeout)
                    if download_link:
                        outfile.write(download_link + '\n')
                        break
                    else:
                        time.sleep(30)
                        continue
                else:
                    raise Exception('Unable to get download link.')
                            
            
class DownloadOccurrences(GBIFTask):

    # This task downloads and consolidates the zipped occurrence datasets using
    # the previous list of download links.
    
    def requires(self):
        return GetDownloadLinks()
    
    def output(self):
        return luigi.LocalTarget('data/occurrences.zip', format=luigi.format.Nop)
    
    def run(self):
        with ExitStack() as stack:
            infile = stack.enter_context(self.input().open('r'))
            outfile = stack.enter_context(self.output().open('w'))
            s = stack.enter_context(requests.Session())
            s.mount(self.url, self.adapter)
            
            download_links = infile.read().splitlines()
            
            with ZipFile(outfile, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
                for i, download_link in enumerate(download_links):
                    message = 'Progress: {0:.0%}'.format(i / len(download_links))
                    self.set_status_message(message)
                    print(message)
                    
                    with s.get(download_link, stream=True, timeout=120) as r:
                        if r.status_code == requests.codes.ok:
                            stream = io.BytesIO(r.content)
                            utils.copy_stream(stream, archive)
                                
                
class ClassifyOccurrences(luigi.Task):

    # This task iterates through the occurrence datasets and returns a
    # consolidated list of classified species keys.

    coord_uncertainty_limit = luigi.IntParameter(default=4500)
    path_to_raster_data = luigi.Parameter(default='data/k2classes.tif')

    def requires(self):
        return DownloadOccurrences()
    
    def output(self):
        return luigi.LocalTarget('data/classifications.txt')
    
    def run(self):
        with ExitStack() as outer_stack:
            infile = outer_stack.enter_context(self.input().open('r'))
            outfile = outer_stack.enter_context(self.output().open('w'))
            archive = outer_stack.enter_context(ZipFile(infile, 'r'))
            raster_data = outer_stack.enter_context(
                rasterio.open(self.path_to_raster_data))
            
            files = archive.infolist()
            band = raster_data.read(1)
            
            for i, file in enumerate(files):
                message = 'Progress: {0:.0%}'.format(i / len(files))
                self.set_status_message(message)
                print(message)
                with ExitStack() as inner_stack:
                    binary = inner_stack.enter_context(archive.open(file))
                    text = inner_stack.enter_context(io.TextIOWrapper(binary,
                        encoding='utf-8'))
                    reader = csv.reader(text, delimiter='\t',
                        quoting=csv.QUOTE_NONE)
                    next(reader) # Skips header.
                    for row in reader:
                        coord_uncertainty = row[18]
                        x = row[17] #longitude
                        y = row[16] #latitude
                        species_key = row[29]
                        args = [coord_uncertainty, x, y, 
                                raster_data, band,
                                self.coord_uncertainty_limit]
                        belt = utils.classify(*args)
                        if belt:
                            data = {'skey':species_key, 'belt':belt} 
                            outfile.write('{skey},{belt}\n'.format(**data))
        
        
class AggregateClassifications(luigi.Task):

    # This task groups the classified species keys and aggregates the classes
    # (bioclimatic belts) based on their mode.

    def requires(self):
        return ClassifyOccurrences()
    
    def output(self):
        return luigi.LocalTarget('data/agg-classifications.txt')
    
    def run(self):
        with ExitStack() as stack:
            infile = stack.enter_context(self.input().open('r'))
            outfile = stack.enter_context(self.output().open('w'))
            
            names = ['Species Key','Belt']
            df = pd.read_csv(infile, header=None, names=names)
            grouped = df.groupby('Species Key')
            aggregated = grouped.agg(lambda x: pd.Series.mode(x)[0])
            aggregated.to_csv(outfile)
            
            
class ClassifySequences(luigi.Task):

    # This task performs a series of joins on the list of UIDs, Taxonomy IDs and
    # aggregated classified species keys to finally return a list of classified
    # UIDs.

    def requires(self):
        return [GetNuccoreSummaries(),
                GBIFSpeciesMatch(),
                AggregateClassifications()
                ]
    
    def output(self):
        return luigi.LocalTarget('data/classified-sequences.txt')
    
    def run(self):
        with ExitStack() as stack:
            infiles = [stack.enter_context(f.open('r')) for f in self.input()]
            outfile = stack.enter_context(self.output().open('w'))
            
            df1 = pd.read_csv(infiles[0], header=None,
                names=['UID','Taxonomy ID'], index_col=0)

            df2 = pd.read_csv(infiles[1], header=None,
                names=['Taxonomy ID','Species Key'], index_col=0)

            df3 = pd.read_csv(infiles[2], index_col=0)
            
            first_join = df1.join(df2, on='Taxonomy ID', how='inner')
            second_join = first_join.join(df3, on='Species Key', how='inner')
            second_join.to_csv(outfile)
            
            
class RunAllTasks(luigi.WrapperTask):
    
    # This dummy tasks invokes all upstream tasks.
    
    def requires(self):
        yield GetDOIs()
        yield ClassifySequences()
        
    
if __name__ == '__main__':
    luigi.run()
        
