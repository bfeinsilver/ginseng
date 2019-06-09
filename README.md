# What is GINSENG?
The Global Inventory of Nucleotide Sequences Across Environmental Gradients (GINSENG) is an Extract,
Transform and Load (ETL) pipeline built to georeference and classify genomic sequence data in the
National Center for Biotechnology Information (NCBI) Nucleotide database.

# How Does GINSENG Work?
GINSENG is written in Python and uses the open source pipeline and workflow management
software, Luigi. GINSENG starts by searching the NCBI Nucleotide database for genetic
sequences based on a user-defined query. A Taxonomy ID maps each sequence to the scientific name of
a species. The scientific names are then used to perform a fuzzy search against the Global Biodiversity
Information Facility (GBIF) Checklist Bank resolving any synonyms and returning a list of Species Keys.
The Species Keys are then posted to the GBIF Occurrence Store from which a set of georeferenced
occurrences is downloaded. Next, the occurrences are classified using a global raster map of seven
bioclimatic belts in mountains developed by the U.S. Geological Survey (USGS) and Körner et al. (2011).

![K2 Raster Map](https://github.com/bfeinsilver/ginseng/blob/master/map-large.png)

Finally, the classified occurrences are grouped, aggregated and linked back to their corresponding
sequences according to the following relationships:

![Relationship Diagram](https://github.com/bfeinsilver/ginseng/blob/master/relationship-diagram.png)

# Example – Chloroplast Genomes
In this example, we want to classify all complete chloroplast genome sequences in the NCBI Nucleotide
database. We begin by constructing the following query statement:
```sql
chloroplast[Filter]
AND plants[Filter]
AND complete[Properties]
NOT unverified[Title]
AND (120000[SLEN]:160000[SLEN])
```
We save this file locally as `data/query.txt` and run the following command:
```bash
$ luigi --module ginseng-pipeline ClassifySequences
```
For more information on installing and running Luigi, please refer to their [documentation](https://luigi.readthedocs.io/en/stable/).

We are able to monitor the status of our tasks as well as view all dependencies in the Luigi Central
Scheduler:

![Dependency Graph](https://github.com/bfeinsilver/ginseng/blob/master/dependency-graph-screenshot.PNG)

When all our tasks have completed, our output will look like this:

final output txt

Of the 5,972 complete chloroplast genomes, 2,302 represented unique species occurring in mountains.

![Histogram](https://github.com/bfeinsilver/ginseng/blob/master/hist.png)
