# What is GINSENG?
The Global Inventory of Nucleotide Sequences Across Environmental Gradients (GINSENG) is an Extract,
Transform and Load (ETL) pipeline built to georeference and classify genomic sequence data in the
National Center for Biotechnology Information (NCBI) Nucleotide database.

# How Does GINSENG Work?
GINSENG is written in Python and uses the open source pipeline and workflow management
software, Luigi. GINSENG starts by searching the NCBI Nucleotide database for genetic
sequences based on a user-defined query. A Taxonomy ID maps each sequence to the scientific name of
a species. The scientific names are then fuzzy-matched against the Global Biodiversity
Information Facility (GBIF) Checklist Bank which resolves synonyms to accepted names and varities and subspecies to species-level names. A final list of Species Keys, returned from the GBIF Checklist Bank, is posted to the GBIF Occurrence Store and a set of georeferenced occurrences is downloaded. Next, the downloaded occurrences are classified by using their geocoordinates to sample a global raster map of seven bioclimatic belts in mountains. This map, referred to as K2, was developed in 2017 by the U.S. Geological Survey (USGS) and derived from an earlier map produced by Korner et al. in 2016. According to its authors, the K2 characterization of mountains, shown below, offers a robust framework for the integration of mountain biota in regional and larger scale biodiversity assessments, for biogeography, bioclimatology, macroecology, and conservation research (citation).

![K2 Raster Map](https://github.com/bfeinsilver/ginseng/blob/master/map-large.png)

In the final steps of the pipeline, the classified occurrences are aggregated by Species Key using the most frequently occurring bioclimatic belt and linked back to their corresponding genetic sequences according to the set of relationships defined below:

![Relationship Diagram](https://github.com/bfeinsilver/ginseng/blob/master/relationship-diagram.png)

# Example – Chloroplast Genomes
In this example, we want to classify all complete chloroplast genome sequences in the NCBI Nucleotide
database. We begin by constructing the following query statement:
```sql
chloroplast[Filter]
AND plants[Filter]
AND complete[Properties]
NOT unverified[Title]
AND (120000[SLEN]:160000[SLEN]) # This statement limits our search results to sequences between 120-160 Kbp.
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

# References
