# What is GINSENG?
The Global Inventory of Nucleotide Sequences Across Environmental Gradients (GINSENG) is an Extract, Transform and Load (ETL) pipeline built to georeference and classify genomic sequence data in the National Center for Biotechnology Information (NCBI) [Nucleotide database][3].

# How Does GINSENG Work?
GINSENG is written in Python and uses the open source pipeline and workflow management software, Luigi. GINSENG starts by searching the NCBI Nucleotide database for genetic sequences based on a user-defined query. A *Taxonomy ID* maps each sequence to the scientific name of a species. The scientific names are then fuzzy-matched against the Global Biodiversity Information Facility ([GBIF][4]) Checklist Bank which resolves synonyms, homonyms, varieties and subspecies to accepted species-level names. The resolved list of *Species Keys*, returned from the GBIF Checklist Bank, is then posted to the GBIF Occurrence Store in a download request. Once the requested georeferenced occurrences become available, they are downloaded and classified using their geocoordinates and a global raster map of seven bioclimatic belts in mountains.

---
The GINSENG classification engine uses the “K2” raster map developed by the U.S. Geological Survey (USGS)([2018][1]) and Körner et al. ([2016][2]). The K2 characterization of mountains first rigorously and consistently delineates global mountain regions then subdivides those regions into seven bioclimatic belts: nival, upper alpine, lower alpine, upper montane, lower montane, mountain area with frost and mountain area without frost. According to its authors, the K2 map, “offers a robust framework for the integration of mountain biota in regional and larger scale biodiversity assessments, for biogeography, bioclimatology, macroecology, and conservation research.” The K2 datafiles can be downloaded [here][5].

![K2 Raster Map](https://github.com/bfeinsilver/ginseng/blob/master/map-large.png)

---

In the final steps of the pipeline, the classified occurrences are aggregated by *Species Key* using the most frequently occurring bioclimatic belt and linked back to their corresponding genetic sequences according to the following relationships:

![Relationship Diagram](https://github.com/bfeinsilver/ginseng/blob/master/relationship-diagram.png)

# Example – Chloroplast Genomes
In this example, we show how GINSENG can be used to classify all complete chloroplast genome sequences in the NCBI Nucleotide
database.
## Setting Up and Running The Pipeline
We begin by constructing the following query statement:
```sql
chloroplast[Filter]
AND plants[Filter]
AND complete[Properties]
NOT unverified[Title]
AND (120000[SLEN]:160000[SLEN]) # This limits our results to sequences between 120-160 Kbp.
```
We then run the following command passing our previously constructed query statement as the `<term>` argument:
```
$ luigi --module ginseng-pipeline RunAllTasks --SearchNuccore-term <term>
```
For more information on using Luigi, please refer to the [documentation][6].

Once the pipeline is up and running, we are able to monitor the status of our tasks as well as view all dependencies in the Luigi Central Scheduler:

![Dependency Graph](https://github.com/bfeinsilver/ginseng/blob/master/dependency-graph-screenshot.PNG)

When the Central Scheduler shows that all tasks have completed successfully, our output data is saved in the file `data\classified-sequences.txt` and should look something like this:

```
UID        Taxonomy ID  Species Key  Belt
1677650587 141191       5420912      7
1654700304 141191       5420912      7
1674864531 4442         3189635      5
1468712715 4442         3189635      5
430728250  4442         3189635      5
...        ...          ...          ...
```

## Analysis
Our query of the Nucleotide database yielded 5,972 complete chloroplast genomes, 2,302 of which represented unique plant species occurring in mountains. This indicates that over 60% of the genomes represented either multiple sequences of a single species, sequences of synonymous species, sequences of varieties and subspecies, or sequences of species that did not occur in mountains. A histogram of the results is generally consistent with the observation that the number of plant species declines with increasing elevation. However, it is unclear why so few species occurred in Belt 6 (mountain area with frost)...

![Histogram](https://github.com/bfeinsilver/ginseng/blob/master/hist.png)

[1]: https://bioone.org/journals/Mountain-Research-and-Development/volume-38/issue-3/MRD-JOURNAL-D-17-00107.1/A-New-High-Resolution-Map-of-World-Mountains-and-an/10.1659/MRD-JOURNAL-D-17-00107.1.full

[2]: https://link.springer.com/article/10.1007/s00035-016-0182-6

[3]: https://www.ncbi.nlm.nih.gov/nucleotide

[4]: https://www.gbif.org

[5]: https://rmgsc.cr.usgs.gov/outgoing/ecosystems/Global

[6]: https://luigi.readthedocs.io/en/stable
