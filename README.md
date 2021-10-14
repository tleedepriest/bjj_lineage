# bjj_lineage

## How to Run Pipeline

- Please see the requirements.txt file for necessary libraries to install in virtual environment. This project was coded on XUbuntu 20.04 with Python 3.8, although the code should be OS dependent.

run `python3 scripts/run_pipeline_luigi.py TransformLineagePathsToParentChild --local-scheduler`

to run the entire pipeline. This will save files to a local file system. Please report any errors to me. The visualization is not yet added to the pipeline.

## Final Output of Pipeline
![](hierarchy_kk.png)
