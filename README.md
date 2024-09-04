# Edito Resampling datasets

## About
This project is part of EDITO-INFRA ([Grant agreement ID: 101101473](https://doi.org/10.3030/101101473)):
- T7.3: End-to-end demonstrator for aquaculture and maritime industry

Author: Willem Boone | contact: [willem.boone@vliz.be](willem.boone@vliz.be)

## Goal
#### Summary of Demonstrator
The demonstrator use case (DUC) consists of a smartviewer that hosts a model to predict habitat suitability based on environmental living conditions. The smartviewer is maintained in a separate [repository](https://github.com/willem0boone/Edito_model_viewer) and is based on Carbonplan its [seaweed-farming-web](https://carbonplan.org/research/seaweed-farming)/[GitHub](https://github.com/carbonplan/seaweed-farming-web).

In this demonstrator, habitat suitability is calculated using a deterministic model that uses minimum and maximum thresholds on the environmental variables.
 The environmental parameters that are used are: 

- Sea surface temperature (°C)
- Sea surface salinity ()
- Bathymetry (depth in m) 

The thresholds for all variables can be adopted using slider widgets. On any changing parameter, the suitability map is updated and rendered in the viewer. Using a time slider, environmental parameters for several future climate scenarios can be accessed and converted in suitability maps. 

#### Data formatting
The environmental variable dataset used by the smartviewer, need to be provided in a specific format. To create this dataset, different sources and storage from Edito data lake are used. Two pipelines were required: 
- Downscaling large .zarr datasets to lower resolution. E.g. the bathymetry dataset is around 20GB, which is to large for the demonstrator purpose.
- Creating pyramids in which each level has increasing resolution (for optimal zooming/rendering).

## How to use
- resampling: package, pip install & documentation will follow. 
- notebooks: example .ipynb of downscaling & creating pyramids.
- pipeline_edito: .py scripts for processing as it was done for the DUC.
- tests: testing modules in resampling package. 