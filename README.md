# proline-cli
A Command Line Interface to execute Proline data processings


## Getting started

### How to get the CLI?

Download the latest zip file in the github releases section and unzip it on your computer.

From a Linux shell this can be done by executing the following commands:

```
# Download the zip file
wget https://github.com/profiproteomics/proline-cli/releases/download/0.2.0-SNAPSHOT-2019-10-04/proline-cli-0.2.0-SNAPSHOT-bin.zip
# Install unzip (only required if unzip is not already installed)
apt-get install unzip
# Unzip the zip file
unzip proline-cli-0.2.0-SNAPSHOT-bin.zip
```

### How to run the CLI on the example dataset?

The zip file includes a small example dataset. It corresponds to a short acquisition (10 minutes gradient) of a sample containing 48 human proteins ([UPS1 sample](https://www.sigmaaldrich.com/content/dam/sigma-aldrich/docs/Sigma/Datasheet/2/ups1dat.pdf)) on an LTQ Orbitrap Velos instrument. Two technical replicates of the same UPS1 concentration are provided as mzDB files (OVEMB150205_25.raw.mzDB, OVEMB150205_27.raw.mzDB) and Mascot MS/MS search results (F071232.dat, F071233.dat).

To run the analysis of the example dataset, just execute this simple command form the CLI folder:
```./run_lfq_workflow```

This should work the same from both Linux and Windows.

## User guide

The CLI main entry point is the script called "run_cmd". Two versions are provided:
* run_cmd.bat for Windows systems
* run_cmd.sh for Linux systems

The second script called "run_lfq_workflow" is a wrapper of "run_cmd" to execute the command "run_lfq_workflow".
In the next section you will see find the list of commands available from the "run_cmd" script.

### Command line usage

Usage: ```run_cmd <command_name> <parameters>```

Bellow is the list of available commands and their respective parameters (if you execute "run_cmd" without any parameter this will be automatically displayed).
The commands ```import_result_files```,```validate_dataset``` and ```quantify_dataset``` are provided for testing purpose only, and thus we recommand to only use the ```run_lfq_workflow``` command for now.

```
Commands:
  import_result_files      Import result file in the Proline Databases
    Usage: import_result_files [options]
      Options:
        -c, --config_file
          Path to the import configuration file
      * -i, --input_file_list
          Text file containing the list of input files to import
          Default: <empty string>

  validate_dataset      Merge and validate imported result files
    Usage: validate_dataset [options]
      Options:
        -c, --config_file
          Path to the validation configuration file
          Default: <empty string>

  quantify_dataset      Quantify dataset using label-free method
    Usage: quantify_dataset [options]
      Options:
      * -c, --config_file
          Path to the quant configuration file
          Default: <empty string>
      * -ed, --exp_design_file
          Path to the experimental design file
          Default: <empty string>

  run_lfq_workflow      Run the full Proline label-free workflow
    Usage: run_lfq_workflow [options]
      Options:
      * -c, --config_file
          Path to the quant configuration file
          Default: <empty string>
      * -ed, --exp_design_file
          Path to the experimental design file
          Default: <empty string>
      * -i, --ident_file_list
          Text file containing the list of identification files to import
          Default: <empty string>
```


### How to use the Label-Free Workflow on your dataset?

Before running the label-free workflow you must:
* perform an MS/MS search using your favorite search engine. Proline supports natively result files created by Mascot, OMSSA and X!Tandem and it can also imports results in the mzIdentML file format.
* convert your raw data to the mzDB file format (see the [PWIZ-mzDB documentation](https://github.com/mzdb/pwiz-mzdb))

To analyze your dataset, follow these different steps:

1. Create a data folder, for instance in the CLI main folder. Then copy the mzDB and the MS/MS result files in this folder.

2. Open the config folder and edit the file named "import_file_list.txt". Remove the lines and put instead the paths to your MS/MS search results files.

3. In the same config folder edit the file named "quant_exp_design.txt". This file describes the experimental design of your dataset by mapping biological groups to you mzDB files. Replace thus its content by your own list of mzDB files and by annotating appropriatly the corresponding biological groups.

4. In the same config folder edit the file named "lfq_workflow.conf". This is the configuration file used by Proline to execute the Label-Free Workflow. It contains 3 different sections coresponding to the major phases of the workflow:
     * import-result-files-config
     * validate-dataset-config
     * quantify-dataset-config

- Configuration of the ```import-result-files-config``` section
If you use a concatenated database for your MS/MS search you have to tell Proline how to discriminate decoy hits from target one. To do this you can specify a regular expression in the ```decoyRegex``` field, and Proline will search for it in the protein description information. All protein hits matched by the regular expression will be considered as decoy hits. Example:
```
import-result-files-config {
  decoyRegex = "###REV###\S+"
}
```
   - Configuration of the ```validate-dataset-config``` section

In this section you can change the name given to the dataset after its validation and configure the Proline validation parameters.
For more information about the list of possible filters and validation settings please refer to the [Proline documentation](http://www.profiproteomics.fr/software/doc/2.0/#id.1t3h5sf).

We suggest to use the default parameters or eventually to change the "expected FDR" value in ```pep_match_validator_config``` and ```prot_set_validator_config```.

  - Configuration of the ```quantify-dataset-config``` section

In this section you can change the name given to the dataset after its quantification and configure the Proline quantification parameters.
For more information about the list of possible filters and validation settings please refer to the [Proline documentation](http://www.profiproteomics.fr/software/doc/2.0/#id.1ci93xb).

We suggest to use the default parameters or eventually to change the "m/z tolerance" values (```moz_tol``` field) in the different sub-sections. Another important parameter of the LFQ workflow is the time tolerance (```time_tol``` field) specified in the ```cross_assignment_config.ft_mapping_params``` sub-section.

5. Go back to main CLI folder and execute the script ```run_lfq_workflow```

6. Once the workflow is executed got to data folder, and you should see an output file with the ```.xlsx``` extension (Microsoft Excel file). This file will contain all the data generated during the execution of the workflow.
A description of this output file is available in the [Proline documentation](http://www.profiproteomics.fr/software/doc/2.0/#id.2bn6wsx)
