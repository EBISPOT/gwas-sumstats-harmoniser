---
name: Bug report
description: Report something that is broken or incorrect
labels: bug
title: ''
assignees: ''
---

body:
  - type: markdown
    attributes:
      value: |
        Thank you for reporting a bug with our harmonisation pipeline. To help us diagnose and fix the issue, please follow the instructions below to provide the necessary information.
        
        Before you post this issue, [nextflow troubleshooting](https://training.nextflow.io/basic_training/debugging/#execution-error-debugging) to help identify the real error.
  - type: textarea
    id: system
    attributes:
      label: System information
      description: |
        * Nextflow version _(eg. 22.10.1)_
        * Hardware _(eg. HPC, Desktop, Cloud)_
        * Executor _(eg. slurm, local, awsbatch)_
        * Container engine: _(e.g. Docker, Singularity, Conda, Podman, Shifter or Charliecloud)_
        * OS _(eg. CentOS Linux, macOS, Linux Mint)_
        * Version of `gwas-sumstats-harmoniser` _(eg. v1.1.8, v1.0.8)_
    validations:
      required: true
  - type: textarea
    id: issue_description
    attributes:
      label: Description of the Issue
      description: Please provide a detailed description of the issue.
    validations:
      required: true
  - type: markdown
    attributes:
      value: |
        ### Example Error Message Section
        ```
        [dc/0c96ad] NOTE: Process NFCORE_GWASCATALOGHARM:GWASCATALOGHARM:major_direction:map_to_build (1) terminated with an error exit status (1) -- Error is ignored`
        ```
  - type: markdown
    attributes:
      value: |
        ### Finding the Real Error Message

        1. **Check the `.nextflow.log` file:**
           - This file is located in the directory where you ran the Nextflow command.
           - Look for lines that contain the keyword `ERROR` or `WARN`.

        2. **Identify the Error in Process-Specific Logs:**
           - Each process has its own log file located in the `work` directory.
           - Find the directory corresponding to the failed process. It will have a unique hash name, like `work/dc/0c96ad` in the example error message.
           - Inside this directory, look for files like `.command.log`, `.command.err`, and `.command.out`.

        3. **Extract Relevant Error Information:**
           - Copy the last 20 lines of the `.command.err` or `.command.log` file.
           - If available, include any relevant output from the `.command.out` file.
  - type: textarea
    id: error_message
    attributes:
      label: Error Message
      description: Paste the relevant error message and logs here.
  - type: textarea
    id: command_used
    attributes:
      label: Command used and terminal output
      description: Steps to reproduce the behaviour. Please paste the command you used to launch the pipeline and the output from your terminal.
      render: console
      placeholder: |
        $ nextflow run ...

        Some output where something broke
  - type: textarea
    id: input_file
    attributes:
      label: First 10 Rows of the Input File
      description: Please provide the first 10 rows of the input file used in your analysis.
  - type: textarea
    id: files
    attributes:
      label: Relevant files
      description: |
        Please drag and drop the relevant files here. Create a `.zip` archive if the extension is not allowed.
        Your verbose log file `.nextflow.log` is often useful _(this is a hidden file in the directory where you launched the pipeline)_ as well as relevant process log files (`.command.log`, `.command.err`, `.command.out`).
