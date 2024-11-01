#!/usr/bin/env nextflow
/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    nf-core/gwascatalogharm
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Github : 
    Website: 
    Slack  : 
----------------------------------------------------------------------------------------
*/

nextflow.enable.dsl = 2
/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    VALIDATE & PRINT PARAMETER SUMMARY
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    NAMED WORKFLOW FOR PIPELINE
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
include { PREPARE_REFERENCE } from './workflows/PREPARE_REFERENCE'
include { GWASCATALOGHARM_GWASCATALOG } from './workflows/gwascatalogharm_gwascatalog'
include { GWASCATALOGHARM } from './workflows/gwascatalogharm'

//
// WORKFLOW: Run main nf-core/gwascatalogharm analysis pipeline
//
workflow NFCORE_GWASCATALOGHARM {

    // Check mandatory parameters
    
    params.reference = null
    params.gwascatalog = null
    params.harm = null

    if (!params.chrom) {
    println "ERROR: You didn't set chromsomes to be harmnnised"
    println "Please set --chrom 22 or --chromlist 22,X,Y or set chrom in ./config/default_params.config "
    System.exit(1)
    }

    if (!params.reference) {
        if (!params.to_build) {
            println "ERROR: You didn't set the target build to harmonise to"
            println "Please set --to_build 38"
            System.exit(1)
            }
        
        if (!params.threshold) {
            println "ERROR: You didn't set threshold to imput the direction of palindromic variants"
            println "Please set --threshold 0.99 or set threshold in ./config/default_params.config "
            System.exit(1)
            }
        
        if (!params.version) {
            println " ERROR: Please specific the pipeline version you are running (e.g. v1.1.9) \
            Please set --version and try again (: "
            System.exit(1)
            }   
    }

    // check conditinal input parameters

    if (params.reference) {
        println ("Prepare the reference ...")
        PREPARE_REFERENCE()
    } 
    else if (params.gwascatalog) {
        if (!params.to_harm_folder) {
            println " ERROR: You didn't set any folder to be harmonised \
            Please set --to_harm_folder and try again (: "
            System.exit(1)
        } 
        else if (!params.ftp) {
            println " ERROR: You didn't set any folder to store your final result \
            Please set --ftp and try again (: "
            System.exit(1)
        } 
        else {
            println ("Harmonizing files in the folder ${params.all_harm_folder}")
            GWASCATALOGHARM_GWASCATALOG()
        }
    } 
    else if (params.harm) {
        if (!params.file && !params.list) { 
            println " ERROR: You didn't set any files to be harmonised \
                Please set --file for a single input file or \
                set --list for a list containing all files are waiting to be harmonised \
                and try again (: "
                System.exit(1)
        } 
        else {
            println ("Start harmonising files")
            GWASCATALOGHARM()
        }
    } 
    else {
            println " ERROR: You didn't set any model to run the pipeline \
            Please set --harm and try again (: "
            System.exit(1)
            }
}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RUN ALL WORKFLOWS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/


// WORKFLOW: Execute a single named workflow for the pipeline
// See: https://github.com/nf-core/rnaseq/issues/619

workflow {
    NFCORE_GWASCATALOGHARM ()
}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    THE END
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
