/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    VALIDATE INPUTS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
// Validate input parameters
// Check input path parameters to see if they exist
// Check mandatory parameters
/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    CONFIG FILES
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
params.harm=null
if (params.harm) {

if (!params.harm) {
    println " ERROR: You didn't set any files to be harmonized \
    Please set --harm and try again (: "
    System.exit(1)
}
}
/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT LOCAL MODULES/SUBWORKFLOWS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

//
// SUBWORKFLOW: Consisting of a mix of local and nf-core/modules
//
include {  chr_check  } from '../subworkflows/local/check_reference.nf'
include {  main_harm  } from '../subworkflows/local/main_harm'
include { major_direction } from '../subworkflows/local/major_direction'
include {quality_control} from '../subworkflows/local/quality_control'
/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT NF-CORE MODULES/SUBWORKFLOWS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

//
// MODULE: Installed directly from nf-core/modules
//
/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RUN MAIN WORKFLOW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

// Info required for completion email and summary

workflow GWASCATALOGHARM {
    
    main:
    params.file=null
    params.list=null
    if (params.file){
        println ("Harmonizing the file ${params.file}")
        files = Channel.fromPath(params.file).map{input_files(it)}
    }
    
    else if (params.list){
        println ("Harmonizing files in the file ${params.list}")
        files = Channel
            .fromPath(params.list)
            .splitText()map{it -> it.trim()}
            .map{row->file(row)}
            .map{input_files(it)}
    }
    /* MODULE: check reference
    ch_chrom looks like: [chr1,chr2,chr3...]
    chr_check() cross check required chromsomes with available reference
    ch_for_direction [chr1,chr2...] */

    ch_for_direction=chr_check().ch_input
    major_direction(ch_for_direction,files)

    //major_direction.out.direction_sum: [GCST, path of sum_count]
    //major_direction.out.hm_input: tuple val(GCST), val(palin_mode), val(status), val(chrom), path(merged), path(ref)
    harm_ch = major_direction.out.hm_input.groupTuple().transpose()
    main_harm(harm_ch)
    // out:[GCST009150, forward, path of harmonised.tsv]
    quality_control(main_harm.out.hm,major_direction.out.direction_sum,files,ch_for_direction,major_direction.out.unmapped)
}

def input_files(Path input)
{
    return [input.getName().split('_')[0],input.getName().split('\\.')[0][-2..-1],input]
}
/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    COMPLETION EMAIL AND SUMMARY
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    THE END
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
