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
if (!params.inputPath & !params.to_harm_folder) {
    println " ERROR: You didn't set any folder to be harmonized \
    Please set --to_harm_folder and --inputPath and try again (: "
    System.exit(1)
}

if (!params.to_build & !params.chrom) {
    println "ERROR: You didn't set the target build and chromsomes to be harmnnized"
    println "Please set --to_build 38 or --chrom ['1','2',...]"
    System.exit(1)
}
/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT LOCAL MODULES/SUBWORKFLOWS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

//
// SUBWORKFLOW: Consisting of a mix of local and nf-core/modules
//
include {  main_harm  } from '../subworkflows/local/main_harm'
include { major_direction } from '../subworkflows/local/major_direction'
include {prepare_reference} from '../subworkflows/local/prepare_reference'
include {quality_control} from '../subworkflows/local/quality_control'
include {  move_files } from '../subworkflows/local/move_files'
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
    //
    // MODULE: check reference
    //
    ch_chrom=Channel.from(params.chrom).flatten().map{"chr"+it}
    // ch_chrom looks like: [chr1,chr2,chr3...]
    ch_files=Channel.fromPath("${params.to_harm_folder}/*.tsv").map{input_list(it)}.take(200)
    
    // input path containing all tsv file to be processed
    // ch_files channel looks like: [GCST010681, 37,path of the file]
    prepare_reference(ch_chrom)
    ch_direction=prepare_reference.out.parquet.map{it[0]}.first()
    //ch_direction [chr1]
    
    major_direction(ch_files,ch_direction)


    //major_direction.out.direction_sum: [GCST, path of sum_count]
    //major_direction.out.hm_input: tuple val(GCST), val(palin_mode), val(status), val(chrom), path(merged), path(ref)

    main_harm(major_direction.out.hm_input)
    // out:[GCST009150, forward, path of harmonised.tsv]
    quality_control(main_harm.out.hm,major_direction.out.direction_sum,prepare_reference.out.tbl)
    harmonnized_ch=quality_control.out.qclog
    all_files_ch=ch_files.join(harmonnized_ch,remainder: true)
    //example:[GCST90029037, 37, path *.tsv, path qc.tsv, path GCST90029037.running.log, SUCCESS_HARMONIZATION]

    move_files(all_files_ch)
    move_files.out.tmp.view()
    //[GCST009150, SUCCESS_HARMONIZATION, copied]
}

def input_list(Path input) {
    return [input.getName().split('_')[0],input.getName().split('\\.')[0][-2..-1],input,]
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
