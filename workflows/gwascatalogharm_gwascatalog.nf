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

workflow GWASCATALOGHARM_GWASCATALOG {
    //
    // MODULE: check reference
     //ch_for_direction [chr1]
    ch_direction=chr_check().ch_input
    // ch_chrom looks like: [chr1,chr2,chr3...]
    ch_files=Channel.fromPath(["${params.all_harm_folder}/*.gz", "${params.all_harm_folder}/*.tsv", "${params.all_harm_folder}/*.csv", "${params.all_harm_folder}/*.txt"]).map{input_list(it)}
    // input path containing all file to be processed
    //TODO: for standard implementation - this wildcard match will need to be constrained to tsv.gz
    // metadata files will follow the pattern <filename>-meta.yaml
    // ch_files channel looks like: [GCST010681, 37,path of the file]
    
    major_direction(ch_direction,ch_files)


    //major_direction.out.direction_sum: [GCST, path of sum_count]
    //major_direction.out.hm_input: tuple val(GCST), val(palin_mode), val(status), val(chrom), path(merged), path(ref)
    //ch_count = major_direction.out.hm_input.groupTuple().map{[it[0],it[1].size()]}
    harm_ch = major_direction.out.hm_input.groupTuple().transpose()
    main_harm(harm_ch,ch_files)
    // out:[GCST009150, forward, path of harmonised.tsv]
    quality_control(main_harm.out.hm,major_direction.out.direction_sum,ch_files,ch_direction,major_direction.out.unmapped)
    harmonnized_ch=quality_control.out.qclog
    all_files_ch=ch_files.join(harmonnized_ch,remainder: true)
    //example:[GCST90029037,raw_yaml, path input.tsv, path h.tsv.gz, path h.tsv.gz.tbi path GCST90029037.running.log, path h.tsv.gz-meta.yaml, SUCCESS_HARMONIZATION]

    move_files(all_files_ch)
    //[GCST009150, SUCCESS_HARMONIZATION, copied]
}

def input_list(input) {
    def baseName = input.getName().split("\\.")[0]

    // Check if the base name matches the pattern GCST[0-9]+
    def matcher = (baseName=~ /GCST\d+/).findAll()
    if (matcher) {
        // Extract GCST ID using regex find
        def gcstId = matcher[0]  // Get the first match
        return [gcstId, input+"-meta.yaml", input]
    } else {
        // Default case
        return [baseName, input+"-meta.yaml", input]
    }
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
