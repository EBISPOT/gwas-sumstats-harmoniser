include {map_to_build} from '../../modules/local/map_to_build'
include {ten_percent_counts} from '../../modules/local/ten_percent_counts'
include {ten_percent_counts_sum} from '../../modules/local/ten_percent_counts_sum'
include {generate_strand_counts} from '../../modules/local/generate_strand_counts'
include {summarise_strand_counts} from '../../modules/local/summarise_strand_counts'

workflow major_direction{
    take:
    input
    chr
    
    main:
    map_to_build(input,chr)
    //example: output is [GCST1,[path of 1.merged, path of 2.merged .....]]
    map_to_build.out.mapped
                    .transpose()
                    .map{tuple(get_chr(it[1]),it[0],it[1])}
                    .set{map_chr_ch}
    
    //example: out of map_to_build [GCST010681,[1,2,...]] tranpose into [[GCST010681,path 1.merged],[GCST010681,path 2.merged]] and then into [chr1,GCST010681,path 1.merged][chr2,GCST010681,path 2.merged].....
    
    Channel.fromPath("${params.ref}/homo_sapiens-chr*.vcf.gz") 
           .map { prepare_reference (it) }
           .set{ ref_chr_ch }
    // joint is still needed in case not all chr are running
    /* example:
    homo_sapiens-chr1.vcf.gz ->[chr1, path of homo_sapiens-chr1.vcf.gz] (ref_ch_chr)
    */

    map_chr_ch.combine(ref_chr_ch,by:0).set{count_ch}
    /* example
    [chr1, path of homo_sapiens-chr1.vcf.gz] (ref_chr_ch) + 
    [chr1, GCST1, path of 1.merged] (map_chr_ch)
    -> [chr1, GCST1, path of 1.merged,path of homo_sapiens-chr1.vcf.gz] (count_ch) 
    */
    
    ten_percent_counts(count_ch)
    ten_to_sum=ten_percent_counts.out.ten_sc.map{it[0]}.unique()
    // example: ten_to_sum [GCST1],[GCST2].....
    ten_percent_counts_sum(ten_to_sum)
    //example: output [GCST,path ten_percent.tsv,drop,rerun],[GCST,path ten_percent.tsv,forward,countiune]
    

    // determine whether conatin a string in the output txt file
    ten_percent_counts_sum.out.ten_sum.branch{rerun:it.contains("rerun")
                                          contiune:it.contains("contiune")}
                                      .set{branch}
    
    //branch rerun
    /* example: 
    [GCST, path ten_percent.tsv, drop, rerun] (rerun_branch)
    [chr1, GCST1, path of 1.merged,path of homo_sapiens-chr1.vcf.gz] (count_ch) 
    */
    branch.rerun.map{tuple(it[3],it[0])}.set{all_sc_ch}
    //example:rerun_branch into: [rerun,GCST1]
    count_ch.combine(all_sc_ch,by:1).set{rerun_branch}
    //example: rerun_branch: [GCST1,chr1, path of 1.merged,path of homo_sapiens-chr1.vcf.gz,return]
    generate_strand_counts(rerun_branch)
    //example: generate_strand_counts.out.all_sc: [GCST, rerun,path of full_sc]
    all_to_sum=generate_strand_counts.out.all_sc.map{tuple(it[0],it[1])}.unique()
    //all_to_sum: [GCST, rerun]
    summarise_strand_counts(all_to_sum)
    // example: [GCST,path Full.tsv,drop,contiune],[GCST,path Full.tsv,forward,contiune]

    //branch contiune
    // [GCST, ten_percent, forward,contiune] (contiune_branch)
    summarise_strand_counts.out.all_sum.mix(branch.contiune).set{all_files}
    //hm_input: [GCST,path ten_percent.tsv,forward,countiune],[GCST,path Full.tsv,reverse,countiune]
    count_ch.map{tuple(it[1],it[0],it[2],it[3])}.set{rearrnaged_count_ch}
    // example: [chr1, GCST1, path of 1.merged,path of homo_sapiens-chr1.vcf.gz] (count_ch) 
    // example into: [GCST1,chr1,path of merged, path of vcf]
    all_files.combine(rearrnaged_count_ch,by:0).set{all_input}
    //example: [GCST,path ten_percent.tsv,forward,countiune,chr,path of merged, path of vcf]
    all_input.map{it[0,2..6]}.set{hm_input}
    all_input.map{it[0..1]}.unique().set{direction_sum}
    
    emit:
    //hm_input=hm_input
    hm_input=hm_input
    direction_sum=direction_sum
}

// groovy function
def input_list(Path input) {
    return ["chr"+input.getName().split('\\.')[0],input]
}

def prepare_reference (Path input) {
    // extract chromosome from file path and form a list in list
    return [input.getName().split('-')[1].split('\\.')[0], input]
}

def extract_from_build(Path input){
    return[input.getName().split('\\.')[0].reverse().take(2).reverse(),input]
}

def get_chr(Path input) {
    return ("chr"+input.getName().split('\\.')[0])
}