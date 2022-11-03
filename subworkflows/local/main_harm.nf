include {harmonization} from '../../modules/local/harmonization'
include {concatenate_chr_splits} from '../../modules/local/concatenate_chr_splits'

workflow main_harm {
    take:
    hm_input

    main:
    harmonization(hm_input)
    //hm_by_chrom: [GCST009150, forward, path of hm, path of log]
    harmonization.out.hm_by_chrom.map{it[0..2]}.collect().set{concatenate_in_ch}
    //concatenate_in_ch: [GCST008127,forward, path of hm]
    concatenate_chr_splits(concatenate_in_ch)
    //out.all_hm: [GCST009150,forward,path of harmonized.tsv]

    emit:
    hm=concatenate_chr_splits.out.all_hm
}