include {harmonisation} from '../../modules/local/harmonisation'
include {concatenate_chr_splits} from '../../modules/local/concatenate_chr_splits'

workflow main_harm {
    take:
    hm_input

    main:
    harmonisation(hm_input)
    //hm_by_chrom: [GCST009150, forward, path of hm, path of log]
    harmonisation.out.hm_by_chrom.map{it[0..1]}.unique().set{concatenate_in_ch}
    //concatenate_in_ch: [GCST008127,forward]
    concatenate_chr_splits(concatenate_in_ch)
    //out.all_hm: [GCST009150,forward,path of harmonized.tsv]

    emit:
    hm=concatenate_chr_splits.out.all_hm
}