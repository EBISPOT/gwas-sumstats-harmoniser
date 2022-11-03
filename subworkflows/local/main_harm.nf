include {harmonization} from '../../modules/local/harmonization'
include {concatenate_chr_splits} from '../../modules/local/concatenate_chr_splits'

workflow main_harm {
    take:
    hm_input

    main:
    harmonization(hm_input)
    //hm_by_chrom: [GCST009150, forward, path of hm, path of log]
    concatenate_in_ch = harmonization.out.hm_by_chrom.collect().map{it[0..1]}.unique()
    //hm_all = harmonization.out.hm_by_chrom.map{it[2]}.collectFile(name: 'harmonisation.tsv')

    //concatenate_in_ch: [GCST008127,forward, path of hm]
    concatenate_chr_splits(concatenate_in_ch)
    //out.all_hm: [GCST009150,forward,path of harmonized.tsv]

    emit:
    //hm = gcst_palin_ch.concat(hm_all)
    hm=concatenate_chr_splits.out.all_hm
}