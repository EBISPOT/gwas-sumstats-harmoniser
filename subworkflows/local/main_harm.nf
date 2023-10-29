include {harmonization} from '../../modules/local/harmonization'
include {concatenate_chr_splits} from '../../modules/local/concatenate_chr_splits'

workflow main_harm {
    take:
    harm_ch
    //files: [GCST,path yaml, path tsv]

    main:
    //harm_input:[GCST,drop.countiune,chr21,path merged, path vcf,path yaml]
    harmonization(harm_ch)
    //hm_by_chrom: [GCST009150, forward, path of hm, path of log]
    //hm_input.map{it[6]}.dump(tag:'bar')
    id_palin_ch = harmonization.out.hm_by_chrom.map{it[0,1,2]}.unique()
    hm_files = harmonization.out.hm_by_chrom.map{it[0,3]}.groupTuple()
    concatenate_in_ch = id_palin_ch.join(hm_files)

    //concatenate_in_ch: [GCST008127,forward, path of hm]
    concatenate_chr_splits(concatenate_in_ch)
    //out.all_hm: [GCST009150,forward,path of harmonized.tsv]

    emit:
    hm=concatenate_chr_splits.out.all_hm
}
