include {qc} from '../../modules/local/qc'
include {haronization_log} from '../../modules/local/haronization_log'

/*-------- module from nf-core -----------------*/
//include {bgzip} from '../../modules/nf-core/modules/tabix/bgzip/main'
//include {tabix} from '../../modules/nf-core/modules/tabix/tabix/main'

workflow quality_control{
    take:
    ch_tsv
    ch_direction
    sql

    main:
    qc(ch_tsv,sql)
    qc.out.qc_ed.combine(ch_direction,by:0).set{ch_to_log}
    //ch_to_log: val(GCST), val(mode),path(all_hm), path('harmonised.qc.tsv'), path('report.txt'), path (total_sum)
    haronization_log("${params.ref}/homo_sapiens-chr22.vcf.gz",ch_to_log)

    emit:
    qclog=haronization_log.out.running_result
}
