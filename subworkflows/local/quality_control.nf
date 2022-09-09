include {qc} from '../../modules/local/qc'
include {harmonization_log} from '../../modules/local/harmonization_log'

/*-------- module from nf-core -----------------*/
//include {bgzip} from '../../modules/nf-core/modules/tabix/bgzip/main'
//include {tabix} from '../../modules/nf-core/modules/tabix/tabix/main'

workflow quality_control{
    take:
    ch_tsv
    ch_direction
    inputs

    main:
    qc(ch_tsv)
    ch_to_log=qc.out.qc_ed.combine(ch_direction,by:0).combine(inputs,by:0)
    ch_to_log.view()
    //ch_to_log: val(GCST), val(mode),path(all_hm), path('harmonized.qc.tsv'), path('report.txt'), path (total_sum), val(build), path (input)

    def to_log=params.chrom.last()
    harmonization_log("${params.ref}/homo_sapiens-chr${to_log}.vcf.gz",ch_to_log)

    emit:
    qclog=harmonization_log.out.running_result
}
