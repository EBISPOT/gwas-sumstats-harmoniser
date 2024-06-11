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
    chroms
    unmapped

    main:
    qc(ch_tsv)
    ch_to_log=qc.out.qc_ed.combine(ch_direction,by:0).combine(inputs,by:0)
    //ch_to_log: val(GCST), val(mode),path(all_hm), path('harmonized.qc.tsv'), path('report.txt'), path (total_sum), path(yaml), path (input)

    def to_log=chroms.flatten().last()
    input_log=ch_to_log.combine(unmapped,by:0)
    harmonization_log(to_log,input_log)

    emit:
    qclog=harmonization_log.out.running_result
}
