include {qc} from '../../modules/local/qc'
include {harmonization_log} from '../../modules/local/harmonization_log'
include {update_meta_yaml} from '../../modules/local/update_meta_yaml'

/*-------- module from nf-core -----------------*/
//include {bgzip} from '../../modules/nf-core/modules/tabix/bgzip/main'

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
    input_log=ch_to_log.combine(unmapped,by:0)
    def to_log=chroms.flatten().last()
    harmonization_log(to_log,input_log)
    update_meta_yaml(harmonization_log.out)

    emit:
    qclog=update_meta_yaml.out.running_result
}
