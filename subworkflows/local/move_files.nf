include {failed_copy} from '../../modules/local/failed_copy'
include {ftp_copy} from '../../modules/local/ftp_copy'

workflow move_files{
    take:
    input
    
    main:
    
    har_result_ch=input.branch{success:it.contains("SUCCESS_HARMONIZATION")
    failed:it.contains("FAILED_HARMONIZATION")}
    //success_harmonized_file move to FTP
    //failed harmonized file only log moved to FTP
    //har_result_ch.success.view(): [GCST1, yaml,tsv,h.tsv.gz,h.tsv.gz.tbi, running.log,h.tsv.gz-meta.yaml, SUCCESS_HARMONIZATION]
    ftp_copy(har_result_ch.success)
    failed_copy(har_result_ch.failed)
    //return channlel: [GCST,SUCCESS_HARMONIZATION,copied],[GCST,SUCCESS_HARMONIZATION,failed_copied],[GCST,SUCCESS_HARMONIZATION,failed_copy]
    final_ch=ftp_copy.out.done.mix(failed_copy.out.done)

    emit:
    tmp=final_ch
}
