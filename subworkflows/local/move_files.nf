include {failed_copy} from '../../modules/local/failed_copy'
include {ftp_copy} from '../../modules/local/ftp_copy'

workflow move_files{
    take:
    input
    
    main:
    
    har_result_ch=input.map{it[0,2..5]}.branch{success:it.contains("SUCCESS_HARMONIZATION")
    failed:!it.contains("SUCCESS_HARMONIZATION")}
    //success_harmonized_file move to FTP
    //failed harmonized file move to Failed folder
    ftp_copy(har_result_ch.success)
    failed_copy(har_result_ch.failed)
    //return channlel: [GCST,SUCCESS_HARMONIZATION,copied],[GCST,SUCCESS_HARMONIZATION,failed_copied],[GCST,SUCCESS_HARMONIZATION,failed_copy]
    final_ch=ftp_copy.out.done.mix(failed_copy.out.done)

    emit:
    tmp=final_ch
}